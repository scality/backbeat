'use strict';

/**
 * Act 06: workgroups, the mechanism that lets one delivery topic serve a
 * population of destinations without every worker holding a producer for
 * every destination.
 *
 * The topology is decided: ONE internal delivery topic. The populator writes
 * to that one topic, and a workgroup is a consumer group over it with a
 * slice filter, so a worker commits records outside its slice without
 * delivering them. Per-workgroup topics are out.
 *
 * The document in ZooKeeper is the whole contract: a generation, a hashmod
 * rule whose remainders cover the whole modulo, optional static pins, and at
 * a change the barrier offsets. Ownership is a total function, so every
 * record has exactly one owner, which is why a slice filter can commit what
 * it does not deliver without losing anything.
 *
 * Four things happen here, in order:
 *   a  two auto workgroups, six destinations, one worker each
 *   b  one workgroup's worker dies: only its destinations pause
 *   c  a noisy destination is pinned to its own workgroup
 *   d  a live reshard from modulo 2 to modulo 3, through the cutover tool,
 *      with the barrier, the drain report and verify
 *
 * Every step uses the repository's own tools: bin/notificationWorkgroupCutover.js
 * for plan, cutover, preseed, show and verify, and the ownership function
 * from extensions/notification/utils/workgroups.js for the mapping.
 */

const assert = require('assert');
const env = require('../lib/env');
const kafka = require('../lib/kafka');
const procs = require('../lib/procs');
const wait = require('../lib/wait');
const flow = require('../lib/flow');
const zk = require('../lib/zk');
const s3lib = require('../lib/s3');
const { Act, say, note, watch, step, line } = require('../lib/narrate');

// The five destinations the platform validates. At modulo 2 the md5 of
// these five names puts all three poc ones on the same remainder, so the act
// uses modulo 4, where they spread over three remainders and both slices
// carry real load. The reshard then goes from two workgroups to three over
// the same modulo, which moves one destination and leaves the others where
// they are.
const DESTS = ['poc-dest-1', 'poc-dest-2', 'poc-dest-3',
    'krb-dest-a', 'krb-dest-b'];
const MODULO = '4';
const GEN1 = ['--modulo', MODULO, '--workgroup', 'wg-a:0,1',
    '--workgroup', 'wg-b:2,3'];
const GEN2 = GEN1.concat(['--static', 'wg-pin:poc-dest-1']);
const GEN3 = ['--modulo', MODULO, '--workgroup', 'wg-a:0,1',
    '--workgroup', 'wg-b:2', '--workgroup', 'wg-c:3',
    '--static', 'wg-pin:poc-dest-1'];
const BUCKETS = {};
DESTS.forEach((d, i) => { BUCKETS[d] = `demo-wg-${i + 1}`; });
const CUTOVER = 'bin/notificationWorkgroupCutover.js';
const STOP_EARLY = env.knob('DEMO_WORKGROUPS_STOP_EARLY', '') === '1';
const LOAD_SECONDS = Number(env.knob('DEMO_ACT06_SECONDS', 420));

function register(ctx) {
    describe('Act 06: workgroups', () => {
        const act = new Act('06', 'workgroups', 'W gates and C5r',
            'one delivery topic, sliced into workgroups by a document in '
            + 'ZooKeeper, changed live through a barrier cutover');
        let config;
        const workers = {};
        let load;
        const from = {};

        /**
         * Run the cutover tool.
         *
         * @param {String} cmd - plan, cutover, preseed, show or verify
         * @param {Array} args - extra arguments
         * @return {Object} { code, out }
         */
        function tool(cmd, args) {
            const r = procs.runTool({ act, name: `cutover-${cmd}`,
                argv: [CUTOVER, cmd].concat(args || []),
                configFile: config, timeoutMs: 300000 });
            line(r.out.split('\n').map(l => `      | ${l}`).join('\n'));
            if (r.code !== 0 && cmd !== 'verify') {
                note(`the tool exited ${r.code}`);
            }
            return r;
        }

        async function startWorkgroupWorkers(ids, generation) {
            let n = 0;
            for (const id of ids) {
                n += 1;
                 
                workers[`${id}-gen${generation}`] = await flow.startWorker(
                    act, config, n + (generation - 1) * 4,
                    { workgroupId: id, autoRestart: true });
                say(`  workgroup ${id} joins group `
                    + `${zk.groupIdFor(env.DELIVERY_GROUP, id, generation)}`);
            }
        }


        /**
         * Wait for the drain report to exit 0, which is the only statement
         * that the previous generation can be stopped without losing
         * records.
         *
         * A wedged previous-generation worker is what makes this wait
         * unbounded: it holds its partitions, commits nothing, and its group
         * therefore never reaches its barriers. That is the design/06 wedge
         * landing on the one gate a cutover cannot skip, and the cure is the
         * documented one: restart that one worker.
         *
         * @param {Array} prevIds - the previous generation's workgroup ids
         * @param {Number} prevGen - the previous generation number
         * @param {Number} budgetMs - how long to keep trying
         * @return {Promise} resolves true when verify exits 0
         */
        async function waitForDrain(prevIds, prevGen, budgetMs) {
            const deadline = Date.now() + budgetMs;
            const seen = {};
            let lastMove = Date.now();
            let cures = 0;
            while (Date.now() < deadline) {
                const v = tool('verify', []);
                if (v.code === 0) {
                    say('verify exits 0: every previous group is past every '
                        + 'barrier, so the old generation can be stopped');
                    act.measured('wedge cures needed during the drain', cures);
                    return true;
                }
                let moved = false;
                prevIds.forEach(id => {
                    const g = zk.groupIdFor(env.DELIVERY_GROUP, id, prevGen);
                    const c = kafka.groupState(g, env.DELIVERY_TOPIC).committed;
                    if (seen[id] === undefined || c !== seen[id]) {
                        moved = true;
                    }
                    seen[id] = c;
                });
                if (moved) {
                    lastMove = Date.now();
                }
                if (Date.now() - lastMove > 60000) {
                    for (const id of prevIds) {
                        const g = zk.groupIdFor(env.DELIVERY_GROUP, id, prevGen);
                        const st = kafka.groupState(g, env.DELIVERY_TOPIC);
                        const w = workers[`${id}-gen${prevGen}`];
                        if (!w || st.lag === 0) {
                            continue;
                        }
                        note(`WEDGE SUSPECTED on workgroup ${id} of generation `
                            + `${prevGen}: it has committed ${st.committed} and`);
                        note(`  has not moved, with ${st.lag} still to read. Its`);
                        note('  liveness probe answers 200 all the same. Curing');
                        note('  it the documented way: restart that one worker.');
                        note('  A restart on the same group resumes at that');
                        note('  group\'s committed offset, so anything it');
                        note('  delivered without committing arrives twice.');
                        w.stop();
                         
                        await procs.sleep(env.pause(5000));
                        const idx = prevIds.indexOf(id) + 1 + (prevGen - 1) * 4;
                         
                        workers[`${id}-gen${prevGen}`] = await flow.startWorker(
                            act, config, idx,
                            { workgroupId: id, autoRestart: true });
                        cures += 1;
                        lastMove = Date.now();
                    }
                }
                 
                await procs.sleep(15000);
            }
            act.measured('wedge cures needed during the drain', cures);
            return false;
        }


        /**
         * Wait for every workgroup of a generation to be delivering, and cure
         * a wedged one the documented way.
         *
         * The wedge is the reliability ceiling of this codebase and it fires
         * on consumer starts, so a workgroup can join, hold its partitions,
         * answer liveness 200 and deliver nothing. An operator restarts that
         * one worker; so does this.
         *
         * @param {Array} ids - workgroup ids
         * @param {Number} generation - generation number
         * @param {Number} budgetMs - how long to keep trying
         * @return {Promise} resolves with the ids that never delivered
         */
        async function ensureDelivering(ids, generation, budgetMs) {
            const deadline = Date.now() + (budgetMs || 300000);
            const cured = {};
            const stuck = () => Promise.all(ids.map(async id => {
                const w = workers[`${id}-gen${generation}`];
                const n = Number(w.probePort) - env.PROBE_BASE;
                const delivered = await wait.counter(n, 'delivered');
                const skipped = await wait.counter(n, 'skipped');
                return { id, w, n, delivered, skipped };
            }));
            while (Date.now() < deadline) {
                 
                const rows = await stuck();
                const idle = rows.filter(r => r.delivered === 0 && r.skipped === 0);
                if (!idle.length) {
                    rows.forEach(r => say(`${r.id}: delivered ${r.delivered}, `
                        + `skipped ${r.skipped} records outside its slice`));
                    return [];
                }
                say(`waiting for ${idle.map(r => r.id).join(', ')} to deliver`);
                 
                await procs.sleep(15000);
                 
                const again = await stuck();
                for (const r of again.filter(x => x.delivered === 0 && x.skipped === 0)) {
                    const bal = r.w.rebalances();
                    const group = zk.groupIdFor(env.DELIVERY_GROUP, r.id,
                        generation);
                    const lag = kafka.groupLag(group, env.DELIVERY_TOPIC);
                    // an idle worker with nothing to read looks the same in
                    // the log as a wedged one: what tells them apart is
                    // whether there are records waiting for it
                    if (lag <= 0 || bal.revoke < 3 || (cured[r.id] || 0) >= 2) {
                        continue;
                    }
                    note(`WEDGE on workgroup ${r.id}: ${bal.assign} assigns and`);
                    note(`  ${bal.revoke} revokes, nothing delivered, liveness`);
                    note(`  ${await wait.liveness(r.n)}. This is the design/06`);
                    note('  wedge on a consumer start, and the cure is a');
                    note('  restart of that one worker.');
                    r.w.stop();
                     
                    await procs.sleep(env.pause(5000));
                     
                    workers[`${r.id}-gen${generation}`] = await flow.startWorker(
                        act, config, r.n, { workgroupId: r.id, autoRestart: true });
                    cured[r.id] = (cured[r.id] || 0) + 1;
                    act.measured('wedge cures on a workgroup start',
                        Object.values(cured).reduce((a, b) => a + b, 0));
                }
            }
            const last = await stuck();
            return last.filter(r => r.delivered === 0 && r.skipped === 0)
                .map(r => r.id);
        }

        function stopGeneration(ids, generation) {
            ids.forEach(id => {
                const w = workers[`${id}-gen${generation}`];
                if (w) {
                    w.stop();
                }
            });
        }

        before(async () => {
            act.open();
            act.expect('destination to workgroup mapping',
                'every destination owned by exactly one workgroup');
            act.expect('gaps (loss), generation 1', 0);
            act.expect('workgroup isolation',
                'only the dead workgroup\'s destinations pause');
            act.expect('pinned destination', 'served by the pinned workgroup only');
            act.expect('reshard gaps (loss)', 0);
            act.expect('reshard inversions', 0);
            act.expect('reshard duplicates',
                'the old generation\'s consumption past its barriers');
            act.expect('verify exit code before stopping the old generation', 0);
            config = flow.poolConfig(act, ctx, {
                only: DESTS,
                workgroups: true,
                tag: 'wg',
            });
            for (const d of DESTS) {
                 
                await s3lib.bucketWith(ctx.s3, BUCKETS[d], [d]);
            }
            say('six buckets, one per destination: '
                + `${Object.values(BUCKETS).join(', ')}`);
            DESTS.forEach(d => { from[d] = kafka.head(ctx.customerTopicOf[d], 0); });
        });

        after(() => {
            if (load && load.proc) {
                load.proc.stop();
            }
            procs.stopAll();
            act.close();
        });

        it('slices one topic into two workgroups and keeps them honest',
            async () => {
                step(1, 'write the generation 1 document: two auto workgroups');
                note(`modulo ${MODULO}, one workgroup taking remainders 0 and`);
                note('1 and the other 2 and 3, so the remainders cover the');
                note('whole modulo. That total coverage is what makes');
                note('ownership a function: every record has exactly one');
                note('owner, and a worker can commit what it does not own.');
                tool('plan', GEN1);
                const first = tool('cutover', GEN1);
                assert.strictEqual(first.code, 0,
                    'the generation 1 cutover failed; its own error says why, '
                    + 'and a group that still has members from an earlier run '
                    + 'is the usual reason');
                const doc = zk.workgroupsDoc();
                assert.ok(doc, 'no workgroups document was written');
                line(zk.describeDoc(doc).map(l => `      | ${l}`).join('\n'));
                watch('kafka ui', `tab ZooKeeper, node ${env.ZK_WORKGROUPS_PATH}`);
                watch('terminal', 'demo/bin/zk-show.sh prints the same thing');

                step(2, 'which workgroup owns which destination');
                note('the hash is md5 over the encoded destination token, and');
                note('the remainder picks the owner. This is the repository\'s');
                note('own workgroupIdForDestination, not a copy of the rule.');
                const map = zk.mapping(doc, DESTS);
                Object.entries(map).forEach(([d, id]) => {
                    say(`  ${d.padEnd(12)} -> ${id}`);
                });
                const owners = new Set(Object.values(map));
                act.measured('destination to workgroup mapping',
                    Object.values(map).every(Boolean) && owners.size > 1
                        ? 'every destination owned by exactly one workgroup'
                        : `only ${Array.from(owners).join(',')} carries load`);
                assert.ok(Object.values(map).every(Boolean),
                    'a destination had no owner');

                step(3, 'one worker per workgroup, then load on all six');
                await startWorkgroupWorkers(['wg-a', 'wg-b'], 1);
                await flow.startPopulator(act, config, 'pool');
                load = flow.startDriver(act, {
                    'buckets': Object.values(BUCKETS).join(','),
                    'prefix': 'wg', 'rate': 6, 'duration': LOAD_SECONDS,
                    'straddle': 3, 'straddle-every': 5,
                });
                watch('grafana', 'row "Workgroups": delivered per second by '
                    + 'workgroup, and lag per workgroup consumer group');
                watch('kafka ui', 'one consumer group per workgroup, named '
                    + `${env.DELIVERY_GROUP}-<workgroup>-gen1`);
                note('the populator publishes on a batch cadence, so wait for');
                note('real deliveries rather than for a fixed number of seconds');
                const idle = await ensureDelivering(['wg-a', 'wg-b'], 1, 300000);
                assert.strictEqual(idle.length, 0,
                    `workgroups that never delivered: ${idle.join(', ')}`);
                for (const id of ['wg-a', 'wg-b']) {
                    const w = workers[`${id}-gen1`];
                    const n = Number(w.probePort) - env.PROBE_BASE;
                     
                    const delivered = await wait.counterBy(n, 'delivered', 'target');
                    say(`${id} delivered by destination: ${JSON.stringify(delivered)}`);
                }
                note('the skipped counter is the cost of one shared topic: a');
                note('hash and a commit per record a workgroup does not own,');
                note('no I/O. That is the trade against per-workgroup topics.');
            });

        it('keeps one workgroup\'s failure inside that workgroup', async () => {
            step(4, 'kill -9 the worker of one workgroup');
            const victimId = 'wg-b';
            const victim = workers[`${victimId}-gen1`];
            const survivorId = 'wg-a';
            const survivor = workers[`${survivorId}-gen1`];
            const vN = Number(victim.probePort) - env.PROBE_BASE;
            const sN = Number(survivor.probePort) - env.PROBE_BASE;
            await wait.until('both workgroups to be delivering', async () => {
                const a = await wait.counter(vN, 'delivered');
                const b = await wait.counter(sN, 'delivered');
                return a > 0 && b > 0;
            }, 240000, 5000);
            const before = {
                victim: await wait.counter(vN, 'delivered'),
                survivor: await wait.counter(sN, 'delivered'),
            };
            say(`${victimId} has delivered ${before.victim}, ${survivorId} `
                + `${before.survivor}`);
            say(`killing ${victimId}'s worker (pid ${victim.pid}). Watch the`);
            say(`per-workgroup lag panel: ${victimId} climbs, ${survivorId} does not.`);
            victim.hold();
            victim.kill('SIGKILL');
            act.timeline(`workgroup ${victimId} worker KILL9`);
            await wait.until(`${survivorId} to deliver more while ${victimId} is dead`,
                async () => (await wait.counter(sN, 'delivered')) > before.survivor,
                180000, 5000);
            await procs.sleep(env.pause(30000));
            const during = {
                victim: await wait.counter(vN, 'delivered'),
                survivor: await wait.counter(sN, 'delivered'),
            };
            const victimGroup = zk.groupIdFor(env.DELIVERY_GROUP, victimId, 1);
            const survivorGroup = zk.groupIdFor(env.DELIVERY_GROUP, survivorId, 1);
            say(`${victimId} group lag ${kafka.groupLag(victimGroup, env.DELIVERY_TOPIC)}, `
                + `${survivorId} group lag `
                + `${kafka.groupLag(survivorGroup, env.DELIVERY_TOPIC)}`);
            note('both groups read the whole topic and skip what they do not');
            note('own, so their raw lag is about the same number. The');
            note('isolation is in the DELIVERED counters and in the customer');
            note('topics, not in the lag: that is what the per-workgroup');
            note('delivered panel is for.');
            say(`${survivorId} delivered ${before.survivor} then `
                + `${during.survivor}: it kept working`);
            act.measured('workgroup isolation',
                during.survivor > before.survivor
                    ? 'only the dead workgroup\'s destinations pause'
                    : 'the survivor stopped too');
            note('a wedge looks different from a death, and worse: the worker');
            note('is up, holds its partitions, answers /_/live with 200, and');
            note('its delivered counter does not move. Check the counter, not');
            note('the lag. The cure is a restart of that one worker.');

            step(5, 'bring it back and watch it catch up');
            victim.release();
            await wait.until('the workgroup worker to come back',
                () => victim.isRunning(), 90000, 1000);
            await wait.until('its probe to answer',
                async () => (await wait.liveness(vN)) === 200, 60000, 2000);
            say(`${victimId} worker back as pid ${victim.pid}`);
            await wait.until(`${victimId} to be delivering again`,
                async () => (await wait.counter(vN, 'delivered')) > during.victim,
                180000, 5000);
            say(`${victimId} delivered ${await wait.counter(vN, 'delivered')}`);
            assert.ok(during.survivor > before.survivor,
                'the surviving workgroup stopped delivering too');
        });

        it('pins a noisy destination to its own workgroup', async () => {
            step(6, 'add a static workgroup for one destination, generation 2');
            const pinned = 'poc-dest-1';
            note('a static rule beats the hashmod one, so the destination is');
            note('carved out of the hash space. This is the lever for a noisy');
            note('or hostile tenant: its own workgroup, its own blast radius.');
            const r = tool('cutover', GEN2);
            assert.strictEqual(r.code, 0, 'the pin cutover failed');
            const doc = zk.workgroupsDoc();
            line(zk.describeDoc(doc).map(l => `      | ${l}`).join('\n'));
            const map = zk.mapping(doc, DESTS);
            say(`${pinned} now belongs to ${map[pinned]}`);
            assert.strictEqual(map[pinned], 'wg-pin',
                'the pin did not take effect in the document');

            step(7, 'start generation 2, drain generation 1, then stop it');
            note('the barrier is why this is safe: generation 2 starts at the');
            note('barrier offsets, generation 1 owns everything before them,');
            note('and verify says when it has got there.');
            await startWorkgroupWorkers(['wg-a', 'wg-b', 'wg-pin'], 2);
            await ensureDelivering(['wg-a', 'wg-b', 'wg-pin'], 2, 240000);
            const verified = await waitForDrain(['wg-a', 'wg-b'], 1, 480000);
            act.measured('verify exit code before stopping the old generation',
                verified ? 0 : 2);
            assert.ok(verified, 'verify never reached 0, so generation 1 '
                + 'cannot be stopped without losing records');
            stopGeneration(['wg-a', 'wg-b'], 1);
            await procs.sleep(env.pause(10000));

            step(8, 'the pinned destination is now served by the pin only');
            const pinWorker = workers['wg-pin-gen2'];
            const pinN = Number(pinWorker.probePort) - env.PROBE_BASE;
            const start = await wait.counter(pinN, 'delivered', { target: pinned });
            await procs.sleep(env.pause(45000));
            const now = await wait.counter(pinN, 'delivered', { target: pinned });
            say(`wg-pin delivered ${start} then ${now} records for ${pinned}`);
            act.measured('pinned destination', now > start
                ? 'served by the pinned workgroup only'
                : 'the pinned workgroup delivered nothing');
            assert.ok(now > start, 'the pinned workgroup delivered nothing');
        });

        it('reshards from two workgroups to three under load', async () => {
            step(9, 'plan the reshard: which destinations move, which stay');
            const before = zk.mapping(zk.workgroupsDoc(), DESTS);
            tool('plan', GEN3);
            const fromOffsets = {};
            DESTS.forEach(d => {
                fromOffsets[d] = kafka.head(ctx.customerTopicOf[d], 0);
            });

            step(10, 'run the cutover with traffic flowing');
            note('the tool writes a barrier record on every partition, then');
            note('the document with those barrier offsets, then pre-seeds the');
            note('new generation\'s groups while they are still empty. The');
            note('ZooKeeper write is the commit point.');
            const cut = tool('cutover', GEN3);
            assert.strictEqual(cut.code, 0, 'the reshard cutover failed');
            const doc = zk.workgroupsDoc();
            line(zk.describeDoc(doc).map(l => `      | ${l}`).join('\n'));
            const after = zk.mapping(doc, DESTS);
            const moved = DESTS.filter(d => before[d] !== after[d]);
            const stayed = DESTS.filter(d => before[d] === after[d]);
            say(`moved: ${moved.map(d => `${d} ${before[d]}->${after[d]}`).join(', ')}`);
            say(`stayed: ${stayed.join(', ')}`);
            act.measured('destinations that moved',
                `${moved.length} of ${DESTS.length}`);
            watch('grafana', 'row "Workgroups": cutover barriers seen, and the '
                + 'generation per worker');

            step(11, 'start generation 3, then wait for verify to exit 0');
            await startWorkgroupWorkers(['wg-a', 'wg-b', 'wg-c', 'wg-pin'], 3);
            await ensureDelivering(['wg-a', 'wg-b', 'wg-c', 'wg-pin'], 3, 240000);
            if (STOP_EARLY) {
                note('DEMO_WORKGROUPS_STOP_EARLY is set: generation 2 is being');
                note('stopped BEFORE verify exits 0, which is the operator');
                note('error the tool warns about. Expect loss, and expect the');
                note('drain report to have warned about exactly those records.');
                const v = tool('verify', []);
                say(`verify exit code ${v.code} (2 means not drained)`);
                stopGeneration(['wg-a', 'wg-b', 'wg-pin'], 2);
                act.measured('verify exit code before stopping the old generation',
                    v.code);
            } else {
                const verified = await waitForDrain(
                    ['wg-a', 'wg-b', 'wg-pin'], 2, 600000);
                act.measured('verify exit code before stopping the old generation',
                    verified ? 0 : 2);
                assert.ok(verified, 'verify never reached 0');
                note('every previous group is past every barrier, so the old');
                note('generation can be stopped now, and only now');
                stopGeneration(['wg-a', 'wg-b', 'wg-pin'], 2);
            }
            await procs.sleep(env.pause(10000));

            step(12, 'stop the load, drain generation 3, check every destination');
            load.proc.stop();
            await wait.until('the driver to stop', () => !load.proc.isRunning(),
                90000, 1000);
            await wait.frozen(env.DELIVERY_TOPIC, env.pause(15000));
            for (const id of ['wg-a', 'wg-b', 'wg-c', 'wg-pin']) {
                const group = zk.groupIdFor(env.DELIVERY_GROUP, id, 3);
                 
                await wait.drain({ group, topic: env.DELIVERY_TOPIC,
                    label: `${id} gen3`, timeoutMs: 240000 });
            }
            let gaps = 0;
            let dups = 0;
            let inversions = 0;
            DESTS.forEach(d => {
                const r = flow.dumpAndCheck({ act,
                    topic: ctx.customerTopicOf[d],
                    from: from[d],
                    driver: load.log,
                    bucket: BUCKETS[d],
                    label: d });
                gaps += r.totals.gaps;
                dups += r.totals.duplicate_extras;
                inversions += r.totals.inversions;
            });
            act.measured('reshard gaps (loss)', gaps);
            act.measured('reshard duplicates', dups);
            act.measured('reshard inversions', inversions);
            act.measured('gaps (loss), generation 1', gaps);
            note('duplicates are the old generation\'s consumption past its');
            note('barriers: the longer it runs after the barrier, the more');
            note('there are. Gaps are impossible once verify has exited 0,');
            note('which is the property the barrier buys.');
            note('known gap, from the reshard study: a crashed old-generation');
            note('worker cannot restart to finish its drain once the document');
            note('has been overwritten. The proposed amendment is one');
            note('ZooKeeper node per generation plus a current pointer.');

            if (STOP_EARLY) {
                say(`stopped early on purpose: ${gaps} records lost, and the`);
                say('drain report above had already counted them as remaining');
            } else {
                assert.strictEqual(gaps, 0,
                    'the reshard lost records even though verify exited 0');
                assert.strictEqual(inversions, 0,
                    'the reshard reordered a key');
            }
        });
    });
}

module.exports = { register };
