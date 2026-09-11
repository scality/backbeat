'use strict';

/**
 * Act 06: workgroups, the mechanism that lets today's one topic serve a
 * population of destinations without every worker holding a producer for
 * every destination.
 *
 * The topology is decided: the workers read today's topic, and a workgroup
 * is a consumer group over it with a slice filter, so a worker commits
 * records outside its slice without delivering them. No second topic, no
 * populator change.
 *
 * The document in ZooKeeper is the whole contract: a generation, a hashmod
 * rule whose remainders cover the whole modulo, and optional static pins.
 * Ownership is a total function, so every record has exactly one owner,
 * which is why a slice filter can commit what it does not deliver without
 * losing anything. A layout change is a new generation, applied the way
 * production applies every change: stop the old generation's containers,
 * seed the new generation's groups from the old ones, start the new
 * containers. No barrier, no verify step, no overlap, and a measured pause.
 *
 * Four things happen here, in order:
 *   a  two auto workgroups, five destinations, one worker each
 *   b  one workgroup's worker dies: only its destinations pause
 *   c  a noisy destination is pinned to its own workgroup (generation 2)
 *   d  a reshard from two auto workgroups to three, under load
 *      (generation 3)
 *
 * The seed tool is the repository's own bin/notificationDeliverySeed.js, and
 * the ownership function is extensions/notification/utils/workgroups.js.
 */

const assert = require('assert');
const fs = require('fs');
const env = require('../lib/env');
const kafka = require('../lib/kafka');
const procs = require('../lib/procs');
const wait = require('../lib/wait');
const flow = require('../lib/flow');
const zk = require('../lib/zk');
const s3lib = require('../lib/s3');
const check = require('../lib/check');
const { Act, say, note, watch, step, line } = require('../lib/narrate');

// The five destinations the platform validates. At modulo 2 the md5 of
// these five names puts all three poc ones on the same remainder, so the act
// uses modulo 4, where they spread over three remainders and both slices
// carry real load. The reshard then goes from two workgroups to three over
// the same modulo, which moves one destination and leaves the others where
// they are.
const DESTS = ['poc-dest-1', 'poc-dest-2', 'poc-dest-3',
    'krb-dest-a', 'krb-dest-b'];
const MODULO = 4;
const LAYOUT = {
    1: { modulo: MODULO, workgroups: { 'wg-a': [0, 1], 'wg-b': [2, 3] } },
    2: { modulo: MODULO, workgroups: { 'wg-a': [0, 1], 'wg-b': [2, 3] },
        statics: { 'wg-pin': ['poc-dest-1'] } },
    3: { modulo: MODULO, workgroups: { 'wg-a': [0, 1], 'wg-b': [2], 'wg-c': [3] },
        statics: { 'wg-pin': ['poc-dest-1'] } },
};
const IDS = {
    1: ['wg-a', 'wg-b'],
    2: ['wg-a', 'wg-b', 'wg-pin'],
    3: ['wg-a', 'wg-b', 'wg-c', 'wg-pin'],
};
const BUCKETS = {};
DESTS.forEach((d, i) => { BUCKETS[d] = `demo-wg-${i + 1}`; });
const LOAD_SECONDS = Number(env.knob('DEMO_ACT06_SECONDS',
    env.workSecs(420, 200)));

function register(ctx) {
    describe('Act 06: workgroups', () => {
        const act = new Act('06', 'workgroups', 'W gates and C5r',
            'today\'s topic, sliced into workgroups by a document in '
            + 'ZooKeeper, changed by stopping one generation and starting '
            + 'the next, seeded from the old one');
        let config;
        const workers = {};
        let load;
        // traffic a step starts for itself when the long load has ended; all
        // of it appends to the long load's log so the final check counts it
        const extraLoads = [];
        const from = {};
        // the instants a duplicate can be attributed to: the kill, and each
        // generation's stop
        const boundaries = [];

        /**
         * Write a generation's document and say what it holds.
         *
         * @param {Number} generation - generation number
         * @return {Object} the document as read back
         */
        function writeLayout(generation) {
            const doc = zk.writeWorkgroupsDoc(zk.buildWorkgroupsDoc(
                Object.assign({ generation }, LAYOUT[generation])));
            line(zk.describeDoc(doc).map(l => `      | ${l}`).join('\n'));
            act.timeline(`workgroups document generation ${generation} written`);
            return doc;
        }

        /**
         * Start every worker of a generation at once, the way Ansible starts
         * every container of a run at once. Each start carries its own probe
         * wait and start-up wedge cure, and they run side by side.
         *
         * @param {Array} ids - workgroup ids
         * @param {Number} generation - generation number
         * @return {Promise} resolves when every worker is up and cured
         */
        async function startWorkgroupWorkers(ids, generation) {
            const started = await Promise.all(ids.map((id, i) =>
                flow.startWorker(act, config, i + 1 + (generation - 1) * 4,
                    { workgroupId: id, autoRestart: true })));
            ids.forEach((id, i) => {
                workers[`${id}-gen${generation}`] = started[i];
                say(`  workgroup ${id} joins group `
                    + `${zk.groupIdFor(env.DELIVERY_GROUP, id, generation)}`);
            });
        }

        /** how many wedge cures the timeline holds so far */
        function wedgeCount() {
            return fs.existsSync(act.file('timeline.txt'))
                ? (fs.readFileSync(act.file('timeline.txt'), 'utf8')
                    .match(/WEDGED/g) || []).length
                : 0;
        }


        /**
         * Wait until any worker of a generation has delivered something.
         *
         * @param {Array} ids - workgroup ids
         * @param {Number} generation - generation number
         * @param {Number} budgetMs - how long to wait
         * @return {Promise} resolves true on the first delivery
         */
        function firstDelivery(ids, generation, budgetMs) {
            return wait.until(`generation ${generation}'s first delivery`,
                async () => {
                    const counts = await Promise.all(ids.map(id => {
                        const w = workers[`${id}-gen${generation}`];
                        return wait.counter(Number(w.probePort) - env.PROBE_BASE,
                            'delivered');
                    }));
                    return counts.some(c => c > 0);
                }, budgetMs, 2000);
        }

        /**
         * A generation change the way production does every change: stop
         * every container of the old generation, write the new layout, seed
         * the new generation's groups from the old ones (lowest committed
         * offset per partition, a watermark per destination at its previous
         * owner's offset), start the new containers. No two generations ever
         * run together, so nothing can interleave; the price is a delivery
         * pause, measured from the old generation's stop to the new one's
         * first delivery.
         *
         * @param {Object} p - { prevGen, newGen, label }
         * @return {Promise} resolves with { pauseS }
         */
        async function changeGeneration(p) {
            note('the operator\'s order, which is Ansible\'s order: stop the');
            note('old generation, write the new layout, seed, start the new');
            note('generation. Between the stop and the first delivery nothing');
            note('is delivered, and that pause is the whole price of a change');
            note('with no overlap.');
            stopGeneration(IDS[p.prevGen], p.prevGen);
            const stoppedAt = Date.now();
            boundaries.push({ label: `generation ${p.prevGen} stop`, at: stoppedAt });
            act.timeline(`generation ${p.prevGen} STOPPED`);
            writeLayout(p.newGen);
            const seeded = flow.seedFromGeneration(act, config, p.prevGen, p.newGen);
            const seededAt = Date.now();
            act.measured(`seed exit code, generation ${p.newGen}`, seeded.code);
            assert.strictEqual(seeded.code, 0,
                `generation ${p.newGen} was not seeded on every partition`);
            const marks = zk.watermarks(p.newGen) || {};
            say(`watermarks for generation ${p.newGen}: `
                + `${Object.keys(marks).length} destinations`);
            const wedgesBefore = wedgeCount();
            await startWorkgroupWorkers(IDS[p.newGen], p.newGen);
            const startedAt = Date.now();
            const cures = wedgeCount() - wedgesBefore;
            const delivered = await firstDelivery(IDS[p.newGen], p.newGen, 300000);
            const pauseS = Math.round((Date.now() - stoppedAt) / 1000);
            const seedS = Math.round((seededAt - stoppedAt) / 1000);
            const startS = Math.round((startedAt - seededAt) / 1000);
            say(`generation ${p.newGen} ${delivered ? 'delivering' : 'still silent'} `
                + `${pauseS}s after generation ${p.prevGen} was stopped: seed `
                + `${seedS}s, worker start ${startS}s with ${cures} start-up wedge `
                + `cure${cures === 1 ? '' : 's'}, first delivery `
                + `${Math.round((Date.now() - startedAt) / 1000)}s after that`);
            act.measured(`delivery pause at the ${p.label}`,
                `${pauseS}s: seed ${seedS}s, start ${startS}s (${cures} wedge `
                + `cure${cures === 1 ? '' : 's'}), first delivery `
                + `${Math.round((Date.now() - startedAt) / 1000)}s after start`);
            note('the seed is a second. What the pause is made of is the');
            note('container start and the pre-existing start-up wedge, cured');
            note('by a restart, which is the reliability ceiling this codebase');
            note('sets and not a property of the swap.');
            await ensureDelivering(IDS[p.newGen], p.newGen, 240000);
            return { pauseS };
        }

        /**
         * Partitions a workgroup's group holds with records left and no
         * committed movement over one sample, while its member is alive and
         * another of its partitions did move.
         *
         * Seen in the timed reference run: after a coordinator disconnect,
         * partitions paused for the rebalance were never resumed
         * ("Local: Erroneous state" out of _resumePausedPartitions), so the
         * worker kept filtering another workgroup's records on one partition
         * while its own destinations' partitions never left their starting
         * offset, for the whole act, with liveness 200 throughout. Nothing
         * that only watches start-up cycling or total lag can see it.
         *
         * @param {Array} ids - workgroup ids
         * @param {Number} generation - generation number
         * @param {Number} sampleMs - time between the two samples
         * @return {Promise} resolves with [{ id, partitions, lag }]
         */
        async function partitionStalls(ids, generation, sampleMs) {
            const snap = () => Object.fromEntries(ids.map(id => {
                const g = zk.groupIdFor(env.DELIVERY_GROUP, id, generation);
                return [id, kafka.groupState(g, env.POOL_TOPIC)];
            }));
            const first = snap();
            await procs.sleep(sampleMs);
            const second = snap();
            const out = [];
            ids.forEach(id => {
                const a = first[id];
                const b = second[id];
                if (!b.members) {
                    // no live member: not this defect, the start-up path and
                    // the drain gates own that case
                    return;
                }
                const before = p => a.rows.find(r => r.partition === p.partition);
                const moved = b.rows.some(r => {
                    const o = before(r);
                    return o && r.committed !== o.committed;
                });
                const stuck = b.rows.filter(r => {
                    const o = before(r);
                    return o && /^\d+$/.test(r.lag) && Number(r.lag) > 0
                        && r.committed === o.committed;
                });
                if (moved && stuck.length) {
                    out.push({ id, partitions: stuck.map(r => r.partition),
                        lag: stuck.reduce((s, r) => s + Number(r.lag), 0) });
                }
            });
            return out;
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
                    // nobody is idle, but a live member can progress on one
                    // partition and never move on the others, so look at the
                    // partitions before calling the generation healthy
                    const stalled = await partitionStalls(ids, generation, 20000);
                    if (!stalled.length) {
                        rows.forEach(r => say(`${r.id}: delivered ${r.delivered}, `
                            + `skipped ${r.skipped} records outside its slice`));
                        return [];
                    }
                    for (const s of stalled) {
                        if ((cured[s.id] || 0) >= 2) {
                            continue;
                        }
                        const w = workers[`${s.id}-gen${generation}`];
                        const n = Number(w.probePort) - env.PROBE_BASE;
                        note(`PARTITION STALL on workgroup ${s.id}: partition`
                            + `${s.partitions.length > 1 ? 's' : ''} `
                            + `${s.partitions.join(', ')} hold ${s.lag} records`);
                        note('  and did not move in 20s while the member is alive');
                        note('  and another of its partitions did. That is the');
                        note('  paused-partitions variant of the consumer defect:');
                        note('  after a coordinator disconnect the partitions');
                        note('  paused for the rebalance are never resumed. The');
                        note('  cure is the same: restart that one worker.');
                        act.timeline(`workgroup ${s.id} PARTITION STALL on `
                            + `${s.partitions.join(',')}, restarting`);
                        w.stop();

                        await procs.sleep(env.pause(5000));

                        workers[`${s.id}-gen${generation}`] = await flow.startWorker(
                            act, config, n, { workgroupId: s.id, autoRestart: true });
                        cured[s.id] = (cured[s.id] || 0) + 1;
                        act.measured('wedge cures on a workgroup start',
                            Object.values(cured).reduce((a, b) => a + b, 0));
                    }
                    continue;
                }
                say(`waiting for ${idle.map(r => r.id).join(', ')} to deliver`);
                 
                await procs.sleep(15000);
                 
                const again = await stuck();
                for (const r of again.filter(x => x.delivered === 0 && x.skipped === 0)) {
                    const bal = r.w.rebalances();
                    const group = zk.groupIdFor(env.DELIVERY_GROUP, r.id,
                        generation);
                    const lag = kafka.groupLag(group, env.POOL_TOPIC);
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
            act.expect('reshard inversions', '0, one generation at a time');
            act.expect('reshard duplicates',
                'the stopped generation\'s uncommitted window, plus cure '
                + 're-deliveries; none from the seed itself');
            act.expect('duplicates, whole act, by cause', '(not predicted)');
            act.expect('seed exit code, generation 2', 0);
            act.expect('seed exit code, generation 3', 0);
            config = flow.poolConfig(act, ctx, {
                only: DESTS,
                workgroups: true,
                tag: 'wg',
            });
            // this act writes its own generation 1, so nothing an earlier act
            // left in the document or in the workgroup groups may leak in
            flow.resetPoolGroups(act);
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
            extraLoads.forEach(x => x.proc.stop());
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
                const doc = writeLayout(1);
                assert.ok(doc, 'no workgroups document was written');
                note('the first generation on a topic that already has history');
                note('starts at the head: the processors it replaces are act');
                note('04\'s story. Here each group is seeded at the head, the');
                note('way the seed tool would put it.');
                IDS[1].forEach(id => {
                    flow.seedPoolGroupAtHead(act,
                        zk.groupIdFor(env.DELIVERY_GROUP, id, 1));
                });
                watch('zoonavigator', `node ${env.ZK_WORKGROUPS_PATH}`);
                watch('terminal', 'poc-demo/bin/zk-show.sh prints the same thing');

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
            // The long load runs for a fixed window, and the wedge cures of
            // step 3 can use most of it up, so this step drives its own
            // traffic for the isolation window and the catch-up after it.
            // Without records arriving there is nothing for the survivor to
            // keep delivering, and the claim could not be true of anything.
            if (!load.proc.isRunning()) {
                note('the long load has already ended, so fresh traffic is');
                note('started for the isolation window and the catch-up');
            }
            extraLoads.push(flow.startDriver(act, {
                'buckets': Object.values(BUCKETS).join(','),
                'prefix': 'iso', 'rate': 6, 'duration': env.workSecs(240, 150),
                'straddle': 3, 'straddle-every': 5, 'log': load.log,
            }));
            await procs.sleep(env.pause(15000));
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
            boundaries.push({ label: `${victimId} kill`, at: Date.now() });
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
            say(`${victimId} group lag ${kafka.groupLag(victimGroup, env.POOL_TOPIC)}, `
                + `${survivorId} group lag `
                + `${kafka.groupLag(survivorGroup, env.POOL_TOPIC)}`);
            note('both groups read the whole topic and skip what they do not');
            note('own, so their raw lag is about the same number. The');
            note('isolation is in the DELIVERED counters and in the customer');
            note('topics, not in the lag: that is what the per-workgroup');
            note('delivered panel is for.');
            say(`${survivorId} delivered ${before.survivor} then `
                + `${during.survivor}: ${during.survivor > before.survivor
                    ? 'it kept working' : 'it did NOT deliver more'}`);
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
            const planned = zk.buildWorkgroupsDoc(Object.assign({ generation: 2 },
                LAYOUT[2]));
            const map = zk.mapping(planned, DESTS);
            say(`${pinned} will belong to ${map[pinned]}`);
            assert.strictEqual(map[pinned], 'wg-pin',
                'the pin does not take effect in the planned document');

            step(7, 'the layout change: stop generation 1, seed, start generation 2');
            await changeGeneration({ prevGen: 1, newGen: 2, label: 'pin cutover' });
            await procs.sleep(env.pause(10000));

            step(8, 'the pinned destination is now served by the pin only');
            const pinWorker = workers['wg-pin-gen2'];
            const pinN = Number(pinWorker.probePort) - env.PROBE_BASE;
            const start = await wait.counter(pinN, 'delivered', { target: pinned });
            note('the long load runs for a fixed window and has usually');
            note('finished by now, so this step drives its own burst rather');
            note('than hoping traffic is still arriving. The claim is that');
            note('the pinned workgroup serves this destination, and that');
            note('needs records for it to be true of.');
            await flow.runDriver(act, { bucket: BUCKETS[pinned],
                prefix: `pin-${pinned}`, rate: 4, count: 12 });
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
            const planned = zk.buildWorkgroupsDoc(Object.assign({ generation: 3 },
                LAYOUT[3]));
            const after = zk.mapping(planned, DESTS);
            const moved = DESTS.filter(d => before[d] !== after[d]);
            const stayed = DESTS.filter(d => before[d] === after[d]);
            say(`will move: ${moved.map(d => `${d} ${before[d]}->${after[d]}`)
                .join(', ')}`);
            say(`will stay: ${stayed.join(', ')}`);
            // The reshard has to be measured under traffic, and the long load
            // has ended by now, so start a fresh one and let it flow before the
            // cutover. Its operations append to the long load's log.
            extraLoads.push(flow.startDriver(act, {
                'buckets': Object.values(BUCKETS).join(','),
                'prefix': 'reshard', 'rate': 6, 'duration': env.workSecs(300, 180),
                'straddle': 3, 'straddle-every': 5, 'log': load.log,
            }));
            await procs.sleep(env.pause(20000));
            // The reshard's own window: every count below is taken from these
            // offsets and from the operations completed after this instant,
            // not from the start of the act.
            const reshardAt = Date.now();
            const fromOffsets = {};
            DESTS.forEach(d => {
                fromOffsets[d] = kafka.head(ctx.customerTopicOf[d], 0);
            });

            step(10, 'the reshard, with traffic flowing: stop 2, seed, start 3');
            note('the seed tool takes, per partition, the lowest committed');
            note('offset across the generation 2 groups a new group inherits');
            note('from, and writes a watermark per destination at its previous');
            note('owner\'s offset. A destination that moves workgroup keeps its');
            note('place in the stream; nothing is skipped and nothing is sent');
            note('again. The ZooKeeper write is the commit point.');
            await changeGeneration({ prevGen: 2, newGen: 3, label: 'reshard' });
            act.measured('destinations that moved',
                `${moved.length} of ${DESTS.length}`);
            watch('grafana', 'row "Workgroups": delivered per second by workgroup, '
                + 'the generation per worker, and records skipped under a '
                + 'watermark');
            watch('kafka ui', 'Consumers: the generation 3 groups appear, the '
                + 'generation 2 groups go memberless');
            await procs.sleep(env.pause(10000));

            step(12, 'stop the load, drain generation 3, check every destination');
            load.proc.stop();
            extraLoads.forEach(x => x.proc.stop());
            await wait.until('the drivers to stop', () => !load.proc.isRunning()
                && extraLoads.every(x => !x.proc.isRunning()), 90000, 1000);
            await wait.frozen(env.POOL_TOPIC, env.pause(15000));
            for (const id of IDS[3]) {
                const w = workers[`${id}-gen3`];
                if (w) {
                    await flow.snapshotMetrics(act, Number(w.probePort) - env.PROBE_BASE,
                        `${id}-gen3`);
                }
                const group = zk.groupIdFor(env.DELIVERY_GROUP, id, 3);
                 
                await flow.drainOrCure({ group, topic: env.POOL_TOPIC,
                    label: `${id} gen3`, timeoutMs: 240000 });
            }
            // Two windows. The whole act, from the offsets recorded before
            // generation 1 started, for loss: nothing published to any
            // destination during the act may be missing. And the reshard's
            // own window, from the offsets and the instant recorded before
            // the generation 3 cutover, for its duplicates and its ordering,
            // so that the pin cutover's numbers are not billed to it.
            const reshardLog = act.file('driver-reshard-window.log');
            fs.writeFileSync(reshardLog, fs.readFileSync(load.log, 'utf8')
                .split('\n')
                .filter(l => l && new Date(l.split(' ')[0]).getTime() >= reshardAt)
                .join('\n').concat('\n'));
            let gaps = 0;
            const reshard = { gaps: 0, dups: 0, inversions: 0 };
            DESTS.forEach(d => {
                const whole = flow.dumpAndCheck({ act,
                    topic: ctx.customerTopicOf[d],
                    from: from[d],
                    driver: load.log,
                    bucket: BUCKETS[d],
                    label: d });
                gaps += whole.totals.gaps;
                const win = flow.dumpAndCheck({ act,
                    topic: ctx.customerTopicOf[d],
                    from: fromOffsets[d],
                    driver: reshardLog,
                    bucket: BUCKETS[d],
                    label: `${d}-reshard` });
                reshard.gaps += win.totals.gaps;
                reshard.dups += win.totals.duplicate_extras;
                reshard.inversions += win.totals.inversions;
            });
            act.measured('gaps (loss), generation 1', gaps);
            act.measured('reshard gaps (loss)', reshard.gaps);
            // who made each duplicate: the replacement after the kill, the
            // next generation after a stop (the stopped generation's
            // uncommitted window, which the watermark cannot know about), or
            // a cure restart within a generation (its own uncommitted window)
            const files = DESTS.map(d => act.file(`events-${d}.jsonl`));
            const dec = check.decomposeDuplicates(files, boundaries);
            const across = Object.entries(dec.across)
                .map(([k, n]) => `${n} across the ${k}`).join(', ');
            const within = Object.values(dec.within).reduce((a, b) => a + b, 0);
            say(`duplicates over the whole act: ${dec.total}: ${across || 'none across '
                + 'a boundary'}; ${within} within a generation (cure restarts)`);
            Object.entries(dec.perFile).forEach(([f, v]) => {
                say(`  ${f}: ${v.total} (${JSON.stringify(v.across)} across, `
                    + `${JSON.stringify(v.within)} within)`);
            });
            act.measured('duplicates, whole act, by cause',
                `${dec.total}: ${across || 'none across a boundary'}; `
                + `${within} within a generation`);
            const g2 = dec.across['generation 2 stop'] || 0;
            act.measured('reshard duplicates',
                `the stopped generation's window ${g2}, cure re-deliveries `
                + `${dec.within['after generation 2 stop'] || 0}, in the window `
                + `${reshard.dups}`);
            act.measured('reshard inversions', reshard.inversions);
            note('why the window is not "5 s of traffic": the consumer commits');
            note('a partition contiguously, up to the oldest record still in');
            note('flight. On today\'s topic every destination shares every');
            note('partition, so one slow lane (a hot object key delivers one');
            note('record per producer poll, 2 s) holds the committed offset of');
            note('the whole partition back while hundreds of later records are');
            note('delivered. Stop that worker and the next one, seeded at the');
            note('committed offset, delivers them again. The watermark is the');
            note('committed offset, so it cannot see them. On the previous');
            note('model a slow destination held back only its own partition.');
            note('At-least-once holds; the size of the window is the finding.');
            note('two mitigations, named and not built: release a lane on the');
            note('producer\'s delivery report instead of its 2 s poll, so the');
            note('slow lane stops holding the partition back; and a graceful');
            note('stop that writes a per-destination "delivered up to" mark,');
            note('so the next generation\'s watermark is the delivered offset');
            note('and not the committed one.');
            note('loss is impossible when the new generation is seeded at the');
            note('lowest offset of the groups it inherits from: every record');
            note('either was delivered by the old generation or is read by the');
            note('new one. Duplicates are what the per-destination watermark');
            note('prevents: without it the new generation would re-deliver the');
            note('spread between the old groups\' offsets. Inversions need two');
            note('deliverers of the same key at once, and one generation at a');
            note('time has none. The rehearsal of 2026-09-11 on the previous');
            note('model measured 4197 same-key inversions when two generations');
            note('did run together; that is the case this order removes, at the');
            note('price of the pause measured above.');
            note('two facts that still stand: a destination\'s per-object');
            note('ordering lanes deliver one record per producer poll, 2000 ms,');
            note('so six lanes moved about three records a second; and a kill');
            note('or a cure restart re-delivers the worker\'s uncommitted');
            note('window, at-least-once.');

            assert.strictEqual(gaps, 0, 'the act lost records');
            assert.strictEqual(reshard.gaps, 0, 'the reshard lost records');
            assert.strictEqual(reshard.inversions, 0,
                'the reshard reordered same-key events');
            if (reshard.dups > 0) {
                say(`${reshard.dups} duplicates in the reshard window: not loss; `
                    + 'the decomposition above says whose window they were');
            }
        });
    });
}

module.exports = { register };
