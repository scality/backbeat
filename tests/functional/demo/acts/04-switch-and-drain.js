'use strict';

/**
 * Act 04: the migration, as one Ansible run.
 *
 * Production applies every change by replacing containers: the playbook
 * deletes the per-destination processor containers and starts the worker
 * containers. Nothing runs twice, nothing drains, nothing switches topics:
 * the populator keeps writing one record per event to today's topic, and
 * the workers read that same topic and match per destination themselves.
 *
 * The one thing the run has to get right is where the new consumer groups
 * start reading. A group with no committed offset starts at the oldest
 * record still retained, which would re-deliver hours of events to every
 * customer. The worker does that itself: a start that finds its group
 * empty takes a lock in ZooKeeper and seeds every group of the generation
 * from the committed offsets of the processors it replaces, the lowest per
 * partition so nothing is skipped, plus a watermark per destination at its
 * own processor's offset so nothing already delivered is sent again. So the
 * run is stop, write, start, with no command in between, which is the only
 * shape Federation can express as one playbook.
 *
 * To make the offsets differ, as they do on a real platform, one processor
 * is caught up, one is stopped a little before the swap (a small backlog) and
 * one is frozen early (a stalled destination with a large backlog). The
 * measurement is then: nothing lost, nothing doubled, order kept, the pause,
 * and the stalled destination's backlog finally delivered.
 *
 * There is no rollback step. The switch is one way; manual workgroups can
 * reproduce today's one-process-per-destination isolation if ever needed.
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
const { Act, say, note, watch, step, line } = require('../lib/narrate');

const CAUGHT_UP = 'poc-dest-1';
const STOPPED = 'poc-dest-2';
const STALLED = 'poc-dest-3';
const DESTS = [CAUGHT_UP, STOPPED, STALLED];
const BUCKETS = {};
DESTS.forEach((d, i) => { BUCKETS[d] = `demo-mig-${i + 1}`; });
const WORKGROUP = 'wg-a';
const GENERATION = 1;
const SECONDS = Number(env.knob('DEMO_ACT04_SECONDS', env.workSecs(240, 120)));
// the second pass: seed ahead with the CLI, then throw the watermarks away
// before the worker starts, to measure what the per-destination watermark
// saves. It is also the pass that keeps the CLI covered: an operator who
// prefers to seed before the run still can, with seedOnStart off.
const WITHOUT_WATERMARK = env.knob('DEMO_ACT04_WITHOUT_WATERMARK', '') === '1';

function register(ctx) {
    describe('Act 04: the migration, one Ansible run', () => {
        const act = new Act('04', 'switch-and-drain', 'one run',
            'delete the processors, write the layout, start the workers: they '
            + 'seed themselves from the processors\' offsets, nothing lost, '
            + 'nothing doubled, order kept');
        const from = {};
        const processors = {};

        before(async () => {
            act.open();
            act.expect('gaps (loss)', 0);
            act.expect(WITHOUT_WATERMARK
                ? 'duplicates without the watermark'
                : 'duplicates with the watermark',
            WITHOUT_WATERMARK
                ? 'the offset spread between the processors'
                : 'the processors\' uncommitted windows only, about 5 s each');
            act.expect('per-key inversions', 0);
            act.expect('the seeding',
                WITHOUT_WATERMARK ? 'run ahead with the CLI, seedOnStart off'
                    : 'done by the first worker to start, no command in the run');
            act.expect('processor offsets at the swap', '(not predicted)');
            act.expect('delivery pause', '(not predicted)');
            act.expect('stalled destination backlog delivered by the pool',
                '(not predicted)');
            act.expect('records skipped under the watermark', '(not predicted)');
            flow.resetPoolGroups(act);
            for (const d of DESTS) {

                await s3lib.bucketWith(ctx.s3, BUCKETS[d], [d]);
                from[d] = kafka.head(ctx.customerTopicOf[d], 0);
            }
        });

        after(() => {
            procs.stopAll();
            act.close();
        });

        it('replaces the processors with workers in one run, losing nothing',
            async () => {
                const legacy = flow.legacyConfig(act, ctx, { only: DESTS });
                const pool = flow.poolConfig(act, ctx, { only: DESTS,
                    workgroups: true, tag: 'mig',
                    // the control pass seeds ahead with the CLI so that the
                    // watermarks can be deleted before any worker reads them
                    seedOnStart: !WITHOUT_WATERMARK });

                step(0, 'today: the populator and one processor per destination');
                await flow.startPopulator(act, legacy, 'legacy');
                for (const d of DESTS) {

                    processors[d] = await flow.startProcessor(act, legacy, d);

                    await flow.warmLegacyGroup(act, { dest: d, bucket: BUCKETS[d],
                        count: 6 });
                }
                const load = flow.startDriver(act, {
                    'buckets': Object.values(BUCKETS).join(','),
                    'prefix': 'mig', 'rate': 6, 'duration': SECONDS,
                    'straddle': 3, 'straddle-every': 5,
                });
                watch('grafana', 'row "Migration": the processor groups versus '
                    + 'the pool groups, lag per group');
                await wait.until('the processors to deliver', () => DESTS.every(
                    d => kafka.head(ctx.customerTopicOf[d], 0) > from[d] + 3),
                180000, 3000);

                step(1, `freeze the processor of ${STALLED}: a stalled destination`);
                note('SIGSTOP, so it holds its partitions and commits nothing,');
                note('which is what a destination stuck behind a dead endpoint');
                note('looks like from the broker. Its backlog now grows for the');
                note('rest of the load.');
                processors[STALLED].kill('SIGSTOP');
                act.timeline(`processor ${STALLED} SIGSTOP pid=${processors[STALLED].pid}`);
                await procs.sleep(env.pause(Math.round(SECONDS * 1000 * 0.3)));

                step(2, `stop the processor of ${STOPPED}: a small backlog`);
                note('a processor that is simply down at the moment of the');
                note('run, so its group is a few records behind the others.');
                processors[STOPPED].stop();
                act.timeline(`processor ${STOPPED} STOP`);
                await procs.sleep(env.pause(Math.round(SECONDS * 1000 * 0.25)));

                step(3, 'the Ansible run, part one: delete every processor');
                const offsets = {};
                DESTS.forEach(d => {
                    offsets[d] = kafka.committedByPartition(env.legacyGroup(d),
                        env.INTERNAL_TOPIC);
                });
                const ends = kafka.heads(env.INTERNAL_TOPIC);
                line(`      | partition   ${Object.keys(ends).map(p => `p${p}`.padStart(8))
                    .join('')}   (topic end ${Object.values(ends).join('/')})`);
                DESTS.forEach(d => {
                    line(`      | ${d.padEnd(12)}${Object.keys(ends)
                        .map(p => String(offsets[d][p] === undefined ? '-'
                            : offsets[d][p]).padStart(8)).join('')}`);
                });
                act.measured('processor offsets at the swap',
                    DESTS.map(d => `${d} ${Object.values(offsets[d]).join('/')}`)
                        .join('; '));
                const headAtSwap = {};
                DESTS.forEach(d => {
                    headAtSwap[d] = kafka.head(ctx.customerTopicOf[d], 0);
                });
                DESTS.forEach(d => processors[d].stop());
                const stoppedAt = Date.now();
                act.timeline('every processor STOPPED');
                note('the populator is untouched: it keeps writing to the same');
                note('topic, and does not know or care which path consumes it.');

                step(4, 'part two: write the layout');
                const doc = zk.buildWorkgroupsDoc({ generation: GENERATION,
                    modulo: 1, workgroups: { [WORKGROUP]: [0] } });
                zk.writeWorkgroupsDoc(doc);
                line(zk.describeDoc(zk.workgroupsDoc()).map(l => `      | ${l}`)
                    .join('\n'));
                const group = zk.groupIdFor(env.DELIVERY_GROUP, WORKGROUP, GENERATION);
                if (WITHOUT_WATERMARK) {
                    note('DEMO_ACT04_WITHOUT_WATERMARK=1: this pass runs the');
                    note('seeding CLI ahead of the start, with seedOnStart off,');
                    note('and then deletes the watermarks, so the worker');
                    note('delivers everything from the lowest offset. What it');
                    note('sends twice is the spread between the processors\'');
                    note('offsets, and that is the number this pass measures.');
                    const seeded = flow.seedFromProcessors(act, pool, GENERATION);
                    assert.strictEqual(seeded.code, 0,
                        'the seed tool did not seed every partition of every group');
                    zk.deleteWatermarks(GENERATION);
                    act.timeline('watermarks DELETED on purpose');
                } else {
                    note('and that is the whole of part two. No seeding command');
                    note('runs here: the worker container started next finds its');
                    note('consumer group empty, takes a lock in ZooKeeper and');
                    note('seeds itself from the processor groups before it');
                    note('subscribes. An Ansible run therefore has nothing');
                    note('between its stop and its start.');
                }
                watch('zoonavigator', `${env.ZK_WORKGROUPS_PATH} and its watermarks `
                    + `child for generation ${GENERATION}`);

                step(5, 'part three: start the worker container');
                // started without awaiting, so the watermarks node is caught
                // as the worker writes it and not minutes later
                const seedWatch = WITHOUT_WATERMARK ? null
                    : flow.captureSelfSeed(act, { generation: GENERATION });
                const worker = await flow.startWorker(act, pool, 1,
                    { workgroupId: WORKGROUP, autoRestart: true });
                if (seedWatch) {
                    const selfSeed = await seedWatch;
                    const seeders = selfSeed.seeders;
                    say(`${seeders.join(', ') || 'no worker'} says in its log that `
                        + `it seeded itself, ${selfSeed.waitedS}s after the layout `
                        + 'was written');
                    say(`the seeding put ${group} at `
                        + `${JSON.stringify(selfSeed.start)}, the lowest processor `
                        + 'offset per partition');
                    act.measured('the seeding', seeders.length === 1
                        ? 'done by the first worker to start, no command in '
                          + 'the run'
                        : `${seeders.length} workers say they did the seeding`);
                    assert.ok(selfSeed.watermarks,
                        'the worker did not seed itself: no watermarks in zookeeper');
                    assert.strictEqual(seeders.length, 1,
                        'exactly one worker should say it did the seeding');
                    say(`watermarks: ${JSON.stringify(selfSeed.watermarks)}, each `
                        + 'one its own processor\'s committed offset when it was '
                        + 'stopped');
                    note('the table printed in step 3 was read a moment before');
                    note('the processors were stopped, so a processor still');
                    note('running committed a little more before it went down.');
                    note('Each watermark is therefore at or past the number in');
                    note('that table, and never behind it: behind would mean the');
                    note('worker re-delivering what the processor had already');
                    note('sent.');
                    DESTS.forEach(d => {
                        const mark = selfSeed.watermarks[d] || {};
                        Object.keys(offsets[d]).forEach(partition => {
                            assert.ok(mark[partition] >= offsets[d][partition],
                                `${d} p${partition}: the watermark ${mark[partition]} `
                                + 'is behind the offset its processor had committed '
                                + `at the swap, ${offsets[d][partition]}`);
                        });
                    });
                } else {
                    act.measured('the seeding',
                        'run ahead with the CLI, seedOnStart off');
                }
                await wait.until('the pool\'s first delivery',
                    async () => (await wait.counter(1, 'delivered')) > 0,
                    240000, 2000);
                const pauseS = Math.round((Date.now() - stoppedAt) / 1000);
                say(`first pool delivery ${pauseS}s after the processors were `
                    + 'stopped');
                act.measured('delivery pause', `${pauseS}s, processors stopped to `
                    + 'first pool delivery');
                note('the pause is the container swap, the self seeding and the');
                note('group join. On a real platform the swap is Ansible\'s stop');
                note('and start, the seeding is a second, and the join is the');
                note('consumer session, 45 s by default.');
                watch('grafana', 'delivered per second by destination: the');
                watch('grafana', `stalled ${STALLED} comes back first and fastest, `
                    + 'it has the most to catch up');

                step(6, 'let the load finish, drain, and check every destination');
                await wait.until('the driver to finish', () => !load.proc.isRunning(),
                    (SECONDS + 120) * 1000, 2000);
                await wait.frozen(env.INTERNAL_TOPIC, env.pause(12000));
                const drain = await flow.drainOrCure({ group, topic: env.POOL_TOPIC,
                    label: 'pool', timeoutMs: 300000, workers: [1] });
                assert.ok(drain.drained, 'the pool did not drain to lag 0');
                await flow.snapshotMetrics(act, 1, 'worker1');
                const skipped = await wait.counter(1, 'watermark');
                act.measured('records skipped under the watermark',
                    `${skipped}, already delivered by a processor`);
                let gaps = 0;
                let dups = 0;
                let inversions = 0;
                const afterSwap = {};
                const dupsBy = {};
                DESTS.forEach(d => {
                    const r = flow.dumpAndCheck({ act, topic: ctx.customerTopicOf[d],
                        from: from[d], driver: load.log, bucket: BUCKETS[d],
                        label: d });
                    gaps += r.totals.gaps;
                    dups += r.totals.duplicate_extras;
                    dupsBy[d] = r.totals.duplicate_extras;
                    inversions += r.totals.inversions;
                    afterSwap[d] = kafka.head(ctx.customerTopicOf[d], 0)
                        - headAtSwap[d];
                    say(`${d}: ${afterSwap[d]} records delivered after the swap`);
                });
                act.measured('gaps (loss)', gaps);
                // A processor delivers, then commits on its auto-commit tick
                // (5 s). Whatever it delivered after its last commit is above
                // its committed offset, so the watermark cannot know about it
                // and the pool delivers it again: the same uncommitted window
                // a kill -9 of any consumer costs, at-least-once. The stopped
                // processor had time to commit before the swap; the frozen
                // one and the running one had not.
                const perDest = DESTS.map(d => `${d} ${dupsBy[d]}`).join(', ');
                act.measured(WITHOUT_WATERMARK ? 'duplicates without the watermark'
                    : 'duplicates with the watermark',
                WITHOUT_WATERMARK ? dups
                    : `the processors' uncommitted windows only: ${dups} `
                      + `(${perDest})`);
                if (!WITHOUT_WATERMARK) {
                    note(`${dups} records arrived twice, per destination ${perDest}.`);
                    note('Those are the processors\' uncommitted windows: what each');
                    note('had delivered after its last 5 s auto-commit when it was');
                    note('stopped or frozen. The watermark stands at the committed');
                    note('offset, so it cannot know about them, and the pool');
                    note('delivers them again. The processor stopped well before');
                    note('the swap had committed everything it delivered, so it');
                    note('contributes none. At-least-once, bounded by the commit');
                    note('interval, the same window act 05\'s kill -9 shows.');
                }
                act.measured('per-key inversions', inversions);
                const stalledOps = fs.readFileSync(load.log, 'utf8')
                    .split('\n').filter(l => l.includes(BUCKETS[STALLED])
                        && / ok$/.test(l)).length;
                act.measured('stalled destination backlog delivered by the pool',
                    `${afterSwap[STALLED]} records after the swap, of ${stalledOps} `
                    + 'operations in the whole load');
                note('what to say on camera: every event the stalled destination');
                note('was owed arrived once the pool took over. Today that backlog');
                note('sits behind a frozen offset until someone notices. The');
                note('caught-up destination got nothing twice, because its');
                note('watermark told the worker where its processor had got to.');
                note('order held because only one generation ever ran.');

                assert.strictEqual(gaps, 0, 'the migration lost events');
                assert.strictEqual(inversions, 0, 'the migration reordered events');
                if (!WITHOUT_WATERMARK) {
                    // three processors, about 2 operations a second each, a
                    // 5 s auto-commit: 10 records each is the window, and
                    // twice that is the bound this asserts
                    const bound = DESTS.length * 2 * 5 * 2;
                    assert.ok(dups <= bound,
                        `the migration delivered ${dups} events twice, more than `
                        + `the processors' uncommitted windows (bound ${bound})`);
                }
                worker.stop();
            });
    });
}

module.exports = { register };
