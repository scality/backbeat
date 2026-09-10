'use strict';

/**
 * Act 04, rig scenarios M2b and M4b: the migration, and its rollback.
 *
 * Five operator steps, no new tooling, because today's per-destination
 * processor IS the design's single-destination worker of generation v0:
 *
 *   1. start the delivery worker on the quiet delivery topic
 *   2. switch the populator to that topic
 *   3. let the legacy processors drain the legacy topic to lag 0
 *   4. stop the legacy processors
 *   5. carry on
 *
 * Starting the worker BEFORE the switch is what removes the 69 duplicates
 * the rig measured: the worker joins a topic with nothing to replay. Waiting
 * for lag 0 before step 4 is what keeps per-key order: doing it early cost
 * 284 same-key inversions on the rig.
 *
 * Then the mirror rollback: populator back, pool drains to 0, legacy resumes
 * at its own committed offset, stop the worker. It costs nothing, which is
 * the argument for this path over the drainer one.
 */

const assert = require('assert');
const env = require('../lib/env');
const kafka = require('../lib/kafka');
const procs = require('../lib/procs');
const wait = require('../lib/wait');
const flow = require('../lib/flow');
const s3lib = require('../lib/s3');
const { Act, say, note, watch, step } = require('../lib/narrate');

const DEST = 'poc-dest-1';
const BUCKET = 'demo-bucket';
const SECONDS = Number(env.knob('DEMO_ACT04_SECONDS', 240));

function register(ctx) {
    describe('Act 04: the cutover and its rollback', () => {
        const act = new Act('04', 'switch-and-drain', 'M2b and M4b',
            'cut over to the pool under load with no loss, then roll back for '
            + 'nothing');
        let topic;

        before(() => {
            act.open();
            topic = ctx.customerTopicOf[DEST];
            act.expect('cutover gaps (loss)', 0);
            act.expect('cutover duplicate extras',
                '0 with the worker started first');
            act.expect('cutover per-key inversions', '0 or 1');
            act.expect('legacy committed vs internal head', 'equal');
            act.expect('internal topic after the switch', 'frozen');
            act.expect('rollback gaps (loss)', 0);
            act.expect('rollback duplicate extras', 0);
            act.expect('rollback per-key inversions', 0);
            act.expect('records of the cutover window re-delivered', 0);
            act.expect('delivery topic after the rollback', 'frozen');
        });

        after(() => {
            procs.stopAll();
            act.close();
        });

        it('cuts over with no loss and rolls back for nothing', async () => {
            const legacy = flow.legacyConfig(act, ctx);
            const pool = flow.poolConfig(act, ctx);
            await s3lib.bucketWith(ctx.s3, BUCKET, [DEST]);

            step(0, 'start on the legacy path, caught up, under load');
            let populator = await flow.startPopulator(act, legacy, 'legacy');
            let processor = await flow.startProcessor(act, legacy, DEST);
            const cutFrom = kafka.head(topic, 0);
            const load = flow.startDriver(act, { 'bucket': BUCKET, 'prefix': 'm2b',
                'rate': 2, 'duration': SECONDS, 'straddle': 3, 'straddle-every': 5 });
            watch('grafana', 'row "Cutover": the legacy group versus the pool');
            note('the switch only means something once the legacy path is');
            note('actually delivering, so wait for real events first');
            await wait.until('the legacy path to deliver events',
                () => kafka.head(topic, 0) >= cutFrom + 10, 180000, 3000);
            say(`legacy path delivering: ${topic} at `
                + `${kafka.head(topic, 0)}, internal topic at `
                + `${kafka.headTotal(env.INTERNAL_TOPIC)}`);

            step(1, 'start the delivery worker FIRST, on the quiet topic');
            note('started after the switch it replays whatever accumulated');
            note('during the switch, which is where the rig\'s 69 duplicates');
            note('came from. Started first it has nothing to replay.');
            const worker = await flow.startWorker(act, pool, 1);
            const bal = worker.rebalances();
            say(`worker rebalances so far: ${bal.assign} assign, `
                + `${bal.revoke} revoke`);

            step(2, 'switch the populator to the delivery topic');
            const lagAtSwitch = kafka.groupLag(env.legacyGroup(DEST));
            const internalAtSwitch = kafka.headTotal(env.INTERNAL_TOPIC);
            say(`legacy lag at the instant of the switch: ${lagAtSwitch}`);
            populator.stop();
            await procs.sleep(env.pause(6000));
            populator = await flow.startPopulator(act, pool, 'pool');
            act.timeline(`switched to pool, legacy lag ${lagAtSwitch}`);
            watch('kafka ui', `${env.INTERNAL_TOPIC} freezes, `
                + `${env.DELIVERY_TOPIC} starts moving`);
            note('one destination is one delivery key is one partition: the');
            note('key is the bare destination name, so capacity for a single');
            note('destination goes through spreadFactor, never through the');
            note('topic\'s partition count');

            step(3, 'let the legacy processor drain, gating on progress');
            const drain = await wait.drain({ group: env.legacyGroup(DEST),
                label: `legacy ${DEST}`, timeoutMs: 300000,
                topicAtLeast: { topic: env.INTERNAL_TOPIC, count: 1 } });
            if (drain.stalled) {
                say('restarting the legacy processor, the documented cure');
                processor.stop();
                await procs.sleep(4000);
                processor = await flow.startProcessor(act, legacy, DEST);
                await wait.drain({ group: env.legacyGroup(DEST),
                    label: `legacy ${DEST} after the restart`, timeoutMs: 300000 });
            }
            const internalNow = kafka.headTotal(env.INTERNAL_TOPIC);
            await procs.sleep(env.pause(8000));
            const internalAgain = kafka.headTotal(env.INTERNAL_TOPIC);
            const committed = kafka.groupState(env.legacyGroup(DEST)).committed;
            say(`internal head ${internalAtSwitch} at the switch, `
                + `${internalNow} now, legacy committed ${committed}`);
            act.measured('internal topic after the switch',
                internalNow === internalAgain ? 'frozen'
                    : `still moving (${internalNow} to ${internalAgain})`);
            act.measured('legacy committed vs internal head',
                committed === internalNow ? 'equal'
                    : `committed ${committed} vs head ${internalNow}`);
            note('this equality is what makes the rollback free: the legacy');
            note('group resumes exactly at the boundary');

            step(4, 'stop the legacy processor');
            processor.stop();
            await procs.sleep(env.pause(4000));

            step(5, 'more load on the pool, then stop the driver and drain');
            await procs.sleep(env.pause(30000));
            load.proc.stop();
            await wait.until('the driver to stop', () => !load.proc.isRunning(),
                60000, 1000);
            await wait.frozen(env.DELIVERY_TOPIC, env.pause(12000));
            await wait.drain({ group: env.DELIVERY_GROUP, label: 'pool',
                timeoutMs: 300000, workers: [1] });

            step(6, 'check the whole cutover window');
            const cut = flow.dumpAndCheck({ act, topic, from: cutFrom,
                driver: load.log, keyPrefix: 'm2b', label: 'cutover' });
            act.measured('cutover gaps (loss)', cut.totals.gaps);
            act.measured('cutover duplicate extras', cut.totals.duplicate_extras);
            act.measured('cutover per-key inversions', cut.totals.inversions);
            assert.strictEqual(cut.totals.gaps, 0, 'the cutover lost events');
            assert.ok(cut.totals.expected > 0, 'the driver did nothing');

            // ---------------------------------------------- the rollback ---
            step(7, 'the mirror rollback: populator back to the legacy topic');
            note('state now: pool path, delivery group at lag 0, legacy group');
            note('committed at the frozen internal head. That is exactly what');
            note('the rollback needs, and it is what the cutover left behind.');
            const rbFrom = kafka.head(topic, 0);
            const rbLoad = flow.startDriver(act, { 'bucket': BUCKET,
                'prefix': 'm4b', 'rate': 2, 'duration': Math.round(SECONDS / 2),
                'straddle': 3, 'straddle-every': 5 });
            await procs.sleep(env.pause(15000));
            const deliveryAtSwitch = kafka.headTotal(env.DELIVERY_TOPIC);
            populator.stop();
            await procs.sleep(env.pause(6000));
            populator = await flow.startPopulator(act, legacy, 'legacy2');
            say(`delivery topic at the switch: ${deliveryAtSwitch}`);

            step(8, 'let the pool drain the delivery topic to lag 0');
            await wait.drain({ group: env.DELIVERY_GROUP, label: 'pool',
                timeoutMs: 300000, workers: [1] });
            const deliveryNow = kafka.headTotal(env.DELIVERY_TOPIC);
            await procs.sleep(env.pause(8000));
            act.measured('delivery topic after the rollback',
                deliveryNow === kafka.headTotal(env.DELIVERY_TOPIC)
                    ? 'frozen' : 'still moving');

            step(9, 'start the legacy processor: it resumes at its own offset');
            const beforeResume = kafka.groupState(env.legacyGroup(DEST));
            processor = await flow.startProcessor(act, legacy, DEST);
            await procs.sleep(env.pause(15000));
            const lagOnResume = kafka.groupLag(env.legacyGroup(DEST));
            say(`committed ${beforeResume.committed}, internal head `
                + `${kafka.headTotal(env.INTERNAL_TOPIC)}, so a lag of `
                + `${lagOnResume}`);
            act.measured('legacy lag when it resumes',
                `${lagOnResume}, the records published since the switch`);

            step(10, 'stop the worker, finish the load, drain');
            worker.stop();
            await procs.sleep(env.pause(4000));
            rbLoad.proc.stop();
            await wait.until('the driver to stop', () => !rbLoad.proc.isRunning(),
                60000, 1000);
            await wait.frozen(env.INTERNAL_TOPIC, env.pause(12000));
            await wait.drain({ group: env.legacyGroup(DEST),
                label: `legacy ${DEST}`, timeoutMs: 300000 });

            step(11, 'check the rollback window, and look for re-deliveries');
            const rb = flow.dumpAndCheck({ act, topic, from: rbFrom,
                driver: rbLoad.log, keyPrefix: 'm4b', label: 'rollback' });
            act.measured('rollback gaps (loss)', rb.totals.gaps);
            act.measured('rollback duplicate extras', rb.totals.duplicate_extras);
            act.measured('rollback per-key inversions', rb.totals.inversions);
            const again = flow.dumpAndCheck({ act, topic, from: rbFrom,
                driver: load.log, keyPrefix: 'm2b', label: 'previous-window' });
            act.measured('records of the cutover window re-delivered',
                again.totals.delivered);
            note('the drainer path\'s rollback re-delivered 106 records and');
            note('stranded 161 on the delivery topic, with no reverse drainer');
            note('to recover them. This path creates no divergence at all.');

            assert.strictEqual(rb.totals.gaps, 0, 'the rollback lost events');
            assert.strictEqual(rb.totals.duplicate_extras, 0,
                'the rollback duplicated events');
            assert.strictEqual(again.totals.delivered, 0,
                'the rollback re-delivered records from before the cutover');
        });
    });
}

module.exports = { register };
