'use strict';

/**
 * Act 02, rig scenario M1: today's shipped path, end to end.
 *
 * A real S3 PUT through CloudServer, the metadata oplog, the queue
 * populator, one per-destination queue processor, and the customer's own
 * kafka topic. This is the baseline every later act is compared against, and
 * the event shape the migration has to preserve byte for byte.
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

function register(ctx) {
    describe('Act 02: the legacy baseline', () => {
        const act = new Act('02', 'legacy-baseline', 'M1',
            '20 PUTs and 5 DELETEs arrive as 25 events, in per-key order, in '
            + 'the shape the migration must preserve');
        let topic;

        before(() => {
            act.open();
            topic = ctx.customerTopicOf[DEST];
            act.expect('expected events', 25);
            act.expect('delivered events', 25);
            act.expect('gaps (loss)', 0);
            act.expect('duplicate extras', 0);
            act.expect('per-key inversions', 0);
            act.expect('event size field', 'string, 1000 + sequence');
            act.expect('record key', '<bucket>/<objectKey>');
        });

        after(() => {
            procs.stopAll();
            act.close();
        });

        it('delivers every event, once, in per-key order', async () => {
            step(1, 'start the legacy path: populator, then one processor');
            note('nothing about this path is new code: the populator has no');
            note('deliveryPool block in its config, so it takes the 9.3 route');
            const config = flow.legacyConfig(act, ctx);
            await flow.startPopulator(act, config, 'legacy');
            const proc = await flow.startProcessor(act, config, DEST);
            note('healthy is ONE rdkafka.assign and no revoke. A stream of');
            note('assign then revoke pairs is the design/06 wedge, and the');
            note('cure is a restart of that one consumer.');
            watch('kafka ui', `topic ${env.INTERNAL_TOPIC}, 4 partitions`);
            watch('grafana', 'row "Cutover", the legacy group\'s lag');

            step(2, `create ${BUCKET} and put its notification configuration`);
            const put = await s3lib.bucketWith(ctx.s3, BUCKET, [DEST]);
            assert.ok(put.ok, `the configuration was refused: ${put.message}`);
            say('accepted, read back with '
                + `${put.readBack.QueueConfigurations.length} rule(s), id `
                + `${put.readBack.QueueConfigurations[0].Id}`);
            note('the rule id arrives in every event as s3.configurationId');

            step(3, 'warm the consumer group before measuring anything');
            note('the legacy processor builds its consumer with no fromOffset,');
            note('so auto.offset.reset stays at librdkafka\'s latest: a group');
            note('that has not committed yet can skip what is already on the');
            note('topic. Warming it first is the rig\'s own method note, and');
            note('it is what makes the measured window honest.');
            await flow.runDriver(act, { bucket: BUCKET, prefix: 'warm',
                rate: 4, count: 8 });
            await wait.frozen(env.INTERNAL_TOPIC, env.pause(10000),
                { atLeast: 8 });
            const warmed = await wait.until('the group to hold committed offsets',
                () => {
                    const st = kafka.groupState(env.legacyGroup(DEST));
                    return st.partitions > 0 && st.committed > 0
                        && st.unknown === 0;
                }, 180000, 5000);
            const st0 = kafka.groupState(env.legacyGroup(DEST));
            say(`group warmed: committed ${st0.committed} over `
                + `${st0.partitions} partitions, lag ${st0.lag}`);
            if (!warmed) {
                note('the group never committed on every partition; the');
                note('measurement below may show that as loss');
            }

            const from = kafka.head(topic, 0);
            say(`${topic} starts at offset ${from}`);

            step(4, '20 operations at 2/s, every 4th a PUT then DELETE');
            note('15 plain PUTs plus 5 PUT and DELETE pairs is 25 events');
            note('the sequence travels in the object size (size = 1000 + seq),');
            note('which is how the checker rebuilds the expected order from');
            note('the delivered events alone');
            const drive = await flow.runDriver(act, {
                'bucket': BUCKET, 'prefix': 'base', 'rate': 2, 'count': 20,
                'straddle': 5, 'straddle-every': 4,
            });

            step(5, 'wait for the populator to publish, then for the drain');
            note('the populator reads the oplog on a batch cadence of a few');
            note('seconds, so the internal topic is still empty for a moment');
            note('after the last PUT. A lag of 0 before anything is published');
            note('means nothing, which is why the gate counts records too.');
            const published = await wait.frozen(env.INTERNAL_TOPIC,
                env.pause(12000), { atLeast: 25 });
            say(`internal topic settled at ${published} records`);
            note('the gate is lag 0 AND the committed offset moving: a wedged');
            note('consumer holds its partitions with a lag that stops falling');
            note('while its liveness probe still answers 200');
            const drain = await wait.drain({
                group: env.legacyGroup(DEST),
                label: `legacy ${DEST}`,
                timeoutMs: 240000,
                topicAtLeast: { topic: env.INTERNAL_TOPIC, count: 33 },
            });
            if (drain.stalled) {
                say('restarting the processor, which is the documented cure');
                proc.stop();
                await procs.sleep(3000);
                await flow.startProcessor(act, config, DEST);
                await wait.drain({ group: env.legacyGroup(DEST),
                    label: `legacy ${DEST} after the restart`,
                    timeoutMs: 240000 });
            }
            const bal = proc.rebalances();
            say(`processor rebalances: ${bal.assign} assign, ${bal.revoke} revoke`);
            act.measured('processor rebalances',
                `${bal.assign} assign / ${bal.revoke} revoke`);

            step(6, 'dump the customer topic and check it');
            const result = flow.dumpAndCheck({ act, topic, from,
                driver: drive.log, keyPrefix: 'base', label: 'dest1' });
            flow.recordChecker(act, result);

            step(7, 'the event shape the migration must preserve');
            const golden = s3lib.goldenEvent(act.file('events-dest1.jsonl'),
                act.file('golden-event.json'));
            assert.ok(golden, 'no parseable event arrived');
            const rec = golden.Records[0];
            const s3 = rec.s3;
            say(`eventName       ${rec.eventName}`);
            say(`record key      ${s3.bucket.name}/${s3.object.key}`);
            say(`size            ${JSON.stringify(s3.object.size)} `
                + '(a string, on purpose)');
            say(`configurationId ${s3.configurationId}`);
            note('null on the mongo log source: eTag, versionId, sequencer,');
            note('arn, principalId, sourceIPAddress and both responseElements,');
            note('because the oplog carries metadata but no request context');
            act.measured('event size field',
                typeof s3.object.size === 'string'
                    ? 'string, 1000 + sequence' : `${typeof s3.object.size}`);
            act.measured('record key',
                `${s3.bucket.name}/${s3.object.key}`.startsWith(`${BUCKET}/`)
                    ? '<bucket>/<objectKey>' : 'unexpected shape');

            assert.strictEqual(result.totals.gaps, 0,
                'the legacy path lost events');
            assert.strictEqual(result.totals.expected, 25,
                'the driver did not complete 20 operations as 25 events');
            assert.strictEqual(result.totals.delivered, 25,
                'the number of delivered events is not 25');
            assert.strictEqual(typeof s3.object.size, 'string',
                'the size field changed shape');
        });
    });
}

module.exports = { register };
