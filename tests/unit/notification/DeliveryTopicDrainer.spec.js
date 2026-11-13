const assert = require('assert');
const sinon = require('sinon');

const DeliveryTopicDrainer = require(
    '../../../extensions/notification/deliveryWorker/DeliveryTopicDrainer');
const { buildDeliveryKey } = require(
    '../../../extensions/notification/utils/deliveryKey');

const OLD_TOPIC = 'bucket-notification';
const DESTINATION = { resource: 'destId', type: 'kafka', host: 'external-host' };

const notifConfig = {
    topic: OLD_TOPIC,
    queueProcessor: {
        groupId: 'backbeat-bucket-notification-group',
    },
    deliveryPool: {
        topic: 'bucket-notification-delivery',
    },
    destinations: [DESTINATION],
};

const bucketConfig = {
    bucket: 'mybucket',
    notificationConfiguration: {
        queueConfig: [
            {
                id: 'config-1',
                queueArn: 'arn:scality:bucketnotif:::destId',
                events: ['s3:ObjectCreated:*'],
            },
        ],
    },
};

function makeEntry(overrides) {
    return Object.assign({
        bucket: 'mybucket',
        key: 'obj1',
        eventType: 's3:ObjectCreated:Put',
        value: '{}',
    }, overrides);
}

function makeRecord(topic, partition, offset, entry) {
    return {
        topic,
        partition,
        offset,
        key: Buffer.from(`${entry.bucket}/${entry.key}`),
        value: Buffer.from(JSON.stringify(entry)),
    };
}

/**
 * Stubs the bits of a node-rdkafka consumer the drainer uses. Offset
 * writing methods are spies so that tests can assert the drainer never
 * touches the offsets of the old consumer groups.
 *
 * @param {Object} params - stub params
 * @param {Object} params.watermarks - partition id to { lowOffset, highOffset }
 * @param {Object} params.committed - partition id to committed offset
 * @param {Array[]} params.batches - batches returned by consume(), in order
 * @param {Object[]} [params.positions] - what position() returns
 * @return {Object} consumer stub
 */
function makeConsumer(params) {
    const { watermarks, committed, batches, positions } = params;
    const consumer = {
        consumeCalls: 0,
        unassignCalls: 0,
        assigned: null,
        committedArgs: null,
        onConsume: null,
        commit: sinon.spy(),
        commitSync: sinon.spy(),
        commitMessage: sinon.spy(),
        subscribe: sinon.spy(),
        getMetadata: (opts, cb) => process.nextTick(() => cb(null, {
            topics: [{
                name: opts.topic,
                partitions: Object.keys(watermarks)
                    .map(id => ({ id: Number(id) })),
            }],
        })),
        committed: (toppars, timeout, cb) => {
            consumer.committedArgs = { toppars, timeout };
            return process.nextTick(() => cb(null, toppars.map(tp => ({
                topic: tp.topic,
                partition: tp.partition,
                offset: committed[tp.partition],
            }))));
        },
        queryWatermarkOffsets: (topic, partition, timeout, cb) =>
            process.nextTick(() => cb(null, watermarks[partition])),
        assign: assignments => {
            consumer.assigned = assignments;
        },
        unassign: () => {
            consumer.unassignCalls++;
        },
        consume: (count, cb) => {
            consumer.consumeCalls++;
            if (consumer.onConsume) {
                consumer.onConsume();
            }
            const batch = batches.length > 0 ? batches.shift() : [];
            return process.nextTick(() => cb(null, batch));
        },
        position: () => positions || [],
    };
    return consumer;
}

function makeProducer(sent, onSend) {
    return {
        send: (entries, cb) => {
            sent.push(...entries);
            if (onSend) {
                return onSend(entries, cb);
            }
            return process.nextTick(cb);
        },
    };
}

function makeDrainer(params) {
    return new DeliveryTopicDrainer(Object.assign({
        kafkaConfig: { hosts: 'localhost:9092' },
        notifConfig,
        emptyBatchSleepMs: 0,
        batchSize: 10,
    }, params));
}

describe('notification DeliveryTopicDrainer', () => {
    describe('resolveStartOffset', () => {
        it('should start at the low watermark when the old group never ' +
        'committed', () => {
            // -1001 is what librdkafka reports for an unset offset
            const res = DeliveryTopicDrainer.resolveStartOffset({
                committedOffset: -1001,
                lowOffset: 4,
                highOffset: 20,
            });
            assert.strictEqual(res.startOffset, 4);
            assert.strictEqual(res.skip, false);
        });

        it('should start at the low watermark when there is no committed ' +
        'offset at all', () => {
            const res = DeliveryTopicDrainer.resolveStartOffset({
                committedOffset: undefined,
                lowOffset: 0,
                highOffset: 3,
            });
            assert.strictEqual(res.startOffset, 0);
            assert.strictEqual(res.skip, false);
        });

        it('should start at the low watermark when retention dropped the ' +
        'committed offset', () => {
            const res = DeliveryTopicDrainer.resolveStartOffset({
                committedOffset: 3,
                lowOffset: 10,
                highOffset: 20,
            });
            assert.strictEqual(res.startOffset, 10);
            assert.strictEqual(res.skip, false);
        });

        it('should resume at the committed offset', () => {
            const res = DeliveryTopicDrainer.resolveStartOffset({
                committedOffset: 7,
                lowOffset: 0,
                highOffset: 12,
            });
            assert.strictEqual(res.startOffset, 7);
            assert.strictEqual(res.skip, false);
        });

        it('should skip a partition the old group fully consumed', () => {
            const res = DeliveryTopicDrainer.resolveStartOffset({
                committedOffset: 5,
                lowOffset: 0,
                highOffset: 5,
            });
            assert.strictEqual(res.skip, true);
        });

        it('should skip an empty partition', () => {
            const res = DeliveryTopicDrainer.resolveStartOffset({
                committedOffset: -1001,
                lowOffset: 0,
                highOffset: 0,
            });
            assert.strictEqual(res.skip, true);
        });
    });

    describe('drainDestination', () => {
        it('should read the committed offsets of the old destination group ' +
        'without ever writing them', done => {
            const consumer = makeConsumer({
                watermarks: { 0: { lowOffset: 0, highOffset: 1 } },
                committed: { 0: 0 },
                batches: [[makeRecord(OLD_TOPIC, 0, 0, makeEntry())]],
            });
            const drainer = makeDrainer({
                consumer,
                producer: makeProducer([]),
                bnConfigManager: { getConfig: (b, cb) => cb(null, bucketConfig) },
            });
            const readOffsets = sinon.spy(drainer, '_readCommittedOffsets');
            drainer.drainDestination(DESTINATION, err => {
                assert.ifError(err);
                assert(readOffsets.calledOnce);
                assert.strictEqual(readOffsets.args[0][0], OLD_TOPIC);
                assert.strictEqual(readOffsets.args[0][1],
                    'backbeat-bucket-notification-group-destId');
                assert.deepStrictEqual(consumer.committedArgs.toppars,
                    [{ topic: OLD_TOPIC, partition: 0 }]);
                assert(consumer.commit.notCalled);
                assert(consumer.commitSync.notCalled);
                assert(consumer.commitMessage.notCalled);
                assert(consumer.subscribe.notCalled);
                done();
            });
        });

        it('should assign the partitions left to drain at their start ' +
        'offsets', done => {
            const consumer = makeConsumer({
                watermarks: {
                    0: { lowOffset: 0, highOffset: 3 },
                    1: { lowOffset: 8, highOffset: 20 },
                    2: { lowOffset: 0, highOffset: 5 },
                },
                // partition 1 committed below the low watermark, partition 2
                // was fully consumed by the old queue processor
                committed: { 0: 1, 1: 2, 2: 5 },
                batches: [[
                    makeRecord(OLD_TOPIC, 0, 2, makeEntry()),
                    makeRecord(OLD_TOPIC, 1, 19, makeEntry()),
                ]],
            });
            const drainer = makeDrainer({
                consumer,
                producer: makeProducer([]),
                bnConfigManager: { getConfig: (b, cb) => cb(null, bucketConfig) },
            });
            drainer.drainDestination(DESTINATION, err => {
                assert.ifError(err);
                assert.deepStrictEqual(consumer.assigned, [
                    { topic: OLD_TOPIC, partition: 0, offset: 1 },
                    { topic: OLD_TOPIC, partition: 1, offset: 8 },
                ]);
                done();
            });
        });

        it('should address matching records and key them with the encoded ' +
        'delivery key', done => {
            const sent = [];
            const consumer = makeConsumer({
                watermarks: { 0: { lowOffset: 0, highOffset: 1 } },
                committed: { 0: 0 },
                batches: [[makeRecord(OLD_TOPIC, 0, 0, makeEntry())]],
            });
            const drainer = makeDrainer({
                consumer,
                producer: makeProducer(sent),
                bnConfigManager: { getConfig: (b, cb) => cb(null, bucketConfig) },
            });
            drainer.drainDestination(DESTINATION, err => {
                assert.ifError(err);
                assert.strictEqual(sent.length, 1);
                assert.strictEqual(sent[0].key, encodeURIComponent(
                    buildDeliveryKey(DESTINATION, 'mybucket', 'obj1')));
                assert.strictEqual(sent[0].key, 'destId');
                const message = JSON.parse(sent[0].message);
                assert.strictEqual(message.destinationId, 'destId');
                assert.strictEqual(message.configurationId, 'config-1');
                // the old payload is carried over untouched
                assert.strictEqual(message.bucket, 'mybucket');
                assert.strictEqual(message.key, 'obj1');
                assert.strictEqual(message.eventType, 's3:ObjectCreated:Put');
                assert.deepStrictEqual(drainer.totals,
                    { drained: 1, produced: 1, skipped: 0 });
                done();
            });
        });

        it('should url-encode a spread delivery key', done => {
            const spreadDestination = Object.assign({ spreadFactor: 4 },
                DESTINATION);
            const sent = [];
            const consumer = makeConsumer({
                watermarks: { 0: { lowOffset: 0, highOffset: 1 } },
                committed: { 0: 0 },
                batches: [[makeRecord(OLD_TOPIC, 0, 0, makeEntry())]],
            });
            const drainer = makeDrainer({
                consumer,
                producer: makeProducer(sent),
                bnConfigManager: { getConfig: (b, cb) => cb(null, bucketConfig) },
            });
            drainer.drainDestination(spreadDestination, err => {
                assert.ifError(err);
                const rawKey = buildDeliveryKey(spreadDestination, 'mybucket',
                    'obj1');
                assert(rawKey.includes('|'));
                assert.strictEqual(sent[0].key, encodeURIComponent(rawKey));
                assert(sent[0].key.includes('%7C'));
                done();
            });
        });

        it('should skip records that no longer match a configuration', done => {
            const sent = [];
            const entries = [
                // no configuration at all for this bucket
                makeEntry({ bucket: 'noconfig' }),
                // configuration targets another destination
                makeEntry({ bucket: 'otherdest' }),
                // event type is not subscribed to
                makeEntry({ eventType: 's3:ObjectRemoved:Delete' }),
                // deletion placeholder, no event type
                makeEntry({ eventType: undefined }),
                makeEntry({ key: 'obj5' }),
            ];
            const configs = {
                mybucket: bucketConfig,
                noconfig: undefined,
                otherdest: {
                    bucket: 'otherdest',
                    notificationConfiguration: {
                        queueConfig: [{
                            id: 'other',
                            queueArn: 'arn:scality:bucketnotif:::otherDestId',
                            events: ['s3:ObjectCreated:*'],
                        }],
                    },
                },
            };
            const consumer = makeConsumer({
                watermarks: { 0: { lowOffset: 0, highOffset: 5 } },
                committed: { 0: 0 },
                batches: [entries.map((entry, i) =>
                    makeRecord(OLD_TOPIC, 0, i, entry))],
            });
            const drainer = makeDrainer({
                consumer,
                producer: makeProducer(sent),
                bnConfigManager: {
                    getConfig: (bucket, cb) => cb(null, configs[bucket]),
                },
            });
            drainer.drainDestination(DESTINATION, err => {
                assert.ifError(err);
                assert.strictEqual(sent.length, 1);
                assert.strictEqual(JSON.parse(sent[0].message).key, 'obj5');
                assert.deepStrictEqual(drainer.totals,
                    { drained: 5, produced: 1, skipped: 4 });
                done();
            });
        });

        it('should skip a record whose payload is not valid JSON', done => {
            const sent = [];
            const record = makeRecord(OLD_TOPIC, 0, 0, makeEntry());
            record.value = Buffer.from('{not json');
            const consumer = makeConsumer({
                watermarks: { 0: { lowOffset: 0, highOffset: 1 } },
                committed: { 0: 0 },
                batches: [[record]],
            });
            const drainer = makeDrainer({
                consumer,
                producer: makeProducer(sent),
                bnConfigManager: { getConfig: (b, cb) => cb(null, bucketConfig) },
            });
            drainer.drainDestination(DESTINATION, err => {
                assert.ifError(err);
                assert.strictEqual(sent.length, 0);
                assert.strictEqual(drainer.totals.skipped, 1);
                done();
            });
        });

        it('should consume the next batch only once the previous one is ' +
        'acked', done => {
            const events = [];
            const consumer = makeConsumer({
                watermarks: { 0: { lowOffset: 0, highOffset: 3 } },
                committed: { 0: 0 },
                batches: [
                    [
                        makeRecord(OLD_TOPIC, 0, 0, makeEntry()),
                        makeRecord(OLD_TOPIC, 0, 1, makeEntry()),
                    ],
                    [makeRecord(OLD_TOPIC, 0, 2, makeEntry())],
                ],
            });
            consumer.onConsume = () => events.push('consume');
            const producer = makeProducer([], (entries, cb) => {
                events.push('send');
                // a delivery report is not immediate, the drainer has to
                // wait for it before pulling more records
                return setTimeout(() => {
                    events.push('ack');
                    cb();
                }, 10);
            });
            const drainer = makeDrainer({
                consumer,
                producer,
                bnConfigManager: { getConfig: (b, cb) => cb(null, bucketConfig) },
            });
            drainer.drainDestination(DESTINATION, err => {
                assert.ifError(err);
                assert.deepStrictEqual(events, [
                    'consume', 'send', 'ack',
                    'consume', 'send', 'ack',
                ]);
                done();
            });
        });

        it('should stop at the head offset captured when the drain started',
        done => {
            const sent = [];
            const consumer = makeConsumer({
                watermarks: { 0: { lowOffset: 0, highOffset: 2 } },
                committed: { 0: 0 },
                batches: [
                    [
                        makeRecord(OLD_TOPIC, 0, 0, makeEntry()),
                        makeRecord(OLD_TOPIC, 0, 1, makeEntry()),
                    ],
                    // would be returned if the drainer kept consuming
                    [makeRecord(OLD_TOPIC, 0, 2, makeEntry())],
                ],
            });
            const drainer = makeDrainer({
                consumer,
                producer: makeProducer(sent),
                bnConfigManager: { getConfig: (b, cb) => cb(null, bucketConfig) },
            });
            drainer.drainDestination(DESTINATION, err => {
                assert.ifError(err);
                assert.strictEqual(consumer.consumeCalls, 1);
                assert.strictEqual(sent.length, 2);
                done();
            });
        });

        it('should finish a partition whose remaining offsets yield no ' +
        'record', done => {
            const consumer = makeConsumer({
                watermarks: { 0: { lowOffset: 0, highOffset: 4 } },
                committed: { 0: 2 },
                batches: [],
                positions: [{ topic: OLD_TOPIC, partition: 0, offset: 4 }],
            });
            const drainer = makeDrainer({
                consumer,
                producer: makeProducer([]),
                bnConfigManager: { getConfig: (b, cb) => cb(null, bucketConfig) },
            });
            drainer.drainDestination(DESTINATION, err => {
                assert.ifError(err);
                assert.strictEqual(drainer.totals.drained, 0);
                done();
            });
        });

        it('should give up when a partition never reaches its head offset',
        done => {
            const consumer = makeConsumer({
                watermarks: { 0: { lowOffset: 0, highOffset: 4 } },
                committed: { 0: 0 },
                batches: [],
                positions: [{ topic: OLD_TOPIC, partition: 0, offset: 1 }],
            });
            const drainer = makeDrainer({
                consumer,
                producer: makeProducer([]),
                bnConfigManager: { getConfig: (b, cb) => cb(null, bucketConfig) },
                maxEmptyBatches: 3,
            });
            drainer.drainDestination(DESTINATION, err => {
                assert(err);
                assert.strictEqual(consumer.consumeCalls, 3);
                done();
            });
        });

        it('should abort when a bucket configuration cannot be read', done => {
            const consumer = makeConsumer({
                watermarks: { 0: { lowOffset: 0, highOffset: 1 } },
                committed: { 0: 0 },
                batches: [[makeRecord(OLD_TOPIC, 0, 0, makeEntry())]],
            });
            const drainer = makeDrainer({
                consumer,
                producer: makeProducer([]),
                bnConfigManager: {
                    getConfig: (b, cb) => cb(new Error('mongo is down')),
                },
            });
            drainer.drainDestination(DESTINATION, err => {
                assert(err);
                assert.strictEqual(err.message, 'mongo is down');
                done();
            });
        });

        it('should abort when producing to the delivery topic fails', done => {
            const consumer = makeConsumer({
                watermarks: { 0: { lowOffset: 0, highOffset: 1 } },
                committed: { 0: 0 },
                batches: [[makeRecord(OLD_TOPIC, 0, 0, makeEntry())]],
            });
            const producer = makeProducer([], (entries, cb) =>
                process.nextTick(() => cb(new Error('delivery report error'))));
            const drainer = makeDrainer({
                consumer,
                producer,
                bnConfigManager: { getConfig: (b, cb) => cb(null, bucketConfig) },
            });
            drainer.drainDestination(DESTINATION, err => {
                assert(err);
                assert.strictEqual(err.message, 'delivery report error');
                done();
            });
        });

        it('should read the old topic through its prefixed name', done => {
            process.env.KAFKA_TOPIC_PREFIX = 'pfx-';
            const prefixedTopic = `pfx-${OLD_TOPIC}`;
            const consumer = makeConsumer({
                watermarks: { 0: { lowOffset: 0, highOffset: 1 } },
                committed: { 0: 0 },
                batches: [[makeRecord(prefixedTopic, 0, 0, makeEntry())]],
            });
            const drainer = makeDrainer({
                consumer,
                producer: makeProducer([]),
                bnConfigManager: { getConfig: (b, cb) => cb(null, bucketConfig) },
            });
            drainer.drainDestination(DESTINATION, err => {
                delete process.env.KAFKA_TOPIC_PREFIX;
                assert.ifError(err);
                assert.strictEqual(consumer.committedArgs.toppars[0].topic,
                    prefixedTopic);
                assert.strictEqual(consumer.assigned[0].topic, prefixedTopic);
                done();
            });
        });

        it('should use the destination internal topic when it has one',
        done => {
            const destination = Object.assign({ internalTopic: 'dest-topic' },
                DESTINATION);
            const consumer = makeConsumer({
                watermarks: { 0: { lowOffset: 0, highOffset: 1 } },
                committed: { 0: 0 },
                batches: [[makeRecord('dest-topic', 0, 0, makeEntry())]],
            });
            const drainer = makeDrainer({
                consumer,
                producer: makeProducer([]),
                bnConfigManager: { getConfig: (b, cb) => cb(null, bucketConfig) },
            });
            drainer.drainDestination(destination, err => {
                assert.ifError(err);
                assert.strictEqual(consumer.assigned[0].topic, 'dest-topic');
                done();
            });
        });

        it('should not assign anything when the old topic does not exist',
        done => {
            const consumer = makeConsumer({
                watermarks: {},
                committed: {},
                batches: [],
            });
            const drainer = makeDrainer({
                consumer,
                producer: makeProducer([]),
                bnConfigManager: { getConfig: (b, cb) => cb(null, bucketConfig) },
            });
            drainer.drainDestination(DESTINATION, err => {
                assert.ifError(err);
                assert.strictEqual(consumer.assigned, null);
                assert.strictEqual(consumer.consumeCalls, 0);
                done();
            });
        });
    });

    describe('run', () => {
        it('should drain every destination and report the totals', done => {
            const destinations = [
                { resource: 'destId', type: 'kafka', host: 'h' },
                { resource: 'otherDestId', type: 'kafka', host: 'h',
                    internalTopic: 'other-topic' },
            ];
            const sent = [];
            const consumer = makeConsumer({
                watermarks: { 0: { lowOffset: 0, highOffset: 1 } },
                committed: { 0: 0 },
                batches: [
                    [makeRecord(OLD_TOPIC, 0, 0, makeEntry())],
                    [makeRecord('other-topic', 0, 0, makeEntry())],
                ],
            });
            const drainer = makeDrainer({
                consumer,
                producer: makeProducer(sent),
                bnConfigManager: { getConfig: (b, cb) => cb(null, bucketConfig) },
                notifConfig: Object.assign({}, notifConfig, { destinations }),
            });
            drainer.run((err, totals) => {
                assert.ifError(err);
                // the second destination is not in the bucket configuration
                assert.deepStrictEqual(totals,
                    { drained: 2, produced: 1, skipped: 1 });
                assert.strictEqual(sent.length, 1);
                // each destination drops its assignment when it is done
                assert.strictEqual(consumer.unassignCalls, 2);
                done();
            });
        });
    });
});
