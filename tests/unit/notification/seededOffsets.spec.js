const assert = require('assert');
const sinon = require('sinon');

const { assertSeededOffsets } = require(
    '../../../extensions/notification/deliveryWorker/seededOffsets');

const TOPIC = 'bucket-notification-delivery';
const GROUP_ID = 'backbeat-notification-delivery-wg-a-gen2';

// what committed() answers for a partition the group never committed on
const OFFSET_INVALID = -1001;

function makeLogger() {
    const logger = {
        infos: [],
        warns: [],
        errors: [],
        info: (msg, data) => logger.infos.push({ msg, data }),
        warn: (msg, data) => logger.warns.push({ msg, data }),
        error: (msg, data) => logger.errors.push({ msg, data }),
        debug: () => {},
        trace: () => {},
    };
    return logger;
}

/**
 * Stubs the two calls the assertion makes. Every offset writing method is a
 * spy, so a test can assert the group is only ever read.
 *
 * @param {Object} params - stub params
 * @param {Number[]} params.partitions - partition ids the topic has
 * @param {Object} [params.offsets] - committed offset by partition
 * @param {Object} [params.metadataError] - error getMetadata answers with
 * @param {Object} [params.committedError] - error committed answers with
 * @return {Object} consumer stub
 */
function makeConsumer(params) {
    const consumer = {
        metadataTopic: null,
        subscribe: sinon.spy(),
        assign: sinon.spy(),
        commit: sinon.spy(),
        commitSync: sinon.spy(),
        getMetadata(options, cb) {
            consumer.metadataTopic = options.topic;
            if (params.metadataError) {
                return process.nextTick(() => cb(params.metadataError));
            }
            return process.nextTick(() => cb(null, {
                topics: [{
                    name: options.topic,
                    partitions: params.partitions.map(id => ({ id })),
                }],
            }));
        },
        committed(toppars, timeout, cb) {
            if (params.committedError) {
                return process.nextTick(() => cb(params.committedError));
            }
            return process.nextTick(() => cb(null, toppars.map(tp =>
                Object.assign({}, tp, { offset: params.offsets[tp.partition] }))
            ));
        },
    };
    return consumer;
}

function makeParams(consumer, overrides) {
    return Object.assign({
        kafkaConfig: { hosts: 'localhost:9092' },
        topic: TOPIC,
        groupId: GROUP_ID,
        logger: makeLogger(),
        consumer,
    }, overrides);
}

describe('assertSeededOffsets', () => {
    afterEach(() => {
        delete process.env.KAFKA_TOPIC_PREFIX;
    });

    it('should pass when every partition has a committed offset', done => {
        const consumer = makeConsumer({
            partitions: [0, 1, 2],
            offsets: { 0: 154023, 1: 154990, 2: 153001 },
        });
        assertSeededOffsets(makeParams(consumer), err => {
            assert.ifError(err);
            assert(consumer.subscribe.notCalled);
            assert(consumer.assign.notCalled);
            assert(consumer.commit.notCalled);
            assert(consumer.commitSync.notCalled);
            done();
        });
    });

    it('should fail and name the partitions that were never seeded', done => {
        const consumer = makeConsumer({
            partitions: [0, 1, 2],
            offsets: { 0: 154023, 1: OFFSET_INVALID, 2: 153001 },
        });
        assertSeededOffsets(makeParams(consumer), err => {
            assert(err);
            assert(err.description.includes('partitions 1'));
            assert(err.description.includes(GROUP_ID));
            assert(err.description.includes('preseed'));
            done();
        });
    });

    it('should warn but pass when the group is behind its barrier', done => {
        const consumer = makeConsumer({
            partitions: [0, 1],
            offsets: { 0: 154023, 1: 154900 },
        });
        const params = makeParams(consumer, {
            barriers: { 0: 154023, 1: 154990 },
        });
        assertSeededOffsets(params, err => {
            assert.ifError(err);
            const warned = params.logger.warns.find(entry =>
                entry.msg.includes('behind its cutover barrier'));
            assert(warned);
            assert.deepStrictEqual(warned.data.partitions, [{
                partition: 1,
                barrier: 154990,
                committed: 154900,
            }]);
            done();
        });
    });

    it('should propagate a metadata error', done => {
        const consumer = makeConsumer({
            partitions: [0],
            metadataError: new Error('broker transport failure'),
        });
        assertSeededOffsets(makeParams(consumer), err => {
            assert(err);
            assert(err.description.includes('broker transport failure'));
            done();
        });
    });

    it('should propagate a committed offsets error', done => {
        const consumer = makeConsumer({
            partitions: [0],
            committedError: new Error('coordinator not available'),
        });
        assertSeededOffsets(makeParams(consumer), err => {
            assert(err);
            assert(err.description.includes('coordinator not available'));
            done();
        });
    });

    it('should read the metadata of the prefixed topic', done => {
        process.env.KAFKA_TOPIC_PREFIX = 'ci-';
        const consumer = makeConsumer({
            partitions: [0],
            offsets: { 0: 12 },
        });
        assertSeededOffsets(makeParams(consumer), err => {
            assert.ifError(err);
            assert.strictEqual(consumer.metadataTopic, `ci-${TOPIC}`);
            done();
        });
    });

    it('should fail when the topic has no partitions', done => {
        const consumer = makeConsumer({ partitions: [], offsets: {} });
        assertSeededOffsets(makeParams(consumer), err => {
            assert(err);
            assert(err.description.includes('no partitions'));
            done();
        });
    });
});
