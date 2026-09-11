const assert = require('assert');
const sinon = require('sinon');
const { ZenkoMetrics } = require('arsenal').metrics;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const DeliveryWorker = require(
    '../../../extensions/notification/deliveryWorker/DeliveryWorker');
const DeliveryProducerPool = require(
    '../../../extensions/notification/deliveryWorker/DeliveryProducerPool');
const {
    buildGroupId,
    createSliceFilter,
} = require('../../../extensions/notification/utils/workgroups');

const DELIVERED_METRIC = 's3_notification_delivery_worker_delivered_total';
const DROPPED_METRIC = 's3_notification_delivery_worker_dropped_total';
const SKIPPED_METRIC = 's3_notification_delivery_worker_skipped_total';
const WATERMARK_METRIC = 's3_notification_delivery_watermark_skipped_total';

const INTERNAL_TOPIC = 'backbeat-bucket-notification';
const BUCKET = 'photos';

const kafkaConfig = { hosts: 'internal-kafka-host:9092' };

function destination(resource, extra) {
    return Object.assign({
        resource,
        type: 'kafka',
        host: 'external-kafka-host',
        port: 9092,
        topic: `${resource}-topic`,
    }, extra || {});
}

function makeNotifConfig(overrides) {
    return Object.assign({
        topic: INTERNAL_TOPIC,
        bucketMetastore: '__metastore',
        maxCachedConfigs: 1000,
        destinations: [
            destination('dest-a'),
            destination('dest-b'),
            destination('dest-own', { internalTopic: 'own-internal-topic' }),
        ],
        deliveryPool: {
            enabled: true,
            source: 'internal',
            topic: 'delivery-topic',
            groupId: 'delivery-group',
            deliveryTimeoutMs: 30000,
            producerIdleMs: 300000,
            maxProducers: 50,
            concurrency: 1000,
            maxQueued: 1000,
        },
    }, overrides || {});
}

// the record the populator publishes on the internal topic today: the event
// and the log attributes, nothing addressed
function legacyRecord(overrides) {
    return Object.assign({
        bucket: BUCKET,
        key: 'logs/2026/a.txt',
        eventType: 's3:ObjectCreated:Put',
        versionId: null,
        dateTime: '2026-09-11T01:00:00.000Z',
        region: 'us-east-1',
        size: '42',
    }, overrides || {});
}

function makeEntry(value, extra) {
    return Object.assign({
        topic: INTERNAL_TOPIC,
        partition: 0,
        offset: 42,
        key: Buffer.from(`${value.bucket}/${value.key}`),
        value: JSON.stringify(value),
    }, extra || {});
}

function queueConfig(id, destinationId, events, filterRules) {
    const config = {
        id,
        queueArn: `arn:scality:bucketnotif:::${destinationId}`,
        events,
    };
    if (filterRules) {
        config.filterRules = filterRules;
    }
    return config;
}

function bucketConfig(queueConfigs) {
    return {
        bucket: BUCKET,
        notificationConfiguration: { queueConfig: queueConfigs },
    };
}

/**
 * A configuration manager answering getConfig from a map, or failing
 * @param {Object} configs - bucket name to configuration
 * @param {Error} [error] - error to answer every lookup with
 * @return {Object} configuration manager stub
 */
function fakeConfigManager(configs, error) {
    return {
        getConfig: sinon.stub().callsFake((bucket, cb) =>
            process.nextTick(() => (error ? cb(error) : cb(null, configs[bucket])))),
        setup: cb => process.nextTick(cb),
    };
}

function fakePool(sendImpl, getError) {
    const send = sinon.stub().callsFake(sendImpl || ((messages, cb) => cb()));
    return {
        send,
        start: sinon.stub(),
        closeAll: sinon.stub().callsFake(cb => cb()),
        get: sinon.stub().callsFake((destinationId, cb) => process.nextTick(
            () => (getError ? cb(getError) : cb(null, { send })))),
    };
}

async function counterValue(name, labels) {
    const data = await ZenkoMetrics.getMetric(name).get();
    const entry = data.values.find(value => Object.entries(labels)
        .every(([label, expected]) => value.labels[label] === expected));
    return entry ? entry.value : 0;
}

// dest-a is claimed statically by wg-mine; modulo 1 gives everything else,
// dest-b included, to wg-other
const doc = {
    configVersion: 1,
    generation: 2,
    topic: INTERNAL_TOPIC,
    workgroups: [
        { id: 'wg-mine', rule: { type: 'static', destinationIds: ['dest-a'] } },
        { id: 'wg-other', rule: { type: 'hashmod', modulo: 1, remainders: [0] } },
    ],
};

function makeWorkgroup(id) {
    return {
        id,
        generation: doc.generation,
        groupId: buildGroupId('delivery-group', id, doc.generation),
        filter: createSliceFilter({ doc, workgroupId: id }),
    };
}

function processEntry(worker, entry) {
    return new Promise(resolve => worker.processKafkaEntry(entry, (...args) => {
        assert.strictEqual(args.length, 0, 'the task must never fail');
        resolve();
    }));
}

describe('notification DeliveryWorker on the internal topic', () => {
    afterEach(() => {
        sinon.restore();
    });

    it('should read the internal topic and default to it', done => {
        const withoutSource = makeNotifConfig();
        delete withoutSource.deliveryPool.source;
        const worker = new DeliveryWorker(kafkaConfig, withoutSource, null,
            { configManager: fakeConfigManager({}) });
        assert.strictEqual(worker.isInternalSource(), true);
        sinon.stub(BackbeatConsumer.prototype, '_init');
        sinon.stub(DeliveryProducerPool.prototype, 'start');
        worker.start(null, () => {});
        setImmediate(() => {
            assert(worker._consumer, 'the consumer was never constructed');
            assert.strictEqual(worker._consumer._topic, INTERNAL_TOPIC);
            assert.strictEqual(worker._consumer._groupId, 'delivery-group');
            done();
        });
    });

    it('should keep reading the delivery topic when the source says so', done => {
        const config = makeNotifConfig();
        config.deliveryPool.source = 'delivery';
        const worker = new DeliveryWorker(kafkaConfig, config);
        assert.strictEqual(worker.isInternalSource(), false);
        sinon.stub(BackbeatConsumer.prototype, '_init');
        sinon.stub(DeliveryProducerPool.prototype, 'start');
        worker.start(null, () => {});
        setImmediate(() => {
            assert.strictEqual(worker._consumer._topic, 'delivery-topic');
            done();
        });
    });

    it('should order by object, since destinations are known only after the lookup', () => {
        const worker = new DeliveryWorker(kafkaConfig, makeNotifConfig(), null,
            { configManager: fakeConfigManager({}) });
        const entry = makeEntry(legacyRecord());
        assert.strictEqual(worker._orderBy({ entry }), `${BUCKET}/logs/2026/a.txt`);
        assert.strictEqual(entry._notifSkip, null);
        assert(entry._notifEntry, 'the parsed payload is stashed');
    });

    it('should deliver an event to every destination it matches', async () => {
        const configManager = fakeConfigManager({
            [BUCKET]: bucketConfig([
                queueConfig('all-to-a', 'dest-a', ['s3:ObjectCreated:*']),
                queueConfig('puts-to-b', 'dest-b', ['s3:ObjectCreated:Put']),
            ]),
        });
        const worker = new DeliveryWorker(kafkaConfig, makeNotifConfig(), null,
            { configManager });
        const pool = fakePool();
        worker._producerPool = pool;
        const beforeA = await counterValue(DELIVERED_METRIC, { target: 'dest-a' });
        const beforeB = await counterValue(DELIVERED_METRIC, { target: 'dest-b' });

        await processEntry(worker, makeEntry(legacyRecord()));

        assert(configManager.getConfig.calledOnceWith(BUCKET));
        assert.strictEqual(pool.get.callCount, 2);
        const targets = pool.get.args.map(args => args[0]).sort();
        assert.deepStrictEqual(targets, ['dest-a', 'dest-b']);
        assert.strictEqual(pool.send.callCount, 2);
        pool.send.args.forEach(([messages]) => {
            assert.strictEqual(messages.length, 1);
            assert.strictEqual(messages[0].key, `${BUCKET}/logs/2026/a.txt`);
            const message = JSON.parse(messages[0].message);
            assert.strictEqual(message.Records[0].eventName, 's3:ObjectCreated:Put');
        });
        const configIds = pool.send.args.map(([messages]) =>
            JSON.parse(messages[0].message).Records[0].s3.configurationId).sort();
        assert.deepStrictEqual(configIds, ['all-to-a', 'puts-to-b']);
        assert.strictEqual(await counterValue(DELIVERED_METRIC, { target: 'dest-a' }),
            beforeA + 1);
        assert.strictEqual(await counterValue(DELIVERED_METRIC, { target: 'dest-b' }),
            beforeB + 1);
    });

    it('should commit an event that matches no destination as a skip', async () => {
        const configManager = fakeConfigManager({
            [BUCKET]: bucketConfig([
                queueConfig('deletes-to-a', 'dest-a', ['s3:ObjectRemoved:*']),
            ]),
        });
        const worker = new DeliveryWorker(kafkaConfig, makeNotifConfig(), null,
            { configManager });
        const pool = fakePool();
        worker._producerPool = pool;
        const before = await counterValue(SKIPPED_METRIC, { reason: 'no_match' });

        await processEntry(worker, makeEntry(legacyRecord()));

        assert(pool.send.notCalled);
        assert.strictEqual(await counterValue(SKIPPED_METRIC, { reason: 'no_match' }),
            before + 1);
    });

    it('should skip a bucket with no configuration at all', async () => {
        const configManager = fakeConfigManager({});
        const worker = new DeliveryWorker(kafkaConfig, makeNotifConfig(), null,
            { configManager });
        const pool = fakePool();
        worker._producerPool = pool;

        await processEntry(worker, makeEntry(legacyRecord()));

        assert(pool.send.notCalled);
    });

    it('should deliver only to the destinations its workgroup owns', async () => {
        const configManager = fakeConfigManager({
            [BUCKET]: bucketConfig([
                queueConfig('all-to-a', 'dest-a', ['s3:ObjectCreated:*']),
                queueConfig('all-to-b', 'dest-b', ['s3:ObjectCreated:*']),
            ]),
        });
        const mine = new DeliveryWorker(kafkaConfig, makeNotifConfig(),
            makeWorkgroup('wg-mine'), { configManager });
        const minePool = fakePool();
        mine._producerPool = minePool;
        const other = new DeliveryWorker(kafkaConfig, makeNotifConfig(),
            makeWorkgroup('wg-other'), { configManager });
        const otherPool = fakePool();
        other._producerPool = otherPool;
        const beforeSkip = await counterValue(SKIPPED_METRIC,
            { workgroup: 'wg-mine', reason: 'not_in_slice' });

        await processEntry(mine, makeEntry(legacyRecord()));
        await processEntry(other, makeEntry(legacyRecord()));

        assert.deepStrictEqual(minePool.get.args.map(a => a[0]), ['dest-a']);
        assert.deepStrictEqual(otherPool.get.args.map(a => a[0]), ['dest-b']);
        assert.strictEqual(await counterValue(SKIPPED_METRIC,
            { workgroup: 'wg-mine', reason: 'not_in_slice' }), beforeSkip + 1);
    });

    it('should not touch the record key when a workgroup filters', () => {
        const worker = new DeliveryWorker(kafkaConfig, makeNotifConfig(),
            makeWorkgroup('wg-mine'), { configManager: fakeConfigManager({}) });
        // on the internal topic the key names the object, which hashes to
        // wg-other under the fixture: the slice must not be applied to it
        const entry = makeEntry(legacyRecord());
        assert.strictEqual(worker._classifyEntry(entry), null);
        assert.strictEqual(worker._orderBy({ entry }), `${BUCKET}/logs/2026/a.txt`);
    });

    it('should skip, count and commit a record below the destination watermark', async () => {
        const configManager = fakeConfigManager({
            [BUCKET]: bucketConfig([
                queueConfig('all-to-a', 'dest-a', ['s3:ObjectCreated:*']),
                queueConfig('all-to-b', 'dest-b', ['s3:ObjectCreated:*']),
            ]),
        });
        // dest-a was served up to offset 100 on partition 0 and 5 on
        // partition 1 before this worker took over; dest-b has no history
        const watermarks = { 'dest-a': { 0: 100, 1: 5 } };
        const worker = new DeliveryWorker(kafkaConfig, makeNotifConfig(), null,
            { configManager, watermarks });
        const pool = fakePool();
        worker._producerPool = pool;
        const before = await counterValue(WATERMARK_METRIC, { destination: 'dest-a' });

        await processEntry(worker, makeEntry(legacyRecord(), { partition: 0, offset: 99 }));
        assert.deepStrictEqual(pool.get.args.map(a => a[0]), ['dest-b']);
        assert.strictEqual(await counterValue(WATERMARK_METRIC, { destination: 'dest-a' }),
            before + 1);

        pool.get.resetHistory();
        await processEntry(worker, makeEntry(legacyRecord(), { partition: 0, offset: 100 }));
        assert.deepStrictEqual(pool.get.args.map(a => a[0]).sort(), ['dest-a', 'dest-b']);

        pool.get.resetHistory();
        await processEntry(worker, makeEntry(legacyRecord(), { partition: 1, offset: 4 }));
        assert.deepStrictEqual(pool.get.args.map(a => a[0]), ['dest-b']);

        pool.get.resetHistory();
        // a partition the watermark says nothing about is delivered
        await processEntry(worker, makeEntry(legacyRecord(), { partition: 2, offset: 0 }));
        assert.deepStrictEqual(pool.get.args.map(a => a[0]).sort(), ['dest-a', 'dest-b']);
    });

    it('should accept watermarks set after construction', async () => {
        const configManager = fakeConfigManager({
            [BUCKET]: bucketConfig([
                queueConfig('all-to-a', 'dest-a', ['s3:ObjectCreated:*']),
            ]),
        });
        const worker = new DeliveryWorker(kafkaConfig, makeNotifConfig(), null,
            { configManager });
        const pool = fakePool();
        worker._producerPool = pool;
        worker.setWatermarks({ 'dest-a': { 0: 10 } });

        await processEntry(worker, makeEntry(legacyRecord(), { partition: 0, offset: 3 }));
        assert(pool.get.notCalled);
    });

    it('should not serve a destination reading its own internal topic', async () => {
        const configManager = fakeConfigManager({
            [BUCKET]: bucketConfig([
                queueConfig('all-to-own', 'dest-own', ['s3:ObjectCreated:*']),
                queueConfig('all-to-a', 'dest-a', ['s3:ObjectCreated:*']),
            ]),
        });
        const worker = new DeliveryWorker(kafkaConfig, makeNotifConfig(), null,
            { configManager });
        const pool = fakePool();
        worker._producerPool = pool;
        const before = await counterValue(SKIPPED_METRIC,
            { reason: 'own_internal_topic' });

        await processEntry(worker, makeEntry(legacyRecord()));

        assert.deepStrictEqual(pool.get.args.map(a => a[0]), ['dest-a']);
        assert.strictEqual(await counterValue(SKIPPED_METRIC,
            { reason: 'own_internal_topic' }), before + 1);
    });

    it('should count a configuration lookup failure as a drop and move on', async () => {
        const configManager = fakeConfigManager({}, new Error('mongo is down'));
        const worker = new DeliveryWorker(kafkaConfig, makeNotifConfig(), null,
            { configManager });
        const pool = fakePool();
        worker._producerPool = pool;
        const before = await counterValue(DROPPED_METRIC, { reason: 'config_error' });

        await processEntry(worker, makeEntry(legacyRecord()));

        assert(pool.send.notCalled);
        assert.strictEqual(await counterValue(DROPPED_METRIC, { reason: 'config_error' }),
            before + 1);
    });

    it('should count a delivery failure per destination and still commit', async () => {
        const configManager = fakeConfigManager({
            [BUCKET]: bucketConfig([
                queueConfig('all-to-a', 'dest-a', ['s3:ObjectCreated:*']),
                queueConfig('all-to-b', 'dest-b', ['s3:ObjectCreated:*']),
            ]),
        });
        const worker = new DeliveryWorker(kafkaConfig, makeNotifConfig(), null,
            { configManager });
        const pool = fakePool((messages, cb) => {
            const record = JSON.parse(messages[0].message).Records[0];
            if (record.s3.configurationId === 'all-to-b') {
                return cb(new Error('broker unreachable'));
            }
            return cb();
        });
        worker._producerPool = pool;
        const beforeDelivered = await counterValue(DELIVERED_METRIC, { target: 'dest-a' });
        const beforeDropped = await counterValue(DROPPED_METRIC,
            { target: 'dest-b', reason: 'delivery_error' });

        await processEntry(worker, makeEntry(legacyRecord()));

        assert.strictEqual(await counterValue(DELIVERED_METRIC, { target: 'dest-a' }),
            beforeDelivered + 1);
        assert.strictEqual(await counterValue(DROPPED_METRIC,
            { target: 'dest-b', reason: 'delivery_error' }), beforeDropped + 1);
    });

    it('should build the configuration manager from the mongo config when none is injected', done => {
        const worker = new DeliveryWorker(kafkaConfig, makeNotifConfig(), null,
            { mongoConfig: { replicaSetHosts: 'mongo:27017', database: 'metadata' } });
        assert.strictEqual(worker._configManager, null);
        sinon.stub(BackbeatConsumer.prototype, '_init');
        sinon.stub(DeliveryProducerPool.prototype, 'start');
        const NotificationConfigManager =
            require('../../../extensions/notification/NotificationConfigManager');
        const setup = sinon.stub(NotificationConfigManager.prototype, 'setup')
            .callsFake(cb => process.nextTick(cb));
        worker.start({ disableConsumer: true }, err => {
            assert.ifError(err);
            assert(setup.calledOnce);
            assert(worker._configManager instanceof NotificationConfigManager);
            done();
        });
    });
});
