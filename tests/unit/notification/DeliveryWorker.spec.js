const assert = require('assert');
const sinon = require('sinon');
const { ZenkoMetrics } = require('arsenal').metrics;

const DeliveryWorker = require(
    '../../../extensions/notification/deliveryWorker/DeliveryWorker');

const DELIVERED_METRIC = 's3_notification_delivery_worker_delivered_total';
const DROPPED_METRIC = 's3_notification_delivery_worker_dropped_total';
const DELAY_METRIC = 's3_notification_delivery_worker_delivery_delay_seconds';

const kafkaConfig = {
    hosts: 'internal-kafka-host:9092',
};

const notifConfig = {
    destinations: [
        {
            resource: 'destId',
            type: 'kafka',
            host: 'external-kafka-host',
            port: 9092,
            topic: 'dest-topic',
        },
    ],
    deliveryPool: {
        enabled: true,
        topic: 'delivery-topic',
        groupId: 'delivery-group',
        deliveryTimeoutMs: 30000,
        producerIdleMs: 300000,
        maxProducers: 50,
        concurrency: 1000,
        maxQueued: 1000,
    },
};

const notifRecord = {
    destinationId: 'destId',
    configurationId: 'config-1',
    bucket: 'mybucket',
    key: 'mykey',
    eventType: 's3:ObjectCreated:Put',
    dateTime: '2024-08-02T09:19:43.991Z',
    region: 'us-east-1',
    size: 42,
};

function makeEntry(value) {
    return {
        topic: 'delivery-topic',
        partition: 0,
        offset: 42,
        key: Buffer.from('destId'),
        value: typeof value === 'string' ? value : JSON.stringify(value),
    };
}

/**
 * Read the value of a labelled counter, 0 if not observed yet
 * @param {string} name - metric name
 * @param {object} labels - labels to match
 * @return {Promise<number>} current counter value
 */
async function counterValue(name, labels) {
    const data = await ZenkoMetrics.getMetric(name).get();
    const entry = data.values.find(value => Object.entries(labels)
        .every(([label, expected]) => value.labels[label] === expected));
    return entry ? entry.value : 0;
}

/**
 * Read how many observations a labelled histogram received
 * @param {object} labels - labels to match
 * @return {Promise<number>} number of observations
 */
async function delayObservations(labels) {
    const data = await ZenkoMetrics.getMetric(DELAY_METRIC).get();
    const entry = data.values.find(value =>
        value.metricName === `${DELAY_METRIC}_count` &&
        Object.entries(labels).every(([label, expected]) => value.labels[label] === expected));
    return entry ? entry.value : 0;
}

/**
 * Build a producer pool stub
 * @param {function} sendImpl - implementation of producer.send(messages, cb)
 * @param {Error} [getError] - error to fail pool.get() with
 * @return {object} pool stub, with "send" exposing the send stub
 */
function fakePool(sendImpl, getError) {
    const send = sinon.stub().callsFake(sendImpl);
    return {
        send,
        start: sinon.stub(),
        closeAll: sinon.stub().callsFake(cb => cb()),
        get: sinon.stub().callsFake((destinationId, cb) => process.nextTick(
            () => (getError ? cb(getError) : cb(null, { send })))),
    };
}

describe('notification DeliveryWorker', () => {
    let worker;

    beforeEach(() => {
        worker = new DeliveryWorker(kafkaConfig, notifConfig);
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should hold the callback until the delivery report is received', done => {
        let deliveryReportCb = null;
        const pool = fakePool((messages, cb) => {
            deliveryReportCb = cb;
        });
        worker._producerPool = pool;

        let doneCalled = false;
        worker.processKafkaEntry(makeEntry(notifRecord), err => {
            assert.ifError(err);
            doneCalled = true;
        });

        setTimeout(() => {
            assert(pool.send.calledOnce);
            assert.strictEqual(doneCalled, false,
                'callback must not be called before the delivery report');
            deliveryReportCb();
            setImmediate(() => {
                assert.strictEqual(doneCalled, true);
                done();
            });
        }, 50);
    });

    it('should send one record per send call, keyed by bucket and object key', done => {
        const pool = fakePool((messages, cb) => cb());
        worker._producerPool = pool;

        worker.processKafkaEntry(makeEntry(notifRecord), err => {
            assert.ifError(err);
            assert(pool.get.calledOnceWith('destId'));
            const [messages] = pool.send.args[0];
            assert(Array.isArray(messages));
            assert.strictEqual(messages.length, 1);
            assert.strictEqual(messages[0].key, 'mybucket/mykey');
            const message = JSON.parse(messages[0].message);
            assert.strictEqual(message.Records.length, 1);
            assert.strictEqual(message.Records[0].eventName, 's3:ObjectCreated:Put');
            // the configuration id rides in the payload, no config lookup
            assert.strictEqual(message.Records[0].s3.configurationId, 'config-1');
            done();
        });
    });

    it('should count a delivered notification and observe its delay', async () => {
        const pool = fakePool((messages, cb) => cb());
        worker._producerPool = pool;

        const deliveredBefore = await counterValue(DELIVERED_METRIC, { target: 'destId' });
        const observedBefore = await delayObservations({ target: 'destId', status: 'success' });

        await new Promise(resolve => worker.processKafkaEntry(
            makeEntry(notifRecord), err => {
                assert.ifError(err);
                resolve();
            }));

        assert.strictEqual(
            await counterValue(DELIVERED_METRIC, { target: 'destId' }), deliveredBefore + 1);
        assert.strictEqual(
            await delayObservations({ target: 'destId', status: 'success' }), observedBefore + 1);
    });

    it('should drop and not fail the task when the delivery report is an error', async () => {
        const pool = fakePool((messages, cb) => cb(new Error('delivery error')));
        worker._producerPool = pool;

        const droppedBefore = await counterValue(DROPPED_METRIC,
            { target: 'destId', reason: 'delivery_error' });
        const observedBefore = await delayObservations({ target: 'destId', status: 'failure' });

        await new Promise(resolve => worker.processKafkaEntry(
            makeEntry(notifRecord), (...args) => {
                // never call back with an error, the consumer would emit
                // a consumer level 'error' event for it
                assert.strictEqual(args.length, 0);
                resolve();
            }));

        assert.strictEqual(await counterValue(DROPPED_METRIC,
            { target: 'destId', reason: 'delivery_error' }), droppedBefore + 1);
        assert.strictEqual(
            await delayObservations({ target: 'destId', status: 'failure' }), observedBefore + 1);
    });

    it('should drop with a delivery_timeout reason when the message expired', async () => {
        const timeoutError = new Error('Local: Message timed out');
        // ERR__MSG_TIMED_OUT
        timeoutError.code = -192;
        const pool = fakePool((messages, cb) => cb(timeoutError));
        worker._producerPool = pool;

        const droppedBefore = await counterValue(DROPPED_METRIC,
            { target: 'destId', reason: 'delivery_timeout' });

        await new Promise(resolve => worker.processKafkaEntry(
            makeEntry(notifRecord), err => {
                assert.ifError(err);
                resolve();
            }));

        assert.strictEqual(await counterValue(DROPPED_METRIC,
            { target: 'destId', reason: 'delivery_timeout' }), droppedBefore + 1);
    });

    it('should drop an entry that is not valid JSON', async () => {
        const pool = fakePool((messages, cb) => cb());
        worker._producerPool = pool;

        const droppedBefore = await counterValue(DROPPED_METRIC,
            { target: 'unknown', reason: 'parse_error' });

        await new Promise(resolve => worker.processKafkaEntry(
            makeEntry('this is not json'), err => {
                assert.ifError(err);
                resolve();
            }));

        assert(pool.send.notCalled);
        assert.strictEqual(await counterValue(DROPPED_METRIC,
            { target: 'unknown', reason: 'parse_error' }), droppedBefore + 1);
    });

    it('should drop an entry for a destination that is not configured', async () => {
        const pool = fakePool((messages, cb) => cb());
        worker._producerPool = pool;

        const droppedBefore = await counterValue(DROPPED_METRIC,
            { target: 'goneDestId', reason: 'unknown_destination' });

        await new Promise(resolve => worker.processKafkaEntry(
            makeEntry({ ...notifRecord, destinationId: 'goneDestId' }), err => {
                assert.ifError(err);
                resolve();
            }));

        assert(pool.get.notCalled);
        assert(pool.send.notCalled);
        assert.strictEqual(await counterValue(DROPPED_METRIC,
            { target: 'goneDestId', reason: 'unknown_destination' }), droppedBefore + 1);
    });

    it('should drop an entry when no producer can be obtained', async () => {
        const pool = fakePool((messages, cb) => cb(), new Error('connect failed'));
        worker._producerPool = pool;

        const droppedBefore = await counterValue(DROPPED_METRIC,
            { target: 'destId', reason: 'producer_error' });

        await new Promise(resolve => worker.processKafkaEntry(
            makeEntry(notifRecord), err => {
                assert.ifError(err);
                resolve();
            }));

        assert(pool.send.notCalled);
        assert.strictEqual(await counterValue(DROPPED_METRIC,
            { target: 'destId', reason: 'producer_error' }), droppedBefore + 1);
    });

    describe('ordering', () => {
        it('should order by destination and object, not by the kafka key', () => {
            const entry = makeEntry(notifRecord);
            assert.strictEqual(worker._orderBy({ entry }), 'destId|mybucket/mykey');
        });

        it('should stash the parsed entry so it is parsed only once', done => {
            const entry = makeEntry(notifRecord);
            worker._orderBy({ entry });
            assert.deepStrictEqual(entry._notifEntry, notifRecord);

            // the stash is what gets used: an unparseable value would
            // otherwise be dropped
            entry.value = 'this is not json';
            const pool = fakePool((messages, cb) => cb());
            worker._producerPool = pool;
            worker.processKafkaEntry(entry, err => {
                assert.ifError(err);
                assert(pool.send.calledOnce);
                assert.strictEqual(pool.send.args[0][0][0].key, 'mybucket/mykey');
                done();
            });
        });

        it('should leave an unparseable entry unordered', () => {
            const entry = makeEntry('this is not json');
            assert.strictEqual(worker._orderBy({ entry }), undefined);
            assert.strictEqual(entry._notifEntry, undefined);
        });
    });

    describe('isReady', () => {
        it('should not be ready without a consumer', () => {
            assert.strictEqual(worker.isReady(), false);
        });

        it('should follow the consumer readiness', () => {
            worker._consumer = { isReady: () => true };
            assert.strictEqual(worker.isReady(), true);
        });
    });
});
