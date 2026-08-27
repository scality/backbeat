const assert = require('assert');
const sinon = require('sinon');
const { ZenkoMetrics } = require('arsenal').metrics;

const FakeLogger = require('../../utils/fakeLogger');

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const DeliveryWorker = require(
    '../../../extensions/notification/deliveryWorker/DeliveryWorker');
const DeliveryProducerPool = require(
    '../../../extensions/notification/deliveryWorker/DeliveryProducerPool');
const { DELIVERY_POOL_PROBE_PORT_ENV, resolveProbeServerConfig } = require(
    '../../../extensions/notification/deliveryWorker/probeConfig');
const {
    BARRIER_KEY,
    SKIP_BARRIER,
    SKIP_NOT_IN_SLICE,
    buildBarrierRecord,
    buildGroupId,
    createSliceFilter,
    validateWorkgroupsDoc,
} = require('../../../extensions/notification/utils/workgroups');

const DELIVERED_METRIC = 's3_notification_delivery_worker_delivered_total';
const DROPPED_METRIC = 's3_notification_delivery_worker_dropped_total';
const DELAY_METRIC = 's3_notification_delivery_worker_delivery_delay_seconds';
const SKIPPED_METRIC = 's3_notification_delivery_worker_skipped_total';
const BARRIER_METRIC = 's3_notification_delivery_worker_barrier_seen_total';

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

// 'destId' is claimed statically, so it is owned by wg-mine whatever it
// hashes to, and modulo 1 gives every other destination to wg-other
const workgroupsDocFixture = {
    configVersion: 1,
    generation: 3,
    topic: 'delivery-topic',
    workgroups: [
        { id: 'wg-mine', rule: { type: 'static', destinationIds: ['destId'] } },
        { id: 'wg-other', rule: { type: 'hashmod', modulo: 1, remainders: [0] } },
    ],
};

const workgroup = {
    id: 'wg-mine',
    generation: 3,
    groupId: buildGroupId(notifConfig.deliveryPool.groupId, 'wg-mine', 3),
    filter: createSliceFilter({
        doc: workgroupsDocFixture,
        workgroupId: 'wg-mine',
    }),
};

function makeEntry(value, key) {
    return {
        topic: 'delivery-topic',
        partition: 0,
        offset: 42,
        key: key === undefined ? Buffer.from('destId') : key,
        value: typeof value === 'string' ? value : JSON.stringify(value),
    };
}

function makeBarrierEntry(generation) {
    return makeEntry(buildBarrierRecord({ generation, partition: 0 }),
        Buffer.from(BARRIER_KEY));
}

function makeForeignEntry(value) {
    return makeEntry(value, Buffer.from('otherDest'));
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
 * Compare a rendered label set against a complete expected one. The loose
 * helpers above would match a workgroup labelled series too, which is exactly
 * what the flag-off assertions have to tell apart
 * @param {object} actual - labels of a rendered series
 * @param {object} expected - the complete label set expected
 * @return {boolean} true when the two label sets are identical
 */
function sameLabels(actual, expected) {
    const keys = Object.keys(actual || {});
    return keys.length === Object.keys(expected).length &&
        keys.every(key => actual[key] === expected[key]);
}

/**
 * Read the value of the counter series whose label set is exactly the one given
 * @param {string} name - metric name
 * @param {object} labels - the complete label set of the series
 * @return {Promise<number>} current counter value
 */
async function exactCounterValue(name, labels) {
    const data = await ZenkoMetrics.getMetric(name).get();
    const entry = data.values.find(value => sameLabels(value.labels, labels));
    return entry ? entry.value : 0;
}

/**
 * Read how many observations the histogram series with exactly these labels got
 * @param {object} labels - the complete label set of the series
 * @return {Promise<number>} number of observations
 */
async function exactDelayObservations(labels) {
    const data = await ZenkoMetrics.getMetric(DELAY_METRIC).get();
    const entry = data.values.find(value =>
        value.metricName === `${DELAY_METRIC}_count` &&
        sameLabels(value.labels, labels));
    return entry ? entry.value : 0;
}

/**
 * Sum a counter over every series matching the labels given, whatever other
 * labels those series carry
 * @param {string} name - metric name
 * @param {object} labels - labels to match
 * @return {Promise<number>} sum of the matching series
 */
async function counterTotal(name, labels) {
    const data = await ZenkoMetrics.getMetric(name).get();
    return data.values
        .filter(value => Object.entries(labels)
            .every(([label, expected]) => value.labels[label] === expected))
        .reduce((sum, value) => sum + value.value, 0);
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

    describe('probe server config', () => {
        const probeServer = { bindAddress: '0.0.0.0', port: 8900 };
        const withProbe = { ...notifConfig.deliveryPool, probeServer };

        it('should keep the configured port when the environment is silent', () => {
            assert.strictEqual(resolveProbeServerConfig(withProbe, {}), probeServer);
        });

        it('should let the environment give this worker its own port', () => {
            const resolved = resolveProbeServerConfig(withProbe,
                { [DELIVERY_POOL_PROBE_PORT_ENV]: '8902' });
            assert.strictEqual(resolved.port, 8902);
            assert.strictEqual(resolved.bindAddress, '0.0.0.0');
            // the configured object is left alone
            assert.strictEqual(probeServer.port, 8900);
        });

        it('should fall back to the configured port and warn for a bad override', () => {
            ['notaport', '0', '70000', '8900abc'].forEach(value => {
                const logger = { ...FakeLogger, warn: sinon.stub() };
                const resolved = resolveProbeServerConfig(withProbe,
                    { [DELIVERY_POOL_PROBE_PORT_ENV]: value }, logger);
                assert.strictEqual(resolved.port, 8900, `for value "${value}"`);
                assert(logger.warn.calledOnce, `no warning for value "${value}"`);
                assert.strictEqual(logger.warn.args[0][1].value, value);
            });
        });

        it('should fall back quietly when the override is empty', () => {
            ['', '   '].forEach(value => {
                const logger = { ...FakeLogger, warn: sinon.stub() };
                const resolved = resolveProbeServerConfig(withProbe,
                    { [DELIVERY_POOL_PROBE_PORT_ENV]: value }, logger);
                assert.strictEqual(resolved, probeServer, `for value "${value}"`);
                assert(logger.warn.notCalled, `unexpected warning for value "${value}"`);
            });
        });

        it('should stay undefined when no probe server is configured', () => {
            assert.strictEqual(
                resolveProbeServerConfig(notifConfig.deliveryPool, {}), undefined);
            assert.strictEqual(resolveProbeServerConfig(undefined, {}), undefined);
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

    describe('workgroups', () => {
        let wgWorker;

        beforeEach(() => {
            wgWorker = new DeliveryWorker(kafkaConfig, notifConfig, workgroup);
        });

        it('should build its filter from a legal workgroups document', () => {
            assert.ifError(validateWorkgroupsDoc(workgroupsDocFixture).error);
        });

        describe('consumer group id', () => {
            /**
             * Start a worker far enough to construct its consumer, without
             * connecting anything: _init is where BackbeatConsumer builds its
             * rdkafka client
             * @param {DeliveryWorker} target - worker to start
             * @param {function} cb - callback: cb(consumer)
             * @return {undefined}
             */
            function captureConsumer(target, cb) {
                sinon.stub(BackbeatConsumer.prototype, '_init');
                sinon.stub(DeliveryProducerPool.prototype, 'start');
                target.start(null, () => {});
                setImmediate(() => {
                    const consumer = target._consumer;
                    assert(consumer, 'the consumer was never constructed');
                    cb(consumer);
                });
            }

            it('should join the configured group when flag-off', done => {
                captureConsumer(worker, consumer => {
                    assert.strictEqual(consumer._groupId, 'delivery-group');
                    done();
                });
            });

            it('should join the workgroup group when flag-on', done => {
                captureConsumer(wgWorker, consumer => {
                    assert.strictEqual(consumer._groupId,
                        'delivery-group-wg-mine-gen3');
                    done();
                });
            });
        });

        describe('flag-off', () => {
            it('should render a delivered series with no workgroup label', async () => {
                worker._producerPool = fakePool((messages, cb) => cb());

                await new Promise(resolve => worker.processKafkaEntry(
                    makeEntry(notifRecord), err => {
                        assert.ifError(err);
                        resolve();
                    }));

                const data = await ZenkoMetrics.getMetric(DELIVERED_METRIC).get();
                const entry = data.values.find(value =>
                    sameLabels(value.labels, { target: 'destId' }));
                assert(entry, 'no delivered series without a workgroup label');
                assert.deepStrictEqual(Object.keys(entry.labels), ['target']);
            });

            it('should skip a barrier record and commit it', async () => {
                const pool = fakePool((messages, cb) => cb());
                worker._producerPool = pool;

                const labels = { reason: SKIP_BARRIER };
                const before = await exactCounterValue(SKIPPED_METRIC, labels);

                await new Promise(resolve => worker.processKafkaEntry(
                    makeBarrierEntry(3), (...args) => {
                        assert.strictEqual(args.length, 0);
                        resolve();
                    }));

                assert(pool.send.notCalled);
                assert.strictEqual(
                    await exactCounterValue(SKIPPED_METRIC, labels), before + 1);
            });

            it('should deliver every non barrier record', async () => {
                const pool = fakePool((messages, cb) => cb());
                worker._producerPool = pool;

                await new Promise(resolve => worker.processKafkaEntry(
                    makeForeignEntry(notifRecord), err => {
                        assert.ifError(err);
                        resolve();
                    }));

                assert(pool.send.calledOnce);
            });
        });

        describe('flag-on metric labels', () => {
            it('should label a delivered sample and its delay', async () => {
                wgWorker._producerPool = fakePool((messages, cb) => cb());

                const labels = { workgroup: 'wg-mine', target: 'destId' };
                const deliveredBefore =
                    await exactCounterValue(DELIVERED_METRIC, labels);
                const observedBefore = await exactDelayObservations(
                    { ...labels, status: 'success' });

                await new Promise(resolve => wgWorker.processKafkaEntry(
                    makeEntry(notifRecord), err => {
                        assert.ifError(err);
                        resolve();
                    }));

                assert.strictEqual(await exactCounterValue(DELIVERED_METRIC, labels),
                    deliveredBefore + 1);
                assert.strictEqual(await exactDelayObservations(
                    { ...labels, status: 'success' }), observedBefore + 1);
            });

            it('should label a dropped sample and its delay', async () => {
                wgWorker._producerPool = fakePool(
                    (messages, cb) => cb(new Error('delivery error')));

                const labels = {
                    workgroup: 'wg-mine',
                    target: 'destId',
                    reason: 'delivery_error',
                };
                const droppedBefore =
                    await exactCounterValue(DROPPED_METRIC, labels);
                const observedBefore = await exactDelayObservations(
                    { workgroup: 'wg-mine', target: 'destId', status: 'failure' });

                await new Promise(resolve => wgWorker.processKafkaEntry(
                    makeEntry(notifRecord), err => {
                        assert.ifError(err);
                        resolve();
                    }));

                assert.strictEqual(await exactCounterValue(DROPPED_METRIC, labels),
                    droppedBefore + 1);
                assert.strictEqual(await exactDelayObservations(
                    { workgroup: 'wg-mine', target: 'destId', status: 'failure' }),
                    observedBefore + 1);
            });
        });

        describe('records outside the slice', () => {
            const skipLabels = {
                workgroup: 'wg-mine',
                reason: SKIP_NOT_IN_SLICE,
            };

            it('should commit a foreign record without delivering it', async () => {
                const pool = fakePool((messages, cb) => cb());
                wgWorker._producerPool = pool;

                const before = await exactCounterValue(SKIPPED_METRIC, skipLabels);

                await new Promise(resolve => wgWorker.processKafkaEntry(
                    makeForeignEntry({ ...notifRecord, destinationId: 'otherDest' }),
                    (...args) => {
                        assert.strictEqual(args.length, 0);
                        resolve();
                    }));

                assert(pool.get.notCalled);
                assert(pool.send.notCalled);
                assert.strictEqual(
                    await exactCounterValue(SKIPPED_METRIC, skipLabels), before + 1);
            });

            it('should never parse the payload of a foreign record', async () => {
                wgWorker._producerPool = fakePool((messages, cb) => cb());

                const skippedBefore =
                    await exactCounterValue(SKIPPED_METRIC, skipLabels);
                const parseBefore =
                    await counterTotal(DROPPED_METRIC, { reason: 'parse_error' });

                await new Promise(resolve => wgWorker.processKafkaEntry(
                    makeForeignEntry('this is not json'), err => {
                        assert.ifError(err);
                        resolve();
                    }));

                assert.strictEqual(
                    await exactCounterValue(SKIPPED_METRIC, skipLabels),
                    skippedBefore + 1);
                assert.strictEqual(
                    await counterTotal(DROPPED_METRIC, { reason: 'parse_error' }),
                    parseBefore);
            });

            it('should never count a foreign record as an unknown destination', async () => {
                wgWorker._producerPool = fakePool((messages, cb) => cb());

                const droppedBefore = await counterTotal(DROPPED_METRIC,
                    { reason: 'unknown_destination' });

                await new Promise(resolve => wgWorker.processKafkaEntry(
                    makeForeignEntry({ ...notifRecord, destinationId: 'goneDestId' }),
                    err => {
                        assert.ifError(err);
                        resolve();
                    }));

                assert.strictEqual(await counterTotal(DROPPED_METRIC,
                    { reason: 'unknown_destination' }), droppedBefore);
            });
        });

        describe('the skip decision', () => {
            it('should stash a null reason for a record this workgroup owns', () => {
                const entry = makeEntry(notifRecord);
                assert.strictEqual(wgWorker._orderBy({ entry }),
                    'destId|mybucket/mykey');
                assert.strictEqual(entry._notifSkip, null);
            });

            it('should stash the reason and leave a foreign record unordered', () => {
                const entry = makeForeignEntry(notifRecord);
                assert.strictEqual(wgWorker._orderBy({ entry }), undefined);
                assert.strictEqual(entry._notifSkip, SKIP_NOT_IN_SLICE);
                assert.strictEqual(entry._notifEntry, undefined);
            });

            it('should stash a null reason when no workgroup is configured', () => {
                const entry = makeEntry(notifRecord);
                assert.strictEqual(worker._orderBy({ entry }),
                    'destId|mybucket/mykey');
                assert.strictEqual(entry._notifSkip, null);
            });

            it('should leave a barrier record unordered with no workgroup', () => {
                const entry = makeBarrierEntry(3);
                assert.strictEqual(worker._orderBy({ entry }), undefined);
                assert.strictEqual(entry._notifSkip, SKIP_BARRIER);
            });

            it('should classify from the key when nothing was stashed', async () => {
                const pool = fakePool((messages, cb) => cb());
                wgWorker._producerPool = pool;

                const before = await exactCounterValue(SKIPPED_METRIC,
                    { workgroup: 'wg-mine', reason: SKIP_NOT_IN_SLICE });
                const entry = makeForeignEntry(notifRecord);
                assert.strictEqual(entry._notifSkip, undefined);

                await new Promise(resolve => wgWorker.processKafkaEntry(entry,
                    err => {
                        assert.ifError(err);
                        resolve();
                    }));

                assert(pool.send.notCalled);
                assert.strictEqual(await exactCounterValue(SKIPPED_METRIC,
                    { workgroup: 'wg-mine', reason: SKIP_NOT_IN_SLICE }), before + 1);
            });

            it('should trust a stashed decision over the key', async () => {
                const pool = fakePool((messages, cb) => cb());
                wgWorker._producerPool = pool;

                const entry = makeForeignEntry(notifRecord);
                entry._notifSkip = null;

                await new Promise(resolve => wgWorker.processKafkaEntry(entry,
                    err => {
                        assert.ifError(err);
                        resolve();
                    }));

                assert(pool.send.calledOnce);
            });
        });

        describe('barrier records', () => {
            it('should count a barrier of the running generation as current', async () => {
                const pool = fakePool((messages, cb) => cb());
                wgWorker._producerPool = pool;

                const matchLabels = { workgroup: 'wg-mine', match: 'current' };
                const skipLabels = { workgroup: 'wg-mine', reason: SKIP_BARRIER };
                const seenBefore = await exactCounterValue(BARRIER_METRIC, matchLabels);
                const skippedBefore = await exactCounterValue(SKIPPED_METRIC, skipLabels);

                await new Promise(resolve => wgWorker.processKafkaEntry(
                    makeBarrierEntry(3), (...args) => {
                        assert.strictEqual(args.length, 0);
                        resolve();
                    }));

                assert(pool.send.notCalled);
                assert.strictEqual(
                    await exactCounterValue(BARRIER_METRIC, matchLabels), seenBefore + 1);
                assert.strictEqual(
                    await exactCounterValue(SKIPPED_METRIC, skipLabels), skippedBefore + 1);
            });

            it('should count a barrier of another generation as other', async () => {
                wgWorker._producerPool = fakePool((messages, cb) => cb());

                const labels = { workgroup: 'wg-mine', match: 'other' };
                const before = await exactCounterValue(BARRIER_METRIC, labels);

                await new Promise(resolve => wgWorker.processKafkaEntry(
                    makeBarrierEntry(2), (...args) => {
                        assert.strictEqual(args.length, 0);
                        resolve();
                    }));

                assert.strictEqual(
                    await exactCounterValue(BARRIER_METRIC, labels), before + 1);
            });

            it('should count a barrier keyed record with no barrier payload', async () => {
                wgWorker._producerPool = fakePool((messages, cb) => cb());

                const labels = { workgroup: 'wg-mine', match: 'other' };
                const before = await exactCounterValue(BARRIER_METRIC, labels);

                await new Promise(resolve => wgWorker.processKafkaEntry(
                    makeEntry('this is not json', Buffer.from(BARRIER_KEY)),
                    (...args) => {
                        assert.strictEqual(args.length, 0);
                        resolve();
                    }));

                assert.strictEqual(
                    await exactCounterValue(BARRIER_METRIC, labels), before + 1);
            });
        });
    });
});
