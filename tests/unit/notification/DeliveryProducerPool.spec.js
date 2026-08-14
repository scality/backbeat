const assert = require('assert');
const sinon = require('sinon');

const FakeLogger = require('../../utils/fakeLogger');

const DeliveryProducerPool = require(
    '../../../extensions/notification/deliveryWorker/DeliveryProducerPool');
const DeliveryKafkaProducer = require(
    '../../../extensions/notification/deliveryWorker/DeliveryKafkaProducer');

const destinationsById = {
    destA: {
        resource: 'destA',
        type: 'kafka',
        host: 'external-kafka-host',
        port: 9092,
        topic: 'topic-a',
        pollIntervalMs: 1000,
        requiredAcks: 1,
        compressionType: 'none',
    },
    destB: {
        resource: 'destB',
        type: 'kafka',
        host: 'other-kafka-host',
        port: 9092,
        topic: 'topic-b',
    },
    destC: {
        resource: 'destC',
        type: 'kafka',
        host: 'third-kafka-host',
        topic: 'topic-c',
    },
};

const deliveryPoolConfig = {
    deliveryTimeoutMs: 30000,
    producerIdleMs: 1000,
    maxProducers: 50,
};

/**
 * Build a pool whose producers become ready asynchronously without ever
 * reaching a broker
 * @param {object} [overrides] - deliveryPoolConfig overrides
 * @return {DeliveryProducerPool} pool under test
 */
function makePool(overrides) {
    return new DeliveryProducerPool({
        destinationsById,
        deliveryPoolConfig: { ...deliveryPoolConfig, ...overrides },
        logger: FakeLogger,
    });
}

describe('notification DeliveryProducerPool', () => {
    let connectStub;
    let closeStub;

    beforeEach(() => {
        // emit 'ready' rather than connecting to a broker
        connectStub = sinon.stub(DeliveryKafkaProducer.prototype, 'connect')
            .callsFake(function connect() {
                setTimeout(() => this.emit('ready'), 10);
            });
        closeStub = sinon.stub(DeliveryKafkaProducer.prototype, 'close')
            .callsFake(cb => process.nextTick(cb));
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should bound message retries with the configured delivery timeout', done => {
        const pool = makePool();
        pool.get('destA', (err, producer) => {
            assert.ifError(err);
            const { topicConfig } = producer.producer;
            assert.strictEqual(topicConfig['message.timeout.ms'], 30000);
            // the inherited topic config is preserved
            assert.strictEqual(topicConfig['request.required.acks'], 1);
            assert.strictEqual(topicConfig['request.timeout.ms'], 5000);
            done();
        });
    });

    it('should configure the producer from the destination config', done => {
        const pool = makePool();
        pool.get('destA', (err, producer) => {
            assert.ifError(err);
            assert.strictEqual(producer.producer._kafkaHosts, 'external-kafka-host:9092');
            assert.strictEqual(producer.producer._topic, 'topic-a');
            assert.strictEqual(producer.producer._pollIntervalMs, 1000);
            assert.strictEqual(producer.producer._compressionType, 'none');
            assert.strictEqual(producer.producer._requiredAcks, 1);
            done();
        });
    });

    it('should use the bare host when the destination has no port', done => {
        const pool = makePool();
        pool.get('destC', (err, producer) => {
            assert.ifError(err);
            assert.strictEqual(producer.producer._kafkaHosts, 'third-kafka-host');
            done();
        });
    });

    it('should create a single producer for concurrent gets while connecting', done => {
        const pool = makePool();
        const handles = [];
        const collect = (err, producer) => {
            assert.ifError(err);
            handles.push(producer);
            if (handles.length === 3) {
                assert.strictEqual(connectStub.callCount, 1);
                assert.strictEqual(handles[0], handles[1]);
                assert.strictEqual(handles[1], handles[2]);
                assert.strictEqual(pool._producers.size, 1);
                done();
            }
        };
        pool.get('destA', collect);
        pool.get('destA', collect);
        pool.get('destA', collect);
    });

    it('should reuse a ready producer', done => {
        const pool = makePool();
        pool.get('destA', (err, first) => {
            assert.ifError(err);
            pool.get('destA', (err2, second) => {
                assert.ifError(err2);
                assert.strictEqual(first, second);
                assert.strictEqual(connectStub.callCount, 1);
                done();
            });
        });
    });

    it('should fail every waiter and forget the producer when connecting fails', done => {
        connectStub.restore();
        const connectError = new Error('cannot reach broker');
        sinon.stub(DeliveryKafkaProducer.prototype, 'connect')
            .callsFake(function connect() {
                setTimeout(() => this.emit('error', connectError), 10);
            });
        const pool = makePool();
        let failures = 0;
        const expectError = err => {
            assert.strictEqual(err, connectError);
            failures++;
            if (failures === 2) {
                // forgotten, so the next entry retries instead of waiting
                // on a producer that will never be ready
                assert.strictEqual(pool._producers.size, 0);
                done();
            }
        };
        pool.get('destA', expectError);
        pool.get('destA', expectError);
    });

    it('should fail for a destination that is not configured', done => {
        const pool = makePool();
        pool.get('goneDestId', err => {
            assert(err);
            assert.strictEqual(connectStub.callCount, 0);
            done();
        });
    });

    it('should track deliveries in flight around a send', done => {
        const pool = makePool();
        let deliveryReportCb = null;
        pool.get('destA', (err, producer) => {
            assert.ifError(err);
            sinon.stub(producer.producer, 'send').callsFake((messages, cb) => {
                deliveryReportCb = cb;
            });
            producer.send([{ key: 'k', message: '{}' }], sendErr => {
                assert.ifError(sendErr);
                assert.strictEqual(producer.inFlight, 0);
                assert.strictEqual(pool._inFlight, 0);
                done();
            });
            assert.strictEqual(producer.inFlight, 1);
            assert.strictEqual(pool._inFlight, 1);
            deliveryReportCb();
        });
    });

    describe('reaping idle producers', () => {
        it('should close a producer that has been idle for too long', done => {
            const pool = makePool();
            pool.get('destA', err => {
                assert.ifError(err);
                const entry = pool._producers.get('destA');
                entry.lastUsed = Date.now() - 5000;
                pool._reapIdleProducers();
                // removed from the map before the close completes, so a get
                // racing the close gets a new producer
                assert.strictEqual(pool._producers.size, 0);
                assert.strictEqual(closeStub.callCount, 1);
                done();
            });
        });

        it('should keep a producer that still has deliveries in flight', done => {
            const pool = makePool();
            pool.get('destA', err => {
                assert.ifError(err);
                const entry = pool._producers.get('destA');
                entry.lastUsed = Date.now() - 5000;
                entry.inFlight = 1;
                pool._reapIdleProducers();
                assert.strictEqual(pool._producers.size, 1);
                assert.strictEqual(closeStub.callCount, 0);
                done();
            });
        });

        it('should keep a recently used producer', done => {
            const pool = makePool();
            pool.get('destA', err => {
                assert.ifError(err);
                pool._reapIdleProducers();
                assert.strictEqual(pool._producers.size, 1);
                assert.strictEqual(closeStub.callCount, 0);
                done();
            });
        });
    });

    describe('capacity', () => {
        it('should evict the least recently used idle producer', done => {
            const pool = makePool({ maxProducers: 2 });
            pool.get('destA', errA => {
                assert.ifError(errA);
                pool.get('destB', errB => {
                    assert.ifError(errB);
                    // make destA the least recently used
                    pool._producers.get('destA').lastUsed = Date.now() - 5000;
                    pool.get('destC', errC => {
                        assert.ifError(errC);
                        assert.strictEqual(pool._producers.size, 2);
                        assert.strictEqual(pool._producers.has('destA'), false);
                        assert.strictEqual(pool._producers.has('destB'), true);
                        assert.strictEqual(pool._producers.has('destC'), true);
                        assert.strictEqual(closeStub.callCount, 1);
                        done();
                    });
                });
            });
        });

        it('should exceed the cap rather than evict a busy producer', done => {
            const pool = makePool({ maxProducers: 2 });
            pool.get('destA', errA => {
                assert.ifError(errA);
                pool.get('destB', errB => {
                    assert.ifError(errB);
                    pool._producers.get('destA').inFlight = 1;
                    pool._producers.get('destB').inFlight = 1;
                    pool.get('destC', errC => {
                        assert.ifError(errC);
                        assert.strictEqual(pool._producers.size, 3);
                        assert.strictEqual(closeStub.callCount, 0);
                        done();
                    });
                });
            });
        });
    });

    it('should close every producer on closeAll', done => {
        const pool = makePool();
        pool.start();
        pool.get('destA', errA => {
            assert.ifError(errA);
            pool.get('destB', errB => {
                assert.ifError(errB);
                assert.strictEqual(pool._producers.size, 2);
                pool.closeAll(() => {
                    assert.strictEqual(pool._producers.size, 0);
                    assert.strictEqual(closeStub.callCount, 2);
                    assert.strictEqual(pool._reapTimer, null);
                    // a closed pool hands out no more producers
                    pool.get('destA', err => {
                        assert(err);
                        done();
                    });
                });
            });
        });
    });
});
