const assert = require('assert');
const sinon = require('sinon');

const MetricsConsumer = require('../../../lib/MetricsConsumer');

// lazyConnect keeps ioredis from opening a socket for the tests that only
// check the wiring
const redisConfig = { host: 'localhost', port: 6379, lazyConnect: true };
const mConfig = { topic: 'backbeat-metrics', groupIdPrefix: 'backbeat-metrics-group' };
const kafkaConfig = { hosts: 'localhost:9092' };

describe('MetricsConsumer', () => {
    let mConsumer;

    beforeEach(() => {
        mConsumer = new MetricsConsumer(redisConfig, mConfig, kafkaConfig, 'crr');
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should keep the redis client it hands to the stats model', () => {
        assert(mConsumer._redisClient);
        assert.strictEqual(mConsumer._statsClient._redis, mConsumer._redisClient);
    });

    describe('close', () => {
        let disconnect;

        beforeEach(() => {
            disconnect = sinon.stub(mConsumer._redisClient, 'disconnect');
        });

        it('should disconnect redis along with the kafka consumer', done => {
            mConsumer._consumer = { close: sinon.stub().yields() };

            mConsumer.close(err => {
                assert.ifError(err);
                sinon.assert.calledOnce(mConsumer._consumer.close);
                sinon.assert.calledOnce(disconnect);
                return done();
            });
        });

        it('should disconnect redis and forward a kafka consumer error', done => {
            const closeError = new Error('close failed');
            mConsumer._consumer = { close: sinon.stub().yields(closeError) };

            mConsumer.close(err => {
                assert.strictEqual(err, closeError);
                sinon.assert.calledOnce(disconnect);
                return done();
            });
        });

        it('should still call back when the consumer is not ready yet', done => {
            assert.strictEqual(mConsumer._consumer, null);

            assert.doesNotThrow(() => mConsumer.close(err => {
                assert.ifError(err);
                sinon.assert.calledOnce(disconnect);
                return done();
            }));
        });
    });

    describe('close, on a live redis client', () => {
        it('should leave the connection unusable', done => {
            const consumer = new MetricsConsumer(
                { host: 'localhost', port: 6379 }, mConfig, kafkaConfig, 'crr');

            consumer.close(err => {
                assert.ifError(err);
                return consumer._redisClient.incrby('test-key', 1, err => {
                    assert(err, 'expected the redis connection to be closed');
                    return done();
                });
            });
        });
    });
});
