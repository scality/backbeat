const assert = require('assert');
const sinon = require('sinon');

const MetricsProducer = require('../../../lib/MetricsProducer');

const kafkaConfig = { hosts: 'localhost:9092' };
const extMetrics = { 'us-east-1': { ops: 1, bytes: 128 } };

describe('MetricsProducer', () => {
    afterEach(() => {
        sinon.restore();
    });

    describe('with no metrics configured', () => {
        let mProducer;

        beforeEach(() => {
            mProducer = new MetricsProducer(kafkaConfig, undefined);
        });

        it('should set up without connecting to kafka', done => {
            mProducer.setupProducer(err => {
                assert.ifError(err);
                assert.strictEqual(mProducer.getProducer(), null);
                return done();
            });
        });

        it('should publish nothing', done => {
            mProducer.setupProducer(err => {
                assert.ifError(err);
                return mProducer.publishMetrics(
                    extMetrics, 'completed', 'crr', err => {
                        assert.ifError(err);
                        return done();
                    });
            });
        });

        it('should close without a producer to close', done => {
            mProducer.close(err => {
                assert.ifError(err);
                return done();
            });
        });
    });

    describe('with a metrics topic configured', () => {
        let mProducer;

        beforeEach(() => {
            mProducer = new MetricsProducer(kafkaConfig, { topic: 'metrics' });
            mProducer._producer = { send: sinon.stub().yields() };
        });

        it('should publish to the metrics topic', done => {
            mProducer.publishMetrics(extMetrics, 'completed', 'crr', err => {
                assert.ifError(err);
                sinon.assert.calledOnce(mProducer._producer.send);
                const [[{ message }]] =
                    mProducer._producer.send.firstCall.args;
                const published = JSON.parse(message);
                assert.strictEqual(published.site, 'us-east-1');
                assert.strictEqual(published.ops, 1);
                assert.strictEqual(published.bytes, 128);
                assert.strictEqual(published.type, 'completed');
                assert.strictEqual(published.extension, 'crr');
                return done();
            });
        });
    });
});
