const assert = require('assert');

const BackbeatProducer = require('../../lib/BackbeatProducer');
const sinon = require('sinon');
const CODES = require('node-rdkafka').CODES;

const { kafka } = require('../config.json');

describe('backbeatProducer', () => {
    describe('onEventError', () => {
        let backbeatProducer;
        let logErrorStub;
        let emitStub;

        beforeEach(() => {
            backbeatProducer = new BackbeatProducer({ kafka });
            logErrorStub = sinon.stub(backbeatProducer._log, 'error');
            emitStub = sinon.stub(backbeatProducer, 'emit');
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should log and emit error for ERR__ALL_BROKERS_DOWN', () => {
            const error = { code: CODES.ERRORS.ERR__ALL_BROKERS_DOWN, message: 'All brokers down' };
            backbeatProducer.onEventError(error);

            assert(logErrorStub.calledOnce);
            assert(logErrorStub.calledWith('error with producer'));
            assert(emitStub.calledOnce);
            assert(emitStub.calledWith('error', error));
        });

        it('should log and emit error for ERR__TRANSPORT', () => {
            const error = { code: CODES.ERRORS.ERR__TRANSPORT, message: 'Transport error' };
            backbeatProducer.onEventError(error);

            assert(logErrorStub.calledOnce);
            assert(logErrorStub.calledWith('error with producer'));
            assert(emitStub.calledOnce);
            assert(emitStub.calledWith('error', error));
        });

        it('should log rdkafka.error for other errors', () => {
            const error = { code: 'OTHER_ERROR', message: 'Some other error' };
            backbeatProducer.onEventError(error);

            assert(logErrorStub.calledOnce);
            assert(logErrorStub.calledWith('rdkafka.error', { error }));
            assert(emitStub.notCalled);
        });
    });
    it('should use default topic name without prefix', () => {
        const backbeatProducer = new BackbeatProducer({
            kafka,
            topic: 'my-test-topic',
        });
        assert.strictEqual(backbeatProducer._topic, 'my-test-topic');
    });

    it('should use default topic name with prefix', () => {
        process.env.KAFKA_TOPIC_PREFIX = 'testing.';
        const backbeatProducer = new BackbeatProducer({
            kafka,
            topic: 'my-test-topic',
        });
        assert.strictEqual(backbeatProducer._topic, 'testing.my-test-topic');
    });

    it('should use default value if maxRequestSize not provided', () => {
        const backbeatProducer = new BackbeatProducer({
            kafka,
            topic: 'my-test-topic',
        });
        assert.strictEqual(backbeatProducer._maxRequestSize, 5000020);
    });

    it('should use the explicitely provided maxRequestSize', () => {
        const backbeatProducer = new BackbeatProducer({
            kafka,
            topic: 'my-test-topic',
            maxRequestSize: 1234567,
        });
        assert.strictEqual(backbeatProducer._maxRequestSize, 1234567);
    });

    it('should return an error if send() called on producer with no default ' +
    'topic', done => {
        const backbeatProducer = new BackbeatProducer({
            kafka,
        });
        backbeatProducer.send([{ key: 'foo', message: 'bar' }], err => {
            assert(err);
            done();
        });
    });

    it('should send to the topic specified in sendToTopic()', done => {
        const backbeatProducer = new BackbeatProducer({
            kafka,
        });
        // shunt condition on producer initialization
        backbeatProducer._ready = true;
        // shunt produce call
        backbeatProducer._producer.produce = topic => {
            assert.strictEqual(topic, 'custom-topic');
            done();
        };
        backbeatProducer.sendToTopic(
            'custom-topic', [{ key: 'foo', message: 'bar' }], () => {});
    });

    afterEach(() => {
        process.env.KAFKA_TOPIC_PREFIX = '';
    });
});
