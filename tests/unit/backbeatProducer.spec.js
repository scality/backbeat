const assert = require('assert');

const BackbeatProducer = require('../../lib/BackbeatProducer');

const { kafka } = require('../config.json');

describe('backbeatProducer', () => {
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
