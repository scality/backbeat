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

    afterEach(() => {
        process.env.KAFKA_TOPIC_PREFIX = '';
    });
});
