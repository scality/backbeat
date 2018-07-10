const assert = require('assert');

const BackbeatConsumer = require('../../lib/BackbeatConsumer');

const { kafka } = require('../config.json');

describe('backbeatConsumer', () => {
    it('should use default topic name without prefix', () => {
        const backbeatConsumer = new BackbeatConsumer({
            kafka,
            groupId: 'unittest-group',
            topic: 'my-test-topic',
        });
        assert.strictEqual(backbeatConsumer._topic, 'my-test-topic');
    });

    it('should use default topic name with prefix', () => {
        process.env.KAFKA_TOPIC_PREFIX = 'testing.';
        const backbeatConsumer = new BackbeatConsumer({
            kafka,
            groupId: 'unittest-group',
            topic: 'my-test-topic',
        });
        assert.strictEqual(backbeatConsumer._topic, 'testing.my-test-topic');
    });

    afterEach(() => {
        process.env.KAFKA_TOPIC_PREFIX = '';
    });
});

