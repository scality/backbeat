const assert = require('assert');

const BackbeatProducer = require('../../lib/BackbeatProducer');

const { kafka, extensions } = require('../config.json');

const CONSUMER_FETCH_MAX_BYTES = 5000020;

describe('backbeatProducer', () => {
    it('should have a default max message bytes value', () => {
        const backbeatProducer = new BackbeatProducer({
            kafka,
            topic: extensions.replication.topic,
        });
        assert.strictEqual(backbeatProducer._messageMaxBytes,
            CONSUMER_FETCH_MAX_BYTES);
    });

    it('should allow setting a custom max message bytes value', () => {
        const backbeatProducer = new BackbeatProducer({
            kafka,
            topic: extensions.replication.topic,
            messageMaxBytes: 1000,
        });
        assert.strictEqual(backbeatProducer._messageMaxBytes, 1000);
    });
});
