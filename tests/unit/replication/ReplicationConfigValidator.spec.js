const assert = require('assert');

const configValidator =
    require('../../../extensions/replication/ReplicationConfigValidator');
const { replication } = require('../../config.json').extensions;

// tests/config.json sets a distinctive (non-default) value so a pass-through is
// provable: queueProcessor 350000.

function withoutMaxPollInterval() {
    const clone = JSON.parse(JSON.stringify(replication));
    delete clone.queueProcessor.maxPollIntervalMs;
    return clone;
}

function withValue(maxPollIntervalMs) {
    return {
        ...replication,
        queueProcessor: {
            ...replication.queueProcessor,
            maxPollIntervalMs,
        },
    };
}

describe('ReplicationConfigValidator maxPollIntervalMs', () => {
    it('should read the queueProcessor value from config', () => {
        const validated = configValidator(null, replication);
        assert.strictEqual(
            validated.queueProcessor.maxPollIntervalMs, 350000);
    });

    it('should leave it unset when not configured', () => {
        const validated = configValidator(null, withoutMaxPollInterval());
        assert.strictEqual(
            validated.queueProcessor.maxPollIntervalMs, undefined);
    });

    it('should reject a value below 45000', () => {
        assert.throws(
            () => configValidator(null, withValue(30000)),
            /greater than or equal to 45000/);
    });
});
