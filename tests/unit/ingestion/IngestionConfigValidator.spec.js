'use strict';

const assert = require('assert');
const config = require('../../../lib/Config');
const configValidator = require('../../../extensions/ingestion/IngestionConfigValidator');

const baseExtConfig = {
    auth: { type: 'service', account: 'test-account' },
    topic: 'backbeat-ingestion',
    zookeeperPath: '/test',
    sources: [],
    probeServer: { port: 4000 },
};

const qpBatchMaxRead = config.queuePopulator.batchMaxRead;

describe('IngestionConfigValidator log override', () => {
    it('should pass through log config when set', () => {
        const validated = configValidator({}, {
            ...baseExtConfig,
            log: { logLevel: 'debug', dumpLevel: 'error' },
        });
        assert.deepStrictEqual(validated.log, { logLevel: 'debug', dumpLevel: 'error' });
    });

    it('should leave log undefined when not set, deferring to global config.log', () => {
        const validated = configValidator({}, baseExtConfig);
        assert.strictEqual(validated.log, undefined);
    });

    it('should reject a partial log config with missing dumpLevel', () => {
        let err;
        try {
            configValidator({}, { ...baseExtConfig, log: { logLevel: 'debug' } });
        } catch (e) {
            err = e;
        }
        assert(err, 'expected configValidator to throw on partial log config');
    });
});

describe('IngestionConfigValidator batchMaxRead fallback', () => {
    it('should override queuePopulator.batchMaxRead when set in extension config', () => {
        const validated = configValidator({}, { ...baseExtConfig, batchMaxRead: 500 });
        assert.strictEqual(validated.batchMaxRead, 500);
        assert.notStrictEqual(validated.batchMaxRead, qpBatchMaxRead);
    });

    it('should leave batchMaxRead undefined when not set, allowing fallback to queuePopulator.batchMaxRead', () => {
        const validated = configValidator({}, baseExtConfig);
        assert.strictEqual(validated.batchMaxRead, undefined);
    });

    it('should reject a non-positive batchMaxRead', () => {
        let err;
        try {
            configValidator({}, { ...baseExtConfig, batchMaxRead: 0 });
        } catch (e) {
            err = e;
        }
        assert(err, 'expected configValidator to throw on batchMaxRead: 0');
    });
});
