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

// the validated backbeat config, passed to every extension validator
const globalConfig = { log: { logLevel: 'info', dumpLevel: 'trace' } };

const qpBatchMaxRead = config.queuePopulator.batchMaxRead;

describe('IngestionConfigValidator log override', () => {
    it('should pass through log config when set', () => {
        const validated = configValidator(globalConfig, {
            ...baseExtConfig,
            log: { logLevel: 'debug', dumpLevel: 'error' },
        });
        assert.deepStrictEqual(validated.log, { logLevel: 'debug', dumpLevel: 'error' });
    });

    it('should leave log undefined when not set, deferring to global config.log', () => {
        const validated = configValidator(globalConfig, baseExtConfig);
        assert.strictEqual(validated.log, undefined);
    });

    it('should inherit the level left out of a partial log config', () => {
        const validated = configValidator(globalConfig, {
            ...baseExtConfig,
            log: { logLevel: 'debug' },
        });
        assert.deepStrictEqual(validated.log, { logLevel: 'debug', dumpLevel: 'trace' });
    });

    it('should accept the log level from the environment on its own', () => {
        process.env.EXTENSIONS_INGESTION_LOG_LEVEL = 'debug';
        try {
            const validated = configValidator(globalConfig, { ...baseExtConfig });
            assert.deepStrictEqual(validated.log, { logLevel: 'debug', dumpLevel: 'trace' });
        } finally {
            delete process.env.EXTENSIONS_INGESTION_LOG_LEVEL;
        }
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
