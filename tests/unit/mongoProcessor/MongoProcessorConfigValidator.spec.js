'use strict';

const assert = require('assert');
const configValidator = require('../../../extensions/mongoProcessor/MongoProcessorConfigValidator');

const baseExtConfig = {
    topic: 'backbeat-ingestion',
    groupId: 'backbeat-ingestion-group',
    probeServer: { port: 4000 },
};

// the validated backbeat config, passed to every extension validator
const globalConfig = { log: { logLevel: 'info', dumpLevel: 'trace' } };

describe('MongoProcessorConfigValidator log override', () => {
    it('should pass through log config when set', () => {
        const validated = configValidator(globalConfig, {
            ...baseExtConfig,
            log: { logLevel: 'warn', dumpLevel: 'error' },
        });
        assert.deepStrictEqual(validated.log, { logLevel: 'warn', dumpLevel: 'error' });
    });

    it('should leave log undefined when not set, deferring to global config.log', () => {
        const validated = configValidator(globalConfig, baseExtConfig);
        assert.strictEqual(validated.log, undefined);
    });

    it('should inherit the level left out of a partial log config', () => {
        const validated = configValidator(globalConfig, { ...baseExtConfig, log: { logLevel: 'warn' } });
        assert.deepStrictEqual(validated.log, { logLevel: 'warn', dumpLevel: 'trace' });
    });

    it('should accept the log level from the environment on its own', () => {
        process.env.EXTENSIONS_MONGO_PROCESSOR_LOG_LEVEL = 'warn';
        try {
            const validated = configValidator(globalConfig, { ...baseExtConfig });
            assert.deepStrictEqual(validated.log, { logLevel: 'warn', dumpLevel: 'trace' });
        } finally {
            delete process.env.EXTENSIONS_MONGO_PROCESSOR_LOG_LEVEL;
        }
    });
});
