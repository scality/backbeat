'use strict';

const assert = require('assert');
const configValidator = require('../../../extensions/mongoProcessor/MongoProcessorConfigValidator');

const baseConfig = {
    topic: 'backbeat-ingestion',
    groupId: 'backbeat-ingestion-group',
    probeServer: { port: 4000 },
};

describe('MongoProcessorConfigValidator log override', () => {
    it('should pass through log config when set', () => {
        const validated = configValidator({}, {
            ...baseConfig,
            log: { logLevel: 'warn', dumpLevel: 'error' },
        });
        assert.deepStrictEqual(validated.log, { logLevel: 'warn', dumpLevel: 'error' });
    });

    it('should leave log undefined when not set, deferring to global config.log', () => {
        const validated = configValidator({}, baseConfig);
        assert.strictEqual(validated.log, undefined);
    });

    it('should reject a partial log config with missing dumpLevel', () => {
        let err;
        try {
            configValidator({}, { ...baseConfig, log: { logLevel: 'warn' } });
        } catch (e) {
            err = e;
        }
        assert(err, 'expected configValidator to throw on partial log config');
    });
});
