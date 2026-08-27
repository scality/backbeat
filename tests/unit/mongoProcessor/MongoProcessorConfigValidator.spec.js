'use strict';

const assert = require('assert');
const configValidator = require('../../../extensions/mongoProcessor/MongoProcessorConfigValidator');

const baseExtConfig = {
    topic: 'backbeat-ingestion',
    groupId: 'backbeat-ingestion-group',
    probeServer: { port: 4000 },
};

const serviceAuth = { type: 'service', account: 'service-md-ingestion' };

const mongodb = {
    replicaSetHosts: 'mongo:27017',
    database: 'datadb',
    authCredentials: { username: 'u', password: 'p' },
};

// what a D/R sink configures: no other extension supplies these
const drExtConfig = { ...baseExtConfig, mode: 'dr', auth: serviceAuth, mongodb };

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
        const validated = configValidator(globalConfig, { ...baseExtConfig });
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

describe('MongoProcessorConfigValidator mongodb', () => {
    it('should accept a mongodb client config', () => {
        const validated = configValidator(globalConfig, { ...baseExtConfig, mongodb });
        assert.strictEqual(validated.mongodb.replicaSetHosts, 'mongo:27017');
        assert.strictEqual(validated.mongodb.database, 'datadb');
        assert.deepStrictEqual(validated.mongodb.authCredentials,
            { username: 'u', password: 'p' });
    });

    it('should leave mongodb undefined when not set, deferring to the ' +
    'queue populator config', () => {
        const validated = configValidator(globalConfig, { ...baseExtConfig });
        assert.strictEqual(validated.mongodb, undefined);
    });

    it('should require mongodb in dr mode, which runs no queue populator', () => {
        let err;
        try {
            const noMongo = { ...drExtConfig };
            delete noMongo.mongodb;
            configValidator(globalConfig, noMongo);
        } catch (e) {
            err = e;
        }
        assert(err, 'expected configValidator to throw on dr mode with no mongodb');
        assert.match(err.message, /mongodb/);
    });

    it('should reject credentials missing a password', () => {
        let err;
        try {
            configValidator(globalConfig, {
                ...baseExtConfig,
                mongodb: { ...mongodb, authCredentials: { username: 'u' } },
            });
        } catch (e) {
            err = e;
        }
        assert(err, 'expected configValidator to throw on partial credentials');
    });
});

describe('MongoProcessorConfigValidator mode', () => {
    it('should default to ingestion so an existing config is unchanged', () => {
        const validated = configValidator(globalConfig, { ...baseExtConfig });
        assert.strictEqual(validated.mode, 'ingestion');
    });

    it('should accept the dr mode', () => {
        const validated = configValidator(globalConfig, { ...drExtConfig });
        assert.strictEqual(validated.mode, 'dr');
    });

    it('should accept the mode from the environment', () => {
        process.env.EXTENSIONS_MONGO_PROCESSOR_MODE = 'dr';
        try {
            const validated = configValidator(globalConfig,
                { ...baseExtConfig, auth: serviceAuth, mongodb });
            assert.strictEqual(validated.mode, 'dr');
        } finally {
            delete process.env.EXTENSIONS_MONGO_PROCESSOR_MODE;
        }
    });

    it('should reject a mode with no implementation', () => {
        let err;
        try {
            configValidator(globalConfig, { ...baseExtConfig, mode: 'sideways' });
        } catch (e) {
            err = e;
        }
        assert(err, 'expected configValidator to throw on an unknown mode');
        assert.match(err.message, /mode/);
    });
});

describe('MongoProcessorConfigValidator auth', () => {
    it('should accept a service account', () => {
        const validated = configValidator(globalConfig,
            { ...baseExtConfig, auth: serviceAuth });
        assert.strictEqual(validated.auth.type, 'service');
        assert.strictEqual(validated.auth.account, 'service-md-ingestion');
    });

    it('should leave auth undefined when not set, deferring to the ' +
    'ingestion extension config', () => {
        const validated = configValidator(globalConfig, { ...baseExtConfig });
        assert.strictEqual(validated.auth, undefined);
    });

    it('should require auth in dr mode, which enables no other extension', () => {
        let err;
        try {
            const noAuth = { ...drExtConfig };
            delete noAuth.auth;
            configValidator(globalConfig, noAuth);
        } catch (e) {
            err = e;
        }
        assert(err, 'expected configValidator to throw on dr mode with no auth');
        assert.match(err.message, /auth/);
    });

    it('should reject a service auth missing the account', () => {
        let err;
        try {
            configValidator(globalConfig, { ...baseExtConfig, auth: { type: 'service' } });
        } catch (e) {
            err = e;
        }
        assert(err, 'expected configValidator to throw on a service auth with no account');
        assert.match(err.message, /account/);
    });
});
