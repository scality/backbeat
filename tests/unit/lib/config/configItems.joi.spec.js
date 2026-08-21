'use strict';

const assert = require('assert');
const joi = require('joi');

const {
    authJoi,
    inheritedAuthJoi,
    logJoi,
    logJoiOptional,
    mongoJoi,
} = require('../../../../lib/config/configItems.joi');
const { envVarMappings } = require('../../../../lib/config/envOverrides');

describe('config items schemas', () => {
    describe('inherited auth', () => {
        const schema = joi.object({
            auth: authJoi.optional(),
            child: joi.object({
                auth: inheritedAuthJoi,
            }),
        });

        const authObject = {
            type: 'service',
            account: 'account1',
        };

        it('fail if auth missing in both parent and child', () => {
            const obj = {
                child: {},
            };

            assert(schema.validate(obj).error);
        });

        it('allow missing auth in child if defined in parent', () => {
            const obj = {
                auth: authObject,
                child: {},
            };

            return schema.validateAsync(obj);
        });

        it('allow missing auth in parent if defined in child', () => {
            const obj = {
                child: {
                    auth: authObject,
                },
            };

            return schema.validateAsync(obj);
        });

        it('allow auth in both parent and child', () => {
            const obj = {
                auth: authObject,
                child: {
                    auth: authObject,
                },
            };

            return schema.validateAsync(obj);
        });
    });

    describe('log', () => {
        // the levels default individually, so that setting one of them leaves
        // the other alone
        it('should default each level of the global config', () => {
            assert.deepStrictEqual(joi.attempt({}, logJoi), { logLevel: 'info', dumpLevel: 'error' });
            assert.deepStrictEqual(joi.attempt({ logLevel: 'debug' }, logJoi),
                                   { logLevel: 'debug', dumpLevel: 'error' });
        });

        // an extension inherits the levels it does not configure from the
        // global log config, passed as the validation context
        it('should inherit the levels an extension does not configure', () => {
            const context = { log: { logLevel: 'info', dumpLevel: 'trace' } };
            const validate = log => joi.attempt(log, logJoiOptional, { context });

            assert.deepStrictEqual(validate({}), { logLevel: 'info', dumpLevel: 'trace' });
            assert.deepStrictEqual(validate({ logLevel: 'debug' }),
                                   { logLevel: 'debug', dumpLevel: 'trace' });
        });

        it('should name the level LOG_LEVEL rather than LOG_LOG_LEVEL', () => {
            assert.deepStrictEqual([...envVarMappings(joi.object({ log: logJoi })).keys()],
                                   ['LOG_LEVEL', 'LOG_DUMP_LEVEL']);
        });
    });

    describe('mongo', () => {
        // the historic names of the fields, which their path does not derive
        it('should name the replica set fields as the entrypoint did', () => {
            const names = [...envVarMappings(joi.object({
                queuePopulator: joi.object({ mongo: mongoJoi.meta({ envVarAlias: 'MONGODB' }) }),
            })).keys()];

            ['MONGODB_HOSTS', 'MONGODB_RS', 'MONGODB_AUTH_USERNAME', 'MONGODB_DATABASE']
                .forEach(name => assert.ok(names.includes(name), `${name} in ${names.join(', ')}`));
        });

        it('should default the replica set hosts and the database', () => {
            const mongo = joi.attempt({}, mongoJoi);

            assert.strictEqual(mongo.replicaSetHosts, 'localhost:27017');
            assert.strictEqual(mongo.database, 'metadata');
            assert.strictEqual(mongo.replicaSet, 'rs0');
        });

        it('should forbid the replica set of a sharded collection', () => {
            assert.throws(() => joi.attempt({ shardCollections: true, replicaSet: 'rs0' }, mongoJoi),
                          /"replicaSet" is not allowed/);
        });
    });
});
