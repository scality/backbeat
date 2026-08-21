'use strict';

const assert = require('assert');

const { getField, setField } = require('../../../../lib/config/fields');

describe('config fields', () => {
    describe('getField', () => {
        const config = { queuePopulator: { mongo: { database: 'metadata' } }, log: null };

        it('should read the value at the path', () => {
            assert.strictEqual(getField(config, ['queuePopulator', 'mongo', 'database']), 'metadata');
        });

        it('should read a section', () => {
            assert.deepStrictEqual(getField(config, ['queuePopulator', 'mongo']), { database: 'metadata' });
        });

        it('should leave a missing node undefined', () => {
            assert.strictEqual(getField(config, ['redis', 'host']), undefined);
            assert.strictEqual(getField(config, ['queuePopulator', 'kafka', 'topic']), undefined);
        });

        it('should leave a path through a null node undefined', () => {
            assert.strictEqual(getField(config, ['log', 'logLevel']), undefined);
        });
    });

    describe('setField', () => {
        it('should set a field of an existing section', () => {
            const config = { redis: { host: 'localhost' } };

            setField(config, ['redis', 'port'], '6380');

            assert.deepStrictEqual(config, { redis: { host: 'localhost', port: '6380' } });
        });

        it('should replace the value a field holds', () => {
            const config = { redis: { host: 'localhost' } };

            setField(config, ['redis', 'host'], 'redis');

            assert.deepStrictEqual(config, { redis: { host: 'redis' } });
        });

        it('should set a field at the root', () => {
            const config = {};

            setField(config, ['replicationGroupId'], 'RG00002');

            assert.deepStrictEqual(config, { replicationGroupId: 'RG00002' });
        });

        it('should create the missing intermediate nodes', () => {
            const config = {};

            setField(config, ['queuePopulator', 'mongo', 'authCredentials', 'username'], 'user');

            assert.deepStrictEqual(config,
                                   { queuePopulator: { mongo: { authCredentials: { username: 'user' } } } });
        });

        it('should report a node of the path holding a value', () => {
            const config = { queuePopulator: { mongo: 'localhost:27017' } };

            assert.throws(() => setField(config, ['queuePopulator', 'mongo', 'database'], 'metadata'),
                          /cannot set queuePopulator.mongo.database: queuePopulator.mongo is not an object/);
        });

        // an array is not a section a field belongs in: the per site probe
        // servers are one, and a single port cannot name any of them
        it('should report a node of the path holding an array', () => {
            const config = { queueProcessor: { probeServer: [{ port: 4043, site: 'a' }] } };

            assert.throws(() => setField(config, ['queueProcessor', 'probeServer', 'port'], '8100'),
                          /queueProcessor.probeServer is not an object/);
            assert.deepStrictEqual(config.queueProcessor.probeServer, [{ port: 4043, site: 'a' }]);
        });
    });
});
