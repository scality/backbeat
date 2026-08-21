'use strict';

const assert = require('assert');
const joi = require('joi');

const {
    applyEnvOverrides,
    envVarMappings,
} = require('../../../../lib/config/envOverrides');
const {
    probeServerJoi,
    probeServerPerSite,
} = require('../../../../lib/config/configItems.joi');

describe('config env var mapping', () => {
    it('should derive names from the config path', () => {
        const schema = joi.object({
            hosts: joi.string(),
            queuePopulator: joi.object({
                batchMaxRead: joi.number(),
                mongo: joi.object({ replicaSetHosts: joi.string() }),
            }),
            queueProcessor: joi.object({
                minMPUSizeMB: joi.number(),
                retry: joi.object({ aws_s3: joi.object({ timeoutS: joi.number() }) }), // eslint-disable-line camelcase
            }),
        });

        assert.deepStrictEqual([...envVarMappings(schema).keys()], [
            'HOSTS',
            'QUEUE_POPULATOR_BATCH_MAX_READ',
            'QUEUE_POPULATOR_MONGO_REPLICA_SET_HOSTS',
            'QUEUE_PROCESSOR_MIN_MPU_SIZE_MB',
            'QUEUE_PROCESSOR_RETRY_AWS_S3_TIMEOUT_S',
        ]);
    });

    it('should prefix the names of an extension schema', () => {
        const schema = joi.object({ topic: joi.string(), consumer: joi.object({ groupId: joi.string() }) });

        assert.deepStrictEqual([...envVarMappings(schema, ['extensions', 'gc']).keys()],
                               ['EXTENSIONS_GC_TOPIC', 'EXTENSIONS_GC_CONSUMER_GROUP_ID']);
    });

    it('should not derive a name for the fields of an object with unconstrained keys', () => {
        const schema = joi.object({
            producerParams: joi.object().unknown(true),
            sites: joi.object().pattern(joi.string(), joi.object({ port: joi.number() })),
        });

        assert.deepStrictEqual([...envVarMappings(schema).keys()], []);
    });

    it('should not derive a name for a field the schema forbids', () => {
        const schema = joi.object({
            kafka: joi.object({ hosts: joi.forbidden(), site: joi.string() }),
        });

        assert.deepStrictEqual([...envVarMappings(schema).keys()], ['KAFKA_SITE']);
    });

    it('should reject a schema deriving the same name for two fields', () => {
        // eslint-disable-next-line camelcase
        const schema = joi.object({ logLevel: joi.string(), log_level: joi.string() });

        assert.throws(() => envVarMappings(schema), /LOG_LEVEL maps to both logLevel and log_level/);
    });

    describe('name annotations', () => {
        it('should rename the segment a node contributes, for itself and its children', () => {
            const schema = joi.object({
                destination: joi.object({
                    transport: joi.string(),
                    auth: joi.object({ type: joi.string() }),
                }).meta({ env: 'DEST' }),
            });

            assert.deepStrictEqual([...envVarMappings(schema, ['extensions', 'replication']).keys()],
                                   ['EXTENSIONS_REPLICATION_DEST_TRANSPORT',
                                       'EXTENSIONS_REPLICATION_DEST_AUTH_TYPE']);
        });

        it('should add a name replacing the path within the schema', () => {
            const schema = joi.object({
                queuePopulator: joi.object({
                    mongo: joi.object({ database: joi.string() }).meta({ envVarAlias: 'MONGODB' }),
                }),
            });

            assert.deepStrictEqual([...envVarMappings(schema).keys()],
                                   ['QUEUE_POPULATOR_MONGO_DATABASE', 'MONGODB_DATABASE']);
        });

        it('should keep the extension prefix of an alias', () => {
            const schema = joi.object({
                bucketTasksTopic: joi.string().meta({ envVarAlias: 'BUCKET_TASK_TOPIC' }),
            });

            assert.deepStrictEqual([...envVarMappings(schema, ['extensions', 'lifecycle']).keys()],
                                   ['EXTENSIONS_LIFECYCLE_BUCKET_TASKS_TOPIC',
                                       'EXTENSIONS_LIFECYCLE_BUCKET_TASK_TOPIC']);
        });

        it('should only annotate the use site of a shared schema', () => {
            const shared = joi.object({ host: joi.string() });
            const schema = joi.object({
                s3: shared.meta({ env: 'CLOUDSERVER' }),
                vaultAdmin: shared,
            });

            assert.deepStrictEqual([...envVarMappings(schema).keys()],
                                   ['CLOUDSERVER_HOST', 'VAULT_ADMIN_HOST']);
        });

    });

    describe('value injection', () => {
        const schema = joi.object({
            host: joi.string(),
            port: joi.number(),
            enabled: joi.boolean(),
            allowFrom: joi.array().items(joi.string()),
            topics: joi.array().items(joi.object({ name: joi.string() })),
            sentinels: joi.alternatives([joi.string(), joi.array()]),
        });
        const apply = env => applyEnvOverrides({}, schema, [], env);

        it('should leave strings and numbers for joi to coerce', () => {
            assert.deepStrictEqual(apply({ HOST: 'h', PORT: '8000' }), { host: 'h', port: '8000' });
            assert.strictEqual(joi.attempt(apply({ PORT: '8000' }), schema).port, 8000);
        });

        it('should accept the usual boolean spellings', () => {
            ['true', 'TRUE', '1', 'y', 'yes', 'on'].forEach(value =>
                assert.strictEqual(apply({ ENABLED: value }).enabled, true, value));
            ['false', 'FALSE', '0', 'n', 'no', 'off'].forEach(value =>
                assert.strictEqual(apply({ ENABLED: value }).enabled, false, value));
        });

        it('should leave an unknown boolean spelling for joi to reject', () => {
            assert.throws(() => joi.attempt(apply({ ENABLED: 'maybe' }), schema));
        });

        it('should split a comma separated list into an array', () => {
            assert.deepStrictEqual(apply({ ALLOW_FROM: '10.0.0.0/8, ::1' }).allowFrom,
                                  ['10.0.0.0/8', '::1']);
            assert.deepStrictEqual(apply({ ALLOW_FROM: '::1' }).allowFrom, ['::1']);
        });

        it('should parse a structured value from JSON', () => {
            assert.deepStrictEqual(apply({ TOPICS: '[{ "name": "t1" }]' }).topics, [{ name: 't1' }]);
            assert.deepStrictEqual(apply({ SENTINELS: '[{ "host": "h", "port": 26379 }]' }).sentinels,
                                   [{ host: 'h', port: 26379 }]);
        });

        it('should report an invalid JSON value', () => {
            assert.throws(() => apply({ TOPICS: '[{ "name" }]' }),
                          /invalid JSON value for TOPICS/);
        });

        it('should not coerce a value of ambiguous type', () => {
            assert.strictEqual(apply({ SENTINELS: 'host1:26379,host2:26379' }).sentinels,
                               'host1:26379,host2:26379');
        });

        it('should ignore an empty value', () => {
            assert.deepStrictEqual(apply({ HOST: '' }), {});
        });

        it('should create the missing intermediate nodes', () => {
            const nested = joi.object({ mongo: joi.object({ auth: joi.object({ user: joi.string() }) }) });
            assert.deepStrictEqual(applyEnvOverrides({}, nested, [], { MONGO_AUTH_USER: 'u' }),
                                   { mongo: { auth: { user: 'u' } } });
        });

        it('should report a node holding something else than a section', () => {
            const nested = joi.object({ mongo: joi.object({ auth: joi.object({ user: joi.string() }) }) });
            assert.throws(
                () => applyEnvOverrides({ mongo: { auth: 'secret' } }, nested, [], { MONGO_AUTH_USER: 'u' }),
                /cannot set mongo.auth.user: mongo.auth is not an object/);
        });

        // one port cannot name the probe server of a specific site
        it('should not replace the per site probe servers with a single one', () => {
            const perSite = joi.object({
                queueProcessor: joi.object({
                    probeServer: joi.alternatives().try(probeServerJoi, probeServerPerSite),
                }),
            });
            const config = { queueProcessor: { probeServer: [{ port: 4043, site: 'a' }] } };

            assert.throws(
                () => applyEnvOverrides(config, perSite, [], { QUEUE_PROCESSOR_PROBE_SERVER_PORT: '8100' }),
                /queueProcessor.probeServer is not an object/);
            assert.deepStrictEqual(config.queueProcessor.probeServer, [{ port: 4043, site: 'a' }]);
        });

        // a partially set object is what setting a single field of one yields
        it('should leave the schema to complete a partially set object', () => {
            const section = joi.object({
                log: joi.object({
                    level: joi.string().default('info'),
                    dump: joi.string().default('error'),
                }),
            });

            assert.deepStrictEqual(
                joi.attempt(applyEnvOverrides({}, section, [], { LOG_LEVEL: 'warn' }), section),
                { log: { level: 'warn', dump: 'error' } });
        });
    });

    describe('field decoder', () => {
        // a field with a syntax of its own, spanning a second variable
        const schema = joi.object({
            servers: joi.array().items(joi.string()).meta({
                envDecodeHook: (value, env) => (value.startsWith('[') ? undefined
                    : [value, env.SERVERS_MORE].filter(more => more)),
            }),
        });
        const apply = env => applyEnvOverrides({}, schema, [], env);

        it('should build the value of the field', () => {
            assert.deepStrictEqual(apply({ SERVERS: 'a:8000' }).servers, ['a:8000']);
        });

        it('should read the companion variable from the environment', () => {
            assert.deepStrictEqual(apply({ SERVERS: 'a:8000', SERVERS_MORE: 'b:8000' }).servers,
                                   ['a:8000', 'b:8000']);
        });

        it('should coerce the value the decoder defers on', () => {
            assert.deepStrictEqual(apply({ SERVERS: '["a:8000"]' }).servers, ['a:8000']);
        });
    });

    describe('liveness probe port', () => {
        const schema = joi.object({
            queuePopulator: joi.object({ probeServer: probeServerJoi }),
            processor: joi.object({ probeServer: probeServerJoi }),
            queueProcessor: joi.object({
                probeServer: joi.alternatives().try(probeServerJoi, probeServerPerSite),
            }),
        });
        const apply = config =>
            applyEnvOverrides(config, schema, [], { LIVENESS_PROBE_PORT: '8100' });

        it('should listen on all interfaces, for every probe server of the process', () => {
            const config = apply({ queuePopulator: { probeServer: { port: 4042 } }, processor: {} });
            assert.deepStrictEqual(config, {
                queuePopulator: { probeServer: { bindAddress: '0.0.0.0', port: '8100' } },
                processor: { probeServer: { bindAddress: '0.0.0.0', port: '8100' } },
            });
        });

        it('should leave the sections missing from the config alone', () => {
            assert.deepStrictEqual(apply({}), {});
        });

        it('should leave per site probe servers alone', () => {
            const perSite = { queueProcessor: { probeServer: [{ port: 4043, site: 'a' }] } };
            assert.deepStrictEqual(apply(perSite),
                                   { queueProcessor: { probeServer: [{ port: 4043, site: 'a' }] } });
        });

        it('should be overriden by the port of a specific probe server', () => {
            const config = applyEnvOverrides({ processor: {} }, schema, [], {
                LIVENESS_PROBE_PORT: '8100',
                PROCESSOR_PROBE_SERVER_PORT: '8200',
            });
            assert.deepStrictEqual(config.processor.probeServer,
                                   { bindAddress: '0.0.0.0', port: '8200' });
        });
    });
});
