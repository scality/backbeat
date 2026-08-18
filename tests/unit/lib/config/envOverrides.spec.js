'use strict';

const assert = require('assert');
const joi = require('joi');

const {
    applyCompositeEnvOverrides,
    applyEnvOverrides,
    envVarMappings,
} = require('../../../../lib/config/envOverrides');
const { backbeatConfigJoi } = require('../../../../lib/config.joi');
const {
    logJoi,
    probeServerJoi,
    probeServerPerSite,
} = require('../../../../lib/config/configItems.joi');
const { Config } = require('../../../../lib/Config');
const { getField } = require('../../../../lib/config/fields');

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

    it('should not derive a name for the params BackbeatProducer sets itself', () => {
        const names = [...envVarMappings(backbeatConfigJoi).keys()];

        assert.ok(!names.some(name => name.startsWith('KAFKA_PRODUCER_PARAMS')), names.join(', '));
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

        it('should apply the annotated names of the backbeat schema', () => {
            // renamed by env: the derived LOG_LOG_LEVEL is replaced
            assert.strictEqual(
                applyEnvOverrides({}, backbeatConfigJoi, [], { LOG_LEVEL: 'warn' }).log.logLevel,
                'warn');
            assert.deepStrictEqual(
                applyEnvOverrides({}, backbeatConfigJoi, [], { LOG_LOG_LEVEL: 'warn' }), {});

            // aliased: both names are honored
            ['HEALTHCHECKS_ALLOWFROM', 'SERVER_HEALTH_CHECKS_ALLOW_FROM'].forEach(name => {
                const config = applyEnvOverrides({}, backbeatConfigJoi, [], { [name]: '::1' });
                assert.deepStrictEqual(config.server.healthChecks.allowFrom, ['::1'], name);
            });
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
            const config = applyEnvOverrides({}, backbeatConfigJoi, [], { LOG_LEVEL: 'warn' });
            assert.deepStrictEqual(joi.attempt(config.log, logJoi),
                                   { logLevel: 'warn', dumpLevel: 'error' });
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

describe('composite config env vars', () => {
    const apply = env => applyCompositeEnvOverrides({ redis: { host: 'localhost', port: 6379 } }, env);

    it('should set the sentinels group name, and drop the standalone host', () => {
        assert.deepStrictEqual(apply({ REDIS_SENTINELS: 'host1:26379' }).redis,
                               { sentinels: 'host1:26379', name: 'mymaster' });
        assert.deepStrictEqual(apply({ REDIS_SENTINELS: 'host1:26379', REDIS_HA_NAME: 'group' }).redis,
                               { sentinels: 'host1:26379', name: 'group' });
    });

    it('should default the standalone redis port', () => {
        assert.deepStrictEqual(apply({ REDIS_HOST: 'redis' }).redis, { host: 'redis', port: '6379' });
        assert.deepStrictEqual(apply({ REDIS_HOST: 'redis', REDIS_PORT: '6380' }).redis,
                               { host: 'redis', port: '6380' });
        assert.deepStrictEqual(apply({ REDIS_PORT: '6380' }).redis,
                               { host: 'localhost', port: '6380' });
    });

    it('should ignore the standalone redis host and port when sentinels are set', () => {
        assert.deepStrictEqual(
            apply({ REDIS_SENTINELS: 'host1:26379', REDIS_HOST: 'redis', REDIS_PORT: '6380' }).redis,
            { sentinels: 'host1:26379', name: 'mymaster' });
    });

    it('should set the replica set hosts, and leave the log source alone', () => {
        const config = applyCompositeEnvOverrides({}, { MONGODB_HOSTS: 'mongo1:27017,mongo2:27017' });
        assert.deepStrictEqual(config.queuePopulator, {
            mongo: { replicaSetHosts: 'mongo1:27017,mongo2:27017' },
        });
    });

    it('should build the replication bootstrap list of a single site', () => {
        const config = applyCompositeEnvOverrides({ extensions: { replication: {} } }, {
            EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST: 'zenko-1:8000',
        });
        assert.deepStrictEqual(config.extensions.replication.destination.bootstrapList,
                               [{ site: 'zenko', servers: ['zenko-1:8000'] }]);
    });

    it('should split the servers of the replication bootstrap site', () => {
        const config = applyCompositeEnvOverrides({ extensions: { replication: {} } }, {
            EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST: 'zenko-1:8000, zenko-2:8000',
        });
        assert.deepStrictEqual(config.extensions.replication.destination.bootstrapList,
                               [{ site: 'zenko', servers: ['zenko-1:8000', 'zenko-2:8000'] }]);
    });

    it('should append the additional replication bootstrap sites', () => {
        const config = applyCompositeEnvOverrides({ extensions: { replication: {} } }, {
            EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST: 'zenko-1:8000',
            EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST_MORE: '{ "site": "aws", "type": "aws_s3" }',
        });
        assert.deepStrictEqual(config.extensions.replication.destination.bootstrapList, [
            { site: 'zenko', servers: ['zenko-1:8000'] },
            { site: 'aws', type: 'aws_s3' },
        ]);
    });

    it('should leave the bootstrap list alone when replication is not configured', () => {
        assert.deepStrictEqual(
            applyCompositeEnvOverrides({}, { EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST: 'zenko-1:8000' }),
            {});
    });
});

/**
 * The env var contract is consumed by zenko-operator and CI: every var the
 * docker entrypoint used to apply with jq must still set the same fields.
 */
describe('historic config env vars', () => {
    /**
     * Every name the entrypoint applied with jq, plus the two lib/Config.js
     * applied on its own. zenko-operator sets some of them, and Federation
     * forwards arbitrary ones through `env_backbeat_extraenv2`, so the whole
     * list has to keep working. Each name is checked below to be either
     * covered by a contract case, or explicitly removed.
     */
    const historicEnvVars = [
        'CLOUDSERVER_HOST',
        'CLOUDSERVER_PORT',
        'EXTENSIONS_GC_TOPIC',
        'EXTENSIONS_INGESTION_AUTH_ACCOUNT',
        'EXTENSIONS_INGESTION_AUTH_TYPE',
        'EXTENSIONS_INGESTION_MAX_PARALLEL_READERS',
        'EXTENSIONS_LIFECYCLE_AUTH_ACCOUNT',
        'EXTENSIONS_LIFECYCLE_AUTH_TYPE',
        'EXTENSIONS_LIFECYCLE_BUCKET_PROCESSOR_GROUP_ID',
        'EXTENSIONS_LIFECYCLE_BUCKET_TASK_TOPIC',
        'EXTENSIONS_LIFECYCLE_CONDUCTOR_CRONRULE',
        'EXTENSIONS_LIFECYCLE_OBJECT_PROCESSOR_GROUP_ID',
        'EXTENSIONS_LIFECYCLE_OBJECT_TASK_TOPIC',
        'EXTENSIONS_LIFECYCLE_RULES_ABORT_INCOMPLETE_MPU_ENABLED',
        'EXTENSIONS_LIFECYCLE_RULES_EXPIRATION_ENABLED',
        'EXTENSIONS_LIFECYCLE_RULES_NC_VERSION_EXPIRATION_ENABLED',
        'EXTENSIONS_LIFECYCLE_RULES_TRANSITIONS_ENABLED',
        'EXTENSIONS_LIFECYCLE_ZOOKEEPER_PATH',
        'EXTENSIONS_REPLICATION_DEST_AUTH_ACCOUNT',
        'EXTENSIONS_REPLICATION_DEST_AUTH_TYPE',
        'EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST',
        'EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST_MORE',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_BACKOFF_FACTOR',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_BACKOFF_JITTER',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_BACKOFF_MAX',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_BACKOFF_MIN',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_MAX_RETRIES',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_TIMEOUT_S',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_BACKOFF_FACTOR',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_BACKOFF_JITTER',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_BACKOFF_MAX',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_BACKOFF_MIN',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_MAX_RETRIES',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_TIMEOUT_S',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_CONCURRENCY',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_BACKOFF_FACTOR',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_BACKOFF_JITTER',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_BACKOFF_MAX',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_BACKOFF_MIN',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_MAX_RETRIES',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_TIMEOUT_S',
        'EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_MAX_POLL_INTERVAL_MS',
        'EXTENSIONS_REPLICATION_SOURCE_AUTH_ACCOUNT',
        'EXTENSIONS_REPLICATION_SOURCE_AUTH_TYPE',
        'EXTENSIONS_REPLICATION_SOURCE_S3_HOST',
        'EXTENSIONS_REPLICATION_SOURCE_S3_PORT',
        'EXTENSIONS_REPLICATION_STATUS_PROCESSOR_CONCURRENCY',
        'HEALTHCHECKS_ALLOWFROM',
        'KAFKA_BACKLOG_METRICS_INTERVALS',
        'KAFKA_BACKLOG_METRICS_ZKPATH',
        'KAFKA_HOSTS',
        'LIVENESS_PROBE_PORT',
        'LOG_LEVEL',
        'MONGODB_AUTH_PASSWORD',
        'MONGODB_AUTH_USERNAME',
        'MONGODB_DATABASE',
        'MONGODB_HOSTS',
        'MONGODB_RS',
        'QUEUE_POPULATOR_BATCH_MAX_READ',
        'QUEUE_POPULATOR_DMD_HOST',
        'QUEUE_POPULATOR_DMD_PORT',
        'REDIS_HA_NAME',
        'REDIS_HOST',
        'REDIS_LOCALCACHE_HOST',
        'REDIS_LOCALCACHE_PORT',
        'REDIS_PORT',
        'REDIS_SENTINELS',
        'REPLICATION_GROUP_ID',
        'ZOOKEEPER_AUTO_CREATE_NAMESPACE',
        'ZOOKEEPER_CONNECTION_STRING',
    ];

    // the lifecycle rules are configured with supportedLifecycleRules, and the
    // local cache is not part of the configuration schema
    const removedEnvVars = [
        'EXTENSIONS_LIFECYCLE_RULES_ABORT_INCOMPLETE_MPU_ENABLED',
        'EXTENSIONS_LIFECYCLE_RULES_EXPIRATION_ENABLED',
        'EXTENSIONS_LIFECYCLE_RULES_NC_VERSION_EXPIRATION_ENABLED',
        'EXTENSIONS_LIFECYCLE_RULES_TRANSITIONS_ENABLED',
        'REDIS_LOCALCACHE_HOST',
        'REDIS_LOCALCACHE_PORT',
    ];

    const retryFields = backend => ({
        [`EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_${backend}_RETRY_MAX_RETRIES`]: '1',
        [`EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_${backend}_RETRY_TIMEOUT_S`]: '2',
        [`EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_${backend}_RETRY_BACKOFF_MIN`]: '3',
        [`EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_${backend}_RETRY_BACKOFF_MAX`]: '4',
        [`EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_${backend}_RETRY_BACKOFF_JITTER`]: '0.5',
        [`EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_${backend}_RETRY_BACKOFF_FACTOR`]: '6',
    });
    const retryExpected = backend => ({
        [`extensions.replication.queueProcessor.retry.${backend}.maxRetries`]: 1,
        [`extensions.replication.queueProcessor.retry.${backend}.timeoutS`]: 2,
        [`extensions.replication.queueProcessor.retry.${backend}.backoff`]:
            { min: 3, max: 4, jitter: 0.5, factor: 6 },
    });

    const probeServer = { bindAddress: '0.0.0.0', port: 8100 };
    const contract = [
        [{ LIVENESS_PROBE_PORT: '8100' }, {
            'queuePopulator.probeServer': probeServer,
            'extensions.ingestion.probeServer': probeServer,
            'extensions.mongoProcessor.probeServer': probeServer,
            'extensions.replication.queueProcessor.probeServer': probeServer,
            'extensions.replication.replicationStatusProcessor.probeServer': probeServer,
            'extensions.lifecycle.conductor.probeServer': probeServer,
            'extensions.lifecycle.bucketProcessor.probeServer': probeServer,
            'extensions.lifecycle.objectProcessor.probeServer': probeServer,
            'extensions.gc.probeServer': probeServer,
        }],
        [{ LOG_LEVEL: 'debug' }, { 'log.logLevel': 'debug' }],
        [{ ZOOKEEPER_AUTO_CREATE_NAMESPACE: 'true' }, { 'zookeeper.autoCreateNamespace': true }],
        [{ ZOOKEEPER_CONNECTION_STRING: 'zk:2181/bb' }, { 'zookeeper.connectionString': 'zk:2181/bb' }],
        [{ KAFKA_HOSTS: 'kafka:9092' }, { 'kafka.hosts': 'kafka:9092' }],
        [{ KAFKA_BACKLOG_METRICS_ZKPATH: '/bb/metrics' }, { 'kafka.backlogMetrics.zkPath': '/bb/metrics' }],
        [{ KAFKA_BACKLOG_METRICS_INTERVALS: '30' }, { 'kafka.backlogMetrics.intervalS': 30 }],
        [{ REDIS_SENTINELS: 'sentinel1:26379,sentinel2:26379', REDIS_HA_NAME: 'group' }, {
            redis: {
                name: 'group',
                sentinels: [{ host: 'sentinel1', port: 26379 }, { host: 'sentinel2', port: 26379 }],
            },
        }],
        [{ REDIS_HOST: 'redis' }, { 'redis.host': 'redis', 'redis.port': 6379 }],
        [{ REDIS_HOST: 'redis', REDIS_PORT: '6380' }, { 'redis.host': 'redis', 'redis.port': 6380 }],
        [{ QUEUE_POPULATOR_BATCH_MAX_READ: '42' }, { 'queuePopulator.batchMaxRead': 42 }],
        [{ QUEUE_POPULATOR_DMD_HOST: 'dmd', QUEUE_POPULATOR_DMD_PORT: '9991' }, {
            'queuePopulator.dmd.host': 'dmd',
            'queuePopulator.dmd.port': 9991,
        }],
        [{ MONGODB_HOSTS: 'mongo1:27017,mongo2:27017' }, {
            'queuePopulator.mongo.replicaSetHosts': 'mongo1:27017,mongo2:27017',
        }],
        [{ MONGODB_RS: 'rs1' }, { 'queuePopulator.mongo.replicaSet': 'rs1' }],
        [{ MONGODB_DATABASE: 'db' }, { 'queuePopulator.mongo.database': 'db' }],
        [{ MONGODB_AUTH_USERNAME: 'user', MONGODB_AUTH_PASSWORD: 'pass' }, {
            'queuePopulator.mongo.authCredentials': { username: 'user', password: 'pass' },
        }],
        [{ CLOUDSERVER_HOST: 'cloudserver', CLOUDSERVER_PORT: '8001' }, {
            's3.host': 'cloudserver',
            's3.port': 8001,
        }],
        [{ HEALTHCHECKS_ALLOWFROM: '10.0.0.0/8' }, {
            // the loopback addresses are always allowed
            'server.healthChecks.allowFrom': ['10.0.0.0/8', '127.0.0.1/8', '::1'],
        }],
        [{ REPLICATION_GROUP_ID: 'RG00002' }, { replicationGroupId: 'RG00002' }],
        [{
            EXTENSIONS_REPLICATION_SOURCE_S3_HOST: 'cloudserver',
            EXTENSIONS_REPLICATION_SOURCE_S3_PORT: '8001',
        }, {
            'extensions.replication.source.s3.host': 'cloudserver',
            'extensions.replication.source.s3.port': 8001,
        }],
        [{
            EXTENSIONS_REPLICATION_SOURCE_AUTH_TYPE: 'account',
            EXTENSIONS_REPLICATION_SOURCE_AUTH_ACCOUNT: 'source-account',
        }, {
            'extensions.replication.source.auth.type': 'account',
            'extensions.replication.source.auth.account': 'source-account',
        }],
        [{
            EXTENSIONS_REPLICATION_DEST_AUTH_TYPE: 'account',
            EXTENSIONS_REPLICATION_DEST_AUTH_ACCOUNT: 'dest-account',
        }, {
            'extensions.replication.destination.auth.type': 'account',
            'extensions.replication.destination.auth.account': 'dest-account',
        }],
        [{ EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST: 'zenko-1:8000' }, {
            'extensions.replication.destination.bootstrapList':
                [{ site: 'zenko', servers: ['zenko-1:8000'], echo: false }],
        }],
        [{
            EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST: 'zenko-1:8000',
            EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST_MORE: '{ "site": "aws", "type": "aws_s3" }',
        }, {
            'extensions.replication.destination.bootstrapList': [
                { site: 'zenko', servers: ['zenko-1:8000'], echo: false },
                { site: 'aws', type: 'aws_s3' },
            ],
        }],
        [retryFields('AWS_S3'), retryExpected('aws_s3')],
        [retryFields('AZURE'), retryExpected('azure')],
        [retryFields('GCP'), retryExpected('gcp')],
        [{ EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_CONCURRENCY: '11' },
            { 'extensions.replication.queueProcessor.concurrency': 11 }],
        [{ EXTENSIONS_REPLICATION_STATUS_PROCESSOR_CONCURRENCY: '7' },
            { 'extensions.replication.replicationStatusProcessor.concurrency': 7 }],
        [{ EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_MAX_POLL_INTERVAL_MS: '60000' },
            { 'extensions.replication.queueProcessor.maxPollIntervalMs': 60000 }],
        [{ EXTENSIONS_LIFECYCLE_ZOOKEEPER_PATH: '/lc' }, { 'extensions.lifecycle.zookeeperPath': '/lc' }],
        [{ EXTENSIONS_LIFECYCLE_BUCKET_TASK_TOPIC: 'lc-buckets' },
            { 'extensions.lifecycle.bucketTasksTopic': 'lc-buckets' }],
        [{ EXTENSIONS_LIFECYCLE_OBJECT_TASK_TOPIC: 'lc-objects' },
            { 'extensions.lifecycle.objectTasksTopic': 'lc-objects' }],
        [{ EXTENSIONS_LIFECYCLE_CONDUCTOR_CRONRULE: '0 0 * * * *' },
            { 'extensions.lifecycle.conductor.cronRule': '0 0 * * * *' }],
        [{ EXTENSIONS_LIFECYCLE_BUCKET_PROCESSOR_GROUP_ID: 'lc-bucket-group' },
            { 'extensions.lifecycle.bucketProcessor.groupId': 'lc-bucket-group' }],
        [{ EXTENSIONS_LIFECYCLE_OBJECT_PROCESSOR_GROUP_ID: 'lc-object-group' },
            { 'extensions.lifecycle.objectProcessor.groupId': 'lc-object-group' }],
        [{ EXTENSIONS_LIFECYCLE_AUTH_TYPE: 'account', EXTENSIONS_LIFECYCLE_AUTH_ACCOUNT: 'lc-account' }, {
            'extensions.lifecycle.auth.type': 'account',
            'extensions.lifecycle.auth.account': 'lc-account',
        }],
        [{ EXTENSIONS_GC_TOPIC: 'gc-topic' }, { 'extensions.gc.topic': 'gc-topic' }],
        [{ EXTENSIONS_INGESTION_AUTH_TYPE: 'service', EXTENSIONS_INGESTION_AUTH_ACCOUNT: 'ingest' }, {
            'extensions.ingestion.auth.type': 'service',
            'extensions.ingestion.auth.account': 'ingest',
        }],
        [{ EXTENSIONS_INGESTION_MAX_PARALLEL_READERS: '3' },
            { 'extensions.ingestion.maxParallelReaders': 3 }],
    ];

    let ogConfigFile;

    before(() => {
        ogConfigFile = process.env.BACKBEAT_CONFIG_FILE;
        process.env.BACKBEAT_CONFIG_FILE = `${__dirname}/config.json`;
    });

    after(() => {
        if (ogConfigFile === undefined) {
            delete process.env.BACKBEAT_CONFIG_FILE;
        } else {
            process.env.BACKBEAT_CONFIG_FILE = ogConfigFile;
        }
    });

    function configWith(env) {
        const og = Object.fromEntries(Object.keys(env).map(name => [name, process.env[name]]));
        Object.assign(process.env, env);
        try {
            return new Config();
        } finally {
            Object.entries(og).forEach(([name, value]) => {
                if (value === undefined) {
                    delete process.env[name];
                } else {
                    process.env[name] = value;
                }
            });
        }
    }

    contract.forEach(([env, expected]) => {
        it(`should apply ${Object.keys(env).join(', ')}`, () => {
            const config = configWith(env);
            Object.entries(expected).forEach(([path, value]) =>
                assert.deepStrictEqual(getField(config, path.split('.')), value, path));
        });
    });

    it('should account for every historic env var', () => {
        const covered = new Set([
            ...contract.flatMap(([env]) => Object.keys(env)),
            ...removedEnvVars,
        ]);

        assert.deepStrictEqual(historicEnvVars.filter(name => !covered.has(name)), []);
    });

    it('should accept the log levels the entrypoint used to reject', () => {
        assert.strictEqual(configWith({ LOG_LEVEL: 'warn' }).log.logLevel, 'warn');
        assert.strictEqual(configWith({ LOG_LEVEL: 'error' }).log.logLevel, 'error');
    });

    it('should ignore the vars removed with the entrypoint', () => {
        const config = configWith(Object.fromEntries(removedEnvVars.map(name => [name, 'redis'])));

        assert.strictEqual(config.extensions.lifecycle.rules, undefined);
        assert.strictEqual(config.localCache, undefined);
    });
});
