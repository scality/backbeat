'use strict';

const assert = require('assert');
const fs = require('fs');
const sinon = require('sinon');

const { Config } = require('../../../../lib/Config');
const { getField } = require('../../../../lib/config/fields');
const backbeatConfig = require('./config.json');

const CONFIG_OVERRIDES = 'BACKBEAT_CONFIG_OVERRIDES';

describe('Config', () => {
    let config;
    let testConfig;

    beforeEach(() => {
        config = new Config();
        // deep copy the config to avoid modifying the original
        testConfig = JSON.parse(JSON.stringify(backbeatConfig));
    });

    it('should make the probeserver config in the queuePoulator' +
        'required when multiple extensions are configured', () => {
        delete testConfig.queuePopulator.probeServer;
        assert.throws(() => config._parseConfig(testConfig));
    });

    it('should make the probeserver config in the queuePoulator' +
        'optional when only notification config is specified', () => {
        delete testConfig.queuePopulator.probeServer;
        testConfig.extensions = { notification: testConfig.extensions.notification };
        assert.doesNotThrow(() => config._parseConfig(testConfig));
    });

    it('should throw an error when dataMoverTopic is not provided and transition is supported', () => {
        delete testConfig.extensions.replication.dataMoverTopic;
        testConfig.extensions.lifecycle.supportedLifecycleRules = [
            'Transition',
            'NoncurrentVersionTransition',
            'Expiration',
            'NoncurrentVersionExpiration',
            'AbortIncompleteMultipartUpload',
        ];
        assert.throws(() => config._parseConfig(testConfig));
    });

    it('should make dataMoverTopic optional when transitions are not supported', () => {
        delete testConfig.extensions.replication.dataMoverTopic;
        testConfig.extensions.lifecycle.supportedLifecycleRules = [
            'Expiration',
            'NoncurrentVersionExpiration',
            'AbortIncompleteMultipartUpload',
        ];
        assert.doesNotThrow(() => config._parseConfig(testConfig));
    });

    describe('configuration overrides', () => {
        afterEach(() => {
            delete process.env[CONFIG_OVERRIDES];
            delete process.env.BACKBEAT_CONFIG_FILE;
            delete process.env.KAFKA_HOSTS;
            delete process.env.EXTENSIONS_GC_TOPIC;
            delete process.env.MONGODB_DATABASE;
            delete process.env.MONGODB_HOSTS;
        });

        it('should win over an env var of the global config', () => {
            process.env.KAFKA_HOSTS = 'from-env:9092';
            process.env[CONFIG_OVERRIDES] = '{"kafka":{"hosts":"from-override:9092"}}';
            config._parseConfig(testConfig);
            assert.strictEqual(config.kafka.hosts, 'from-override:9092');
        });

        it('should win over an env var of an extension', () => {
            process.env.EXTENSIONS_GC_TOPIC = 'from-env-gc';
            process.env[CONFIG_OVERRIDES] = '{"extensions":{"gc":{"topic":"from-override-gc"}}}';
            config._parseConfig(testConfig);
            assert.strictEqual(config.extensions.gc.topic, 'from-override-gc');
        });

        it('should win over an env var setting several fields at once', () => {
            process.env.MONGODB_HOSTS = 'from-env:27017';
            process.env[CONFIG_OVERRIDES] =
                '{"queuePopulator":{"mongo":{"replicaSetHosts":"from-override:27017"}}}';
            config._parseConfig(testConfig);
            assert.strictEqual(config.queuePopulator.mongo.replicaSetHosts,
                               'from-override:27017');
        });

        it('should override a value of the config file', () => {
            process.env[CONFIG_OVERRIDES] = '{"kafka":{"hosts":"patched:9092"}}';
            config._parseConfig(testConfig);
            assert.strictEqual(config.kafka.hosts, 'patched:9092');
        });

        it('should leave the fields it does not mention alone', () => {
            process.env[CONFIG_OVERRIDES] = '{"kafka":{"hosts":"patched:9092"}}';
            config._parseConfig(testConfig);
            assert.strictEqual(config.kafka.maxRequestSize,
                               backbeatConfig.kafka.maxRequestSize);
            assert.strictEqual(config.server.port, backbeatConfig.server.port);
        });

        it('should override a field of an extension', () => {
            process.env[CONFIG_OVERRIDES] = '{"extensions":{"gc":{"topic":"patched-gc"}}}';
            config._parseConfig(testConfig);
            assert.strictEqual(config.extensions.gc.topic, 'patched-gc');
            // the rest of the extension config is untouched
            assert.strictEqual(config.extensions.gc.consumer.concurrency,
                               backbeatConfig.extensions.gc.consumer.concurrency);
        });

        it('should set a field the config file does not define', () => {
            process.env[CONFIG_OVERRIDES] =
                '{"kafka":{"producerParams":{"linger.ms":10,"socket.timeout.ms":5000}}}';
            config._parseConfig(testConfig);
            assert.deepStrictEqual(config.kafka.producerParams,
                                   { 'linger.ms': 10, 'socket.timeout.ms': 5000 });
        });

        it('should replace an array rather than merge it', () => {
            process.env[CONFIG_OVERRIDES] =
                '{"server":{"healthChecks":{"allowFrom":["10.0.0.0/8"]}}}';
            config._parseConfig(testConfig);
            // _parseConfig appends the default health checks to the configured ones
            assert.deepStrictEqual(config.server.healthChecks.allowFrom,
                                   ['10.0.0.0/8', '127.0.0.1/8', '::1']);
        });

        it('should restore the schema default when a field is deleted', () => {
            // a value the schema default differs from, so that the assertion
            // tells the field was deleted from the value it held
            testConfig.kafka.backlogMetrics.intervalS = 120;
            process.env[CONFIG_OVERRIDES] = '{"kafka":{"backlogMetrics":{"intervalS":null}}}';
            config._parseConfig(testConfig);
            // the joi default of the field, not the value of the config file
            assert.strictEqual(config.kafka.backlogMetrics.intervalS, 60);
        });

        it('should coerce the types joi converts', () => {
            process.env[CONFIG_OVERRIDES] = '{"queuePopulator":{"batchMaxRead":"250"}}';
            config._parseConfig(testConfig);
            assert.strictEqual(config.queuePopulator.batchMaxRead, 250);
        });

        it('should validate the merged config, rejecting a wrong type', () => {
            process.env[CONFIG_OVERRIDES] = '{"server":{"port":"not-a-number"}}';
            assert.throws(() => config._parseConfig(testConfig), /port/);
        });

        it('should validate the merged config, rejecting an unknown field', () => {
            process.env[CONFIG_OVERRIDES] = '{"kafka":{"notAKafkaSetting":1}}';
            assert.throws(() => config._parseConfig(testConfig), /notAKafkaSetting/);
        });

        it('should validate the merged config, rejecting a deleted required field', () => {
            process.env[CONFIG_OVERRIDES] = '{"kafka":{"hosts":null}}';
            assert.throws(() => config._parseConfig(testConfig), /hosts/);
        });

        it('should reject an invalid overrides document', () => {
            process.env[CONFIG_OVERRIDES] = '{oops';
            assert.throws(() => config._parseConfig(testConfig),
                          /invalid JSON value for BACKBEAT_CONFIG_OVERRIDES/);
        });

        it('should apply the overrides when loading the configuration file', () => {
            process.env.BACKBEAT_CONFIG_FILE = require.resolve('./config.json');
            process.env[CONFIG_OVERRIDES] = '{"kafka":{"hosts":"patched:9092"}}';
            const loaded = new Config();
            assert.strictEqual(loaded.kafka.hosts, 'patched:9092');
        });
    });
});

describe('backbeat config singleton', () => {
    it('should parse the configuration file at require time', () => {
        assert.notStrictEqual(require('../../../../lib/Config'), undefined);
    });
});

describe('Site name', () => {
    let conf;

    beforeEach(() => {
        conf = new Config();
    });

    afterEach(() => {
        delete process.env.BOOTSTRAP_SITE_NAME;
    });

    it('should filter bootstrapList based on SITE_NAME', () => {
        process.env.BOOTSTRAP_SITE_NAME = 'test-site-2';
        const expectedBootstrapList = conf.bootstrapList.filter(item => item.site === 'test-site-2');
        const newConfig = new Config();
        assert.deepStrictEqual(newConfig.bootstrapList, expectedBootstrapList);
    });

    it('should not filter bootstrapList if SITE_NAME is not set', () => {
        const expectedBootstrapList = conf.bootstrapList;
        const newConfig = new Config();
        assert.deepStrictEqual(newConfig.bootstrapList, expectedBootstrapList);
    });
});


describe('Config', () => {
    describe('getReplicationSiteDestConfig', () => {
        let ogConfigFileEnv;

        before(() => {
            ogConfigFileEnv = process.env.BACKBEAT_CONFIG_FILE;
        });

        afterEach(() => sinon.restore());

        after(() => {
            if (ogConfigFileEnv) {
                process.env.BACKBEAT_CONFIG_FILE = ogConfigFileEnv;
            }
        });

        describe('bootstrapList server normalization', () => {
            let conf;

            before(() => {
                process.env.BACKBEAT_CONFIG_FILE = `${__dirname}/replicationServersConfig.json`;
                conf = new Config();
            });

            it('should normalize server entries with default port 443 for https transport', () => {
                const entry = conf.bootstrapList.find(e => e.site === 'https-site');
                assert.deepStrictEqual(entry.servers, ['s3.example.com:443']);
            });

            it('should normalize server entries with default port 80 for http transport', () => {
                const entry = conf.bootstrapList.find(e => e.site === 'http-site');
                assert.deepStrictEqual(entry.servers, ['s3.example.com:80']);
            });

            it('should preserve explicit port in server entries', () => {
                const entry = conf.bootstrapList.find(e => e.site === 'explicit-port-site');
                assert.deepStrictEqual(entry.servers, ['s3.example.com:8443']);
            });

            it('should not modify endpoint without servers array', () => {
                const entry = conf.bootstrapList.find(e => e.site === 'aws-site');
                assert.strictEqual(entry.servers, undefined);
                assert.strictEqual(entry.type, 'aws_s3');
            });
        });


        describe('getReplicationSiteDestConfig', () => {
            it('should return replication site destination config', () => {
                process.env.BACKBEAT_CONFIG_FILE = `${__dirname}/replicationMultiDestConfig.json`;
                const conf = new Config();
                const destConfig = conf.getReplicationSiteDestConfig('aws3');
                assert.deepStrictEqual(destConfig, {
                    transport: 'https',
                    auth: {
                        type: 'service',
                        account: 'service-replication-3',
                    },
                    replicationEndpoint: {
                        site: 'aws3',
                        type: 'aws_s3',
                    },
                });
            });

            it('should return default replication destination config when site one is not available', () => {
                process.env.BACKBEAT_CONFIG_FILE = `${__dirname}/replicationMultiDestConfig.json`;
                const conf = new Config();
                sinon.stub(conf.extensions.replication, 'destination').value({
                    transport: 'https',
                    auth: {
                        type: 'service',
                        account: 'service-replication',
                    },
                    bootstrapList: [
                        { site: 'aws1', type: 'aws_s3' },
                        { site: 'aws2', type: 'aws_s3' },
                        { site: 'aws3', type: 'aws_s3' }
                    ]
                });
                const destConfig = conf.getReplicationSiteDestConfig('aws3');
                assert.deepStrictEqual(destConfig, {
                    transport: 'https',
                    auth: {
                        type: 'service',
                        account: 'service-replication',
                    },
                    replicationEndpoint: {
                        site: 'aws3',
                        type: 'aws_s3',
                    },
                });
            });
        });
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
                // the passwords of the section survive the parsing of the list
                password: '',
                sentinelPassword: '',
            },
        }, { redis: {} }],
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

    /**
     * @param {Object} env - env vars of the case
     * @param {Object} [sections] - config sections replacing those of the file,
     *   for a case the fixture cannot host as it stands
     * @returns {Config} configuration built from the file and the environment
     */
    function configWith(env, sections) {
        const og = Object.fromEntries(Object.keys(env).map(name => [name, process.env[name]]));
        Object.assign(process.env, env);
        if (sections) {
            sinon.stub(fs, 'readFileSync')
                .callThrough()
                .withArgs(process.env.BACKBEAT_CONFIG_FILE, sinon.match.any)
                .returns(JSON.stringify({ ...backbeatConfig, ...sections }));
        }
        try {
            return new Config();
        } finally {
            sinon.restore();
            Object.entries(og).forEach(([name, value]) => {
                if (value === undefined) {
                    delete process.env[name];
                } else {
                    process.env[name] = value;
                }
            });
        }
    }

    contract.forEach(([env, expected, sections]) => {
        it(`should apply ${Object.keys(env).join(', ')}`, () => {
            const config = configWith(env, sections);
            Object.entries(expected).forEach(([path, value]) =>
                assert.deepStrictEqual(getField(config, path.split('.')), value, path));
        });
    });

    it('should reject the sentinels over a standalone configuration', () => {
        assert.throws(() => configWith({ REDIS_SENTINELS: 'sentinel1:26379' }),
                      /"redis.host" is not allowed/);
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
