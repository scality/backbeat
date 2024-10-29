'use strict'; // eslint-disable-line

const joi = require('joi');
const {
    certFilePathsJoi,
    hostPortJoi,
    logJoi,
    mongoJoi,
    qpKafkaJoi,
    transportJoi,
    authJoi,
    probeServerJoi,
} = require('./config/configItems.joi');

const KAFKA_PRODUCER_MESSAGE_MAX_BYTES = 5000020;
const KAFKA_PRODUCER_DEFAULT_COMPRESSION_TYPE = 'Zstd';
const KAFKA_PRODUCER_DEFAULT_REQUIRED_ACKS = 'all';
const logSourcesJoi = joi.string().valid('bucketd', 'mongo', 'ingestion',
    'dmd', 'kafka');

const joiSchema = joi.object({
    replicationGroupId: joi.string().length(7).required(),
    zookeeper: {
        connectionString: joi.string().required(),
        autoCreateNamespace: joi.boolean().default(false),
    },
    kafka: {
        hosts: joi.string().required(),
        backlogMetrics: {
            zkPath: joi.string().default('/backbeat/run/kafka-backlog-metrics'),
            intervalS: joi.number().default(60),
        },
        maxRequestSize: joi.number().default(KAFKA_PRODUCER_MESSAGE_MAX_BYTES),
        site: joi.string(),
        compressionType: joi.string().default(KAFKA_PRODUCER_DEFAULT_COMPRESSION_TYPE),
        requiredAcks: joi.number().default(KAFKA_PRODUCER_DEFAULT_REQUIRED_ACKS),
    },
    transport: transportJoi,
    s3: hostPortJoi.required(),
    vaultAdmin: hostPortJoi,
    queuePopulator: {
        auth: authJoi,
        cronRule: joi.string().required(),
        batchMaxRead: joi.number().default(10000),
        batchTimeoutMs: joi.number().default(9000),
        zookeeperPath: joi.string().required(),

        logSource: joi.alternatives().try(logSourcesJoi).required(),
        exhaustLogSource: joi.bool().default(false),
        bucketd: hostPortJoi
            .keys({ transport: transportJoi })
            .when('logSource', { is: 'bucketd', then: joi.required() }),
        dmd: hostPortJoi.keys({
            logName: joi.string().default('s3-recordlog'),
        }).when('logSource', { is: 'dmd', then: joi.required() }),
        mongo: mongoJoi,
        kafka: qpKafkaJoi.when('logSource', { is: 'kafka', then: joi.required() }),
        probeServer: probeServerJoi.default(),
        circuitBreaker: joi.object().optional(),
    },
    log: logJoi,
    extensions: joi.object(),
    metrics: {
        topic: joi.string().required(),
        groupIdPrefix: joi.string().default('backbeat-metrics-group'),
    },
    server: {
        healthChecks: joi.object({
            allowFrom: joi.array().items(joi.string()).default([]),
        }).required(),
        host: joi.string().required(),
        port: joi.number().default(8900),
    },
    redis: {
        host: joi.string().when('sentinels', {
            is: joi.exist(),
            then: joi.forbidden(),
            otherwise: joi.required(),
        }),
        port: joi.number().when('sentinels', {
            is: joi.exist(),
            then: joi.forbidden(),
            otherwise: joi.required(),
        }),
        name: joi.string().default('backbeat'),
        password: joi.string().default('').allow(''),
        sentinels: joi.alternatives([joi.string(), joi.array().items(
            joi.object({
                host: joi.string().required(),
                port: joi.number().required(),
            }))]
        ),
        sentinelPassword: joi.string().default('').allow(''),
    },
    certFilePaths: certFilePathsJoi,
    internalCertFilePaths: certFilePathsJoi,
});

module.exports = {
    backbeatConfigJoi: joiSchema,
    KAFKA_PRODUCER_MESSAGE_MAX_BYTES,
    KAFKA_PRODUCER_DEFAULT_COMPRESSION_TYPE,
    KAFKA_PRODUCER_DEFAULT_REQUIRED_ACKS,
};
