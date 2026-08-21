'use strict';

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
const KAFKA_PRODUCER_DEFAULT_REQUIRED_ACKS = -1; // all brokers
const KAFKA_PRODUCER_PARAMS_SCHEMA = joi.object({
    'metadata.broker.list': joi.forbidden(),
    'dr_cb': joi.forbidden(),
    'message.max.bytes': joi.forbidden(),
    'compression.type': joi.forbidden(),
    'statistics.interval.ms': joi.forbidden(),
}).unknown(true).default({});
const KAFKA_CONSUMER_PARAMS_SCHEMA = joi.object({
    'metadata.broker.list': joi.forbidden(),
    'group.id': joi.forbidden(),
    'partition.assignment.strategy': joi.forbidden(),
    'enable.auto.offset.store': joi.forbidden(),
    'offset_commit_cb': joi.forbidden(),
    'allow.auto.create.topics': joi.forbidden(),
    'statistics.interval.ms': joi.forbidden(),
    'rebalance_cb': joi.forbidden(),
    'metadata.max.age.ms': joi.forbidden(),
    'max.poll.interval.ms': joi.forbidden(),
    'fetch.message.max.bytes': joi.forbidden(),
    'client.rack': joi.forbidden(),
    'debug': joi.forbidden(),
    'auto.offset.reset': joi.forbidden(),
    'enable.auto.commit': joi.forbidden(),
}).unknown(true).default({});
const logSourcesJoi = joi.string().valid('bucketd', 'ingestion', 'dmd', 'kafka');

const joiSchema = joi.object({
    replicationGroupId: joi.string().length(7).default('RG00001'),
    zookeeper: {
        connectionString: joi.string().required(),
        autoCreateNamespace: joi.boolean().default(false),
        retries: joi.number().default(3),
    },
    kafka: {
        hosts: joi.string().required(),
        backlogMetrics: {
            zkPath: joi.string().default('/backbeat/run/kafka-backlog-metrics')
                .meta({ env: 'ZKPATH' }),
            intervalS: joi.number().default(60)
                .meta({ env: 'INTERVALS' }),
        },
        maxRequestSize: joi.number().default(KAFKA_PRODUCER_MESSAGE_MAX_BYTES),
        site: joi.string(),
        compressionType: joi.string().default(KAFKA_PRODUCER_DEFAULT_COMPRESSION_TYPE),
        requiredAcks: joi.number().default(KAFKA_PRODUCER_DEFAULT_REQUIRED_ACKS),
        producerParams: KAFKA_PRODUCER_PARAMS_SCHEMA,
        consumerParams: KAFKA_CONSUMER_PARAMS_SCHEMA,
    },
    transport: transportJoi,
    s3: hostPortJoi.meta({ env: 'CLOUDSERVER' }).optional(),
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
        mongo: mongoJoi.meta({ envVarAlias: 'MONGODB' }),
        kafka: qpKafkaJoi.when('logSource', { is: 'kafka', then: joi.required() }),
        // TODO: BB-625 reset to being required after supporting probeserver in S3C
        // for bucket notification proceses
        probeServer: probeServerJoi.when('...extensions', {
            is: joi.object().keys({ notification: joi.exist() }),
            then: joi.optional(),
            otherwise: joi.required(),
        }),
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
            allowFrom: joi.array().items(joi.string()).default([])
                .meta({ envVarAlias: 'HEALTHCHECKS_ALLOWFROM' }),
        }).required(),
        host: joi.string().required(),
        port: joi.number().default(8900),
    },
    redis: joi.alternatives().conditional(joi.ref('.sentinels'), {
        is: joi.exist(),
        then: joi.object({
            sentinels: joi.alternatives([
                // the comma separated form is parsed here, so that the validated
                // configuration holds the list the redis client expects
                joi.string().custom(value => value.split(',').map(sentinel => {
                    const [host, port] = sentinel.split(':');
                    return { host, port: Number.parseInt(port, 10) };
                })),
                joi.array().items(joi.object({
                    host: joi.string().required(),
                    port: joi.number().required(),
                })),
            ]).required(),
            name: joi.string().default('mymaster').meta({ env: 'HA_NAME' }), // sentinel master group
            password: joi.string().default('').allow(''),
            sentinelPassword: joi.string().default('').allow(''),
        }),
        otherwise: joi.object({
            host: joi.string().required(),
            port: joi.number().default(6379),
            password: joi.string().default('').allow(''),
        }),
    }).default({ host: '127.0.0.1', port: 6379 }),
    certFilePaths: certFilePathsJoi,
    internalCertFilePaths: certFilePathsJoi,
});

module.exports = {
    backbeatConfigJoi: joiSchema,
    KAFKA_PRODUCER_MESSAGE_MAX_BYTES,
    KAFKA_PRODUCER_DEFAULT_COMPRESSION_TYPE,
    KAFKA_PRODUCER_DEFAULT_REQUIRED_ACKS,
    KAFKA_PRODUCER_PARAMS_SCHEMA,
    KAFKA_CONSUMER_PARAMS_SCHEMA,
};
