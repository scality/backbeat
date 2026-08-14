const joi = require('joi');
const { probeServerJoi } = require('../../lib/config/configItems.joi');
const { supportedSaslProtocols, supportedScramMechanisms } = require('./constants');

const { MAX_QUEUED_DEFAULT }  = require('../../lib/constants').backbeatConsumer;

const sslSchema = joi.object({
    ssl: joi.boolean().default(false),
    ca: joi.string(),
    client: joi.string(),
    key: joi.string(),
    keyPassword: joi.string(),
});

const saslAuthSchema = sslSchema.append({
    protocol: joi.string().valid(...supportedSaslProtocols).required(),
});

const kerberosAuthSchema = saslAuthSchema.append({
    type: joi.string().valid('kerberos').required(),
    keytab: joi.string().required(),
    principal: joi.string().required(),
    serviceName: joi.string().required(),
});

const basicAuthBaseSchema = saslAuthSchema.append({
    type: joi.string().valid('basic').required(),
});

const basicAuthSchema = joi.alternatives().try(
    basicAuthBaseSchema.append({
        credentialsFile: joi.string().required(),
    }),
    basicAuthBaseSchema.append({
        username: joi.string().required(),
        password: joi.string().required(),
    }),
);

const scramAuthBaseSchema = saslAuthSchema.append({
    type: joi.string().valid('scram').required(),
    mechanism: joi.string().valid(...supportedScramMechanisms).required(),
});

const scramAuthSchema = joi.alternatives().try(
    scramAuthBaseSchema.append({
        credentialsFile: joi.string().required(),
    }),
    scramAuthBaseSchema.append({
        username: joi.string().required(),
        password: joi.string().required(),
    }),
);

const credentialsFileSchema = joi.object({
    username: joi.string().required(),
    password: joi.string().required(),
});

const authSchema = joi.alternatives().try(sslSchema, kerberosAuthSchema, basicAuthSchema, scramAuthSchema).default({});

const destinationSchema = joi.object({
    resource: joi.string().required(),
    type: joi.string().required(),
    host: joi.string().required(),
    port: joi.number().optional(),
    internalTopic: joi.string(),
    topic: joi.string().required(),
    auth: authSchema,
    requiredAcks: joi.number().when('type', {
        is: joi.string().not('kafka'),
        then: joi.forbidden(),
        otherwise: joi.number().default(1),
    }),
    compressionType: joi.string().when('type', {
        is: joi.string().not('kafka'),
        then: joi.forbidden(),
        otherwise: joi.string().default('none'),
    }),
    // number of record keys the destination is spread over: raise it to let
    // more than one delivery worker handle the destination in parallel;
    // keys collide under the broker's crc32(key) % partitions, so m keys
    // reach at most m partitions and usually fewer: size well above the
    // parallelism wanted and verify against the observed partition map
    spreadFactor: joi.number().integer().min(1).default(1),
});

const joiSchema = joi.object({
    topic: joi.string(),
    monitorNotificationFailures: joi.boolean().default(true),
    notificationFailedTopic: joi.string().optional(),
    zookeeperPath: joi.string().optional(),
    queueProcessor: joi.object({
        groupId: joi.string().required(),
        concurrency: joi.number().greater(0).default(1000),
        maxQueued: joi.number().greater(0).default(MAX_QUEUED_DEFAULT),
    }),
    // single consumer group delivering to every destination, addressed by
    // the record itself instead of by one topic per destination.
    // deliveryTimeoutMs must stay above the producer request timeout (5000)
    // and below kafka.maxPollIntervalMs minus a margin, otherwise a slow
    // destination holds the partition past the poll deadline and the
    // consumer is evicted.
    deliveryPool: joi.object({
        enabled: joi.boolean().default(false),
        topic: joi.string().when('enabled', {
            is: joi.boolean().valid(true).required(),
            then: joi.required(),
        }),
        groupId: joi.string().when('enabled', {
            is: joi.boolean().valid(true).required(),
            then: joi.required(),
        }),
        deliveryTimeoutMs: joi.number().min(6000).max(240000).default(30000),
        producerIdleMs: joi.number().greater(0).default(300000),
        maxProducers: joi.number().greater(0).default(50),
        concurrency: joi.number().greater(0).default(1000),
        maxQueued: joi.number().greater(0).default(MAX_QUEUED_DEFAULT),
        probeServer: probeServerJoi.optional(),
    }).optional(),
    destinations: joi.array().items(destinationSchema).default([]),
    // TODO: BB-625 reset to being required after supporting probeserver in S3C
    // for bucket notification proceses
    probeServer: probeServerJoi.optional(),
    bucketMetastore: joi.string().default('__metastore'),
    maxCachedConfigs: joi.number().default(1000),
    // Conrrency to use when updating all local bucket notification configs
    // from zookeeper
    zookeeperOpConcurrency: joi.number().default(10),
});

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = {
    notificationConfigValidator: configValidator,
    authSchema,
    credentialsFileSchema,
};
