const joi = require('joi');
const { probeServerJoi } = require('../../lib/config/configItems.joi');

const { MAX_QUEUED_DEFAULT }  = require('../../lib/constants').backbeatConsumer;

const authSchema = joi.object({
    type: joi.string(),
    ssl: joi.boolean(),
    protocol: joi.string(),
    ca: joi.string(),
    client: joi.string(),
    key: joi.string(),
    keyPassword: joi.string(),
    keytab: joi.string(),
    principal: joi.string(),
    serviceName: joi.string(),
});

const destinationSchema = joi.object({
    resource: joi.string().required(),
    type: joi.string().required(),
    host: joi.string().required(),
    port: joi.number().optional(),
    internalTopic: joi.string(),
    topic: joi.string().required(),
    auth: authSchema.default({}),
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
});

const joiSchema = joi.object({
    topic: joi.string(),
    monitorNotificationFailures: joi.boolean().default(true),
    notificationFailedTopic: joi.string().optional(),
    zookeeperPath: joi.string().optional(),
    queueProcessor: {
        groupId: joi.string().required(),
        concurrency: joi.number().greater(0).default(1000),
        maxQueued: joi.number().greater(0).default(MAX_QUEUED_DEFAULT),
    },
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

module.exports = configValidator;
