const joi = require('joi');
const { probeServerJoi } = require('../../lib/config/configItems.joi');
const { supportedSaslProtocols } = require('./constants');

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

const basicAuthSchema = saslAuthSchema.append({
    type: joi.string().valid('basic').required(),
    credentialsFile: joi.string().required(),
});

const credentialsFileSchema = joi.object({
    username: joi.string().required(),
    password: joi.string().required(),
});

const authSchema = joi.alternatives().try(sslSchema, kerberosAuthSchema, basicAuthSchema).default({});

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
});

const joiSchema = joi.object({
    topic: joi.string(),
    monitorNotificationFailures: joi.boolean().default(true),
    notificationFailedTopic: joi.string().optional(),
    zookeeperPath: joi.string().optional(),
    queueProcessor: joi.object({
        groupId: joi.string().required(),
        concurrency: joi.number().greater(0).default(1000),
    }),
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
    NotificationConfigValidator: configValidator,
    authSchema,
    credentialsFileSchema,
};
