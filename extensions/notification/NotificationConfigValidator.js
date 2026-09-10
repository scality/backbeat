const joi = require('joi');
const { probeServerJoi } = require('../../lib/config/configItems.joi');

const { MAX_QUEUED_DEFAULT }  = require('../../lib/constants').backbeatConsumer;
const { WORKGROUP_ID_PATTERN } = require('./utils/workgroups');
const {
    authSchema,
    credentialsFileSchema,
    destinationSchema,
} = require('./utils/destinationSchema');

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
        // several consumer groups over the same delivery topic, each owning
        // a rule defined slice of the destinations. Absent means the single
        // pool: no zookeeper connection, no slice filter, and the configured
        // groupId is used as it is.
        workgroups: joi.object({
            id: joi.string().pattern(WORKGROUP_ID_PATTERN),
            zookeeperPath: joi.string()
                .default('/notification/delivery-workgroups'),
            cachePath: joi.string()
                .default('/tmp/backbeat-delivery-workgroups.json'),
            generation: joi.number().integer().min(1),
        }).optional(),
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
