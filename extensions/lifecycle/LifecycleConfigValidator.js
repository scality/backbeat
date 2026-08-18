const joi = require('joi');
const {
    authJoi,
    hostPortJoi,
    inheritedAuthJoi,
    mongoJoi,
    probeServerJoi,
    retryParamsJoi,
} = require('../../lib/config/configItems.joi');
const { extensionConfigValidator } = require('../../lib/config/extensionConfigValidator');

const { backbeatConsumer: { MAX_QUEUED_DEFAULT } } = require('../../lib/constants');
const { ValidLifecycleRules: supportedLifecycleRules } = require('arsenal').models;

const joiSchema = joi.object({
    zookeeperPath: joi.string().required(),
    bucketTasksTopic: joi.string().required().meta({ env: 'BUCKET_TASK_TOPIC' }),
    objectTasksTopic: joi.string().required().meta({ env: 'OBJECT_TASK_TOPIC' }),
    transitionTasksTopic: joi.string().default(parent => parent.objectTasksTopic),
    coldStorageTopics: joi.array().items(joi.string()).unique().default([]),
    auth: authJoi.optional(),
    forceLegacyListing: joi.boolean().default(false),
    autoCreateIndexes: joi.boolean().default(false),
    conductor: {
        auth: inheritedAuthJoi,
        bucketSource: joi.string().
            valid('bucketd', 'zookeeper', 'mongodb').default('zookeeper'),
        bucketd: hostPortJoi.
            when('bucketSource', { is: 'bucketd', then: joi.required() }),
        mongodb: mongoJoi.
            when('bucketSource', { is: 'mongodb', then: joi.required() }),
        cronRule: joi.string().required().meta({ env: 'CRONRULE' }),
        concurrency: joi.number().greater(0).default(10),
        concurrentIndexesBuildLimit: joi.number().greater(0).default(10),
        backlogControl: joi.object({
            enabled: joi.boolean().default(true),
        }).default({ enabled: true }),
        filter: joi.object({
            deny: joi.object({
                buckets: joi.array().items(joi.string()),
                accounts: joi.array().items(joi.string()),
            }),
        }),
        probeServer: probeServerJoi.default(),
        vaultAdmin: hostPortJoi,
        circuitBreaker: joi.object().optional(),
    },
    bucketProcessor: {
        auth: inheritedAuthJoi,
        groupId: joi.string().required(),
        retry: retryParamsJoi,
        // a single producer task is already involving concurrency in
        // the processing, no need to add more here to avoid
        // overloading the system
        concurrency: joi.number().greater(0).default(1),
        probeServer: probeServerJoi.default(),
        circuitBreaker: joi.object().optional(),
    },
    objectProcessor: {
        auth: inheritedAuthJoi,
        groupId: joi.string().required(),
        retry: retryParamsJoi,
        concurrency: joi.number().greater(0).default(10),
        maxQueued: joi.number().greater(0).default(MAX_QUEUED_DEFAULT),
        probeServer: probeServerJoi.default(),
        circuitBreaker: joi.object().optional(),
    },
    transitionProcessor: {
        auth: inheritedAuthJoi,
        groupId: joi.string().required(),
        retry: retryParamsJoi,
        concurrency: joi.number().greater(0).default(10),
        maxQueued: joi.number().greater(0).default(MAX_QUEUED_DEFAULT),
        probeServer: probeServerJoi.default(),
        circuitBreaker: joi.object().optional(),
    },
    coldStorageArchiveTopicPrefix: joi.string().default('cold-archive-req-'),
    coldStorageRestoreTopicPrefix: joi.string().default('cold-restore-req-'),
    coldStorageRestoreAdjustTopicPrefix: joi.string().default('cold-restore-adjust-req-'),
    coldStorageGCTopicPrefix: joi.string().default('cold-gc-req-'),
    coldStorageStatusTopicPrefix: joi.string().default('cold-status-'),
    supportedLifecycleRules: joi.array().items(
        joi.string().valid(...supportedLifecycleRules)
    ).default(supportedLifecycleRules),
});

module.exports = extensionConfigValidator('lifecycle', joiSchema);
