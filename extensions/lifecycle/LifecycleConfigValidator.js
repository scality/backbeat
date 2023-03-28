const joi = require('joi');
const {
    authJoi,
    hostPortJoi,
    inheritedAuthJoi,
    mongoJoi,
    probeServerJoi,
    retryParamsJoi,
} = require('../../lib/config/configItems.joi');

const joiSchema = joi.object({
    zookeeperPath: joi.string().required(),
    bucketTasksTopic: joi.string().required(),
    objectTasksTopic: joi.string().required(),
    coldStorageTopics: joi.array().items(joi.string()).unique().default([]),
    auth: authJoi.optional(),
    conductor: {
        auth: inheritedAuthJoi,
        bucketSource: joi.string().
            valid('bucketd', 'zookeeper', 'mongodb').default('zookeeper'),
        bucketd: hostPortJoi.
            when('bucketSource', { is: 'bucketd', then: joi.required() }),
        mongodb: mongoJoi.
            when('bucketSource', { is: 'mongodb', then: joi.required() }),
        cronRule: joi.string().required(),
        concurrency: joi.number().greater(0).default(10),
        backlogControl: joi.object({
            enabled: joi.boolean().default(true),
        }).default({ enabled: true }),
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
        forceLegacyListing: joi.boolean().default(false),
        circuitBreaker: joi.object().optional(),
    },
    objectProcessor: {
        auth: inheritedAuthJoi,
        groupId: joi.string().required(),
        retry: retryParamsJoi,
        concurrency: joi.number().greater(0).default(10),
        probeServer: probeServerJoi.default(),
        circuitBreaker: joi.object().optional(),
    },
    transitionProcessor: {
        auth: inheritedAuthJoi,
        groupId: joi.string().required(),
        retry: retryParamsJoi,
        concurrency: joi.number().greater(0).default(10),
        probeServer: probeServerJoi.default(),
        circuitBreaker: joi.object().optional(),
    },
    coldStorageArchiveTopicPrefix: joi.string().default('cold-archive-req-'),
    coldStorageRestoreTopicPrefix: joi.string().default('cold-restore-req-'),
    coldStorageStatusTopicPrefix: joi.string().default('cold-status-'),
});

function configValidator(backbeatConfig, extConfig) {
    return joi.attempt(extConfig, joiSchema);
}

module.exports = configValidator;
