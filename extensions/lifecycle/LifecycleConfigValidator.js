const joi = require('@hapi/joi');
const { retryParamsJoi, authJoi, probeServerJoi } =
    require('../../lib/config/configItems.joi.js');

const joiSchema = {
    zookeeperPath: joi.string().required(),
    bucketTasksTopic: joi.string().required(),
    objectTasksTopic: joi.string().required(),
    auth: authJoi.required(),
    conductor: {
        cronRule: joi.string().required(),
        concurrency: joi.number().greater(0).default(10),
        backlogControl: joi.object({
            enabled: joi.boolean().default(true),
        }).default({ enabled: true }),
        probeServer: probeServerJoi.default(),
    },
    bucketProcessor: {
        groupId: joi.string().required(),
        retry: retryParamsJoi,
        // a single producer task is already involving concurrency in
        // the processing, no need to add more here to avoid
        // overloading the system
        concurrency: joi.number().greater(0).default(1),
        probeServer: probeServerJoi.default(),
    },
    objectProcessor: {
        groupId: joi.string().required(),
        retry: retryParamsJoi,
        concurrency: joi.number().greater(0).default(10),
        probeServer: probeServerJoi.default(),
    },
    rules: {
        expiration: {
            enabled: joi.boolean().default(true),
        },
        noncurrentVersionExpiration: {
            enabled: joi.boolean().default(true),
        },
        transitions: {
            enabled: joi.boolean().default(true),
        },
        abortIncompleteMultipartUpload: {
            enabled: joi.boolean().default(true),
        },
    },
};

function configValidator(backbeatConfig, extConfig) {
    return joi.attempt(extConfig, joiSchema);
}

module.exports = configValidator;
