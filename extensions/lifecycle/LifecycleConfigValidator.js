const joi = require('joi');

const joiSchema = {
    zookeeperPath: joi.string().required(),
    bucketTasksTopic: joi.string().required(),
    objectTasksTopic: joi.string().required(),
    backlogMetrics: {
        zkPath: joi.string().default('/lifecycle/run/backlog-metrics'),
        intervalS: joi.number().default(60),
    },
    conductor: {
        cronRule: joi.string().required(),
        concurrency: joi.number().greater(0).default(10),
    },
    producer: {
        groupId: joi.string().required(),
        retryTimeoutS: joi.number().default(300),
        // a single producer task is already involving concurrency in
        // the processing, no need to add more here to avoid
        // overloading the system
        concurrency: joi.number().greater(0).default(1),
    },
    consumer: {
        groupId: joi.string().required(),
        retryTimeoutS: joi.number().default(300),
        concurrency: joi.number().greater(0).default(10),
    },
    rules: {
        expiration: {
            enabled: joi.boolean().default(true),
        },
        noncurrentVersionExpiration: {
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
