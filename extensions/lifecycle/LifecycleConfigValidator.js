const joi = require('joi');

const joiSchema = {
    zookeeperPath: joi.string().required(),
    bucketTasksTopic: joi.string().required(),
    objectTasksTopic: joi.string().required(),
    conductor: {
        cronRule: joi.string().required(),
    },
    producer: {
        groupId: joi.string().required(),
        retryTimeoutS: joi.number().default(300),
        concurrency: joi.number().greater(0).default(10),
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
    },
};

function configValidator(backbeatConfig, extConfig) {
    return joi.attempt(extConfig, joiSchema);
}

module.exports = configValidator;
