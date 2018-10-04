const joi = require('joi');
const { retryParamsJoi } = require('../../lib/config/configItems.joi.js');

const joiSchema = {
    zookeeperPath: joi.string().required(),
    bucketTasksTopic: joi.string().required(),
    objectTasksTopic: joi.string().required(),
    auth: joi.object({
        type: joi.alternatives().try('account', 'service', 'vault')
            .required(),
        account: joi.string()
            .when('type', { is: 'account', then: joi.required() })
            .when('type', { is: 'service', then: joi.required() }),
        vault: joi.object({
            host: joi.string().required(),
            port: joi.number().greater(0).required(),
            adminPort: joi.number().greater(0)
                .when('adminCredentialsFile', {
                    is: joi.exist(),
                    then: joi.required(),
                }),
            adminCredentialsFile: joi.string().optional(),
        }).when('type', { is: 'vault', then: joi.required() }),
    }).required(),
    backlogMetrics: {
        zkPath: joi.string().default('/lifecycle/run/backlog-metrics'),
        intervalS: joi.number().default(60),
    },
    conductor: {
        cronRule: joi.string().required(),
        concurrency: joi.number().greater(0).default(10),
        backlogControl: joi.object({
            enabled: joi.boolean().default(true),
        }).default({ enabled: true }),
    },
    bucketProcessor: {
        groupId: joi.string().required(),
        retry: retryParamsJoi,
        // a single producer task is already involving concurrency in
        // the processing, no need to add more here to avoid
        // overloading the system
        concurrency: joi.number().greater(0).default(1),
    },
    objectProcessor: {
        groupId: joi.string().required(),
        retry: retryParamsJoi,
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
