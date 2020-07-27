const joi = require('@hapi/joi');

const joiSchema = {
    topic: joi.string().required(),
    monitorNotificationFailures: joi.boolean().default(true),
    notificationFailedTopic: joi.string().required(),
    queueProcessor: {
        groupId: joi.string().required(),
        zookeeperPath: joi.string().required(),
        retryTimeoutS: joi.number().default(300),
        concurrency: joi.number().greater(0).default(10),
        logConsumerMetricsIntervalS: joi.number().greater(0).default(60),
    },
};

function configValidator(config) {
    const validatedConfig = joi.attempt(config, joiSchema);
    return validatedConfig;
}

module.exports = configValidator;
