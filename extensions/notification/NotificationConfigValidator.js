const joi = require('joi');

const joiSchema = {
    topic: joi.string(),
    monitorNotificationFailures: joi.boolean().default(true),
    notificationFailedTopic: joi.string().optional(),
    zookeeperPath: joi.string().optional(),
    queueProcessor: {
        groupId: joi.string().required(),
        retryTimeoutS: joi.number().default(300),
        concurrency: joi.number().greater(0).default(10),
        logConsumerMetricsIntervalS: joi.number().greater(0).default(60),
    },
};

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = configValidator;
