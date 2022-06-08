const joi = require('joi');

const destinationSchema = joi.object({
    resource: joi.string().required(),
    type: joi.string().required(),
    host: joi.string().required(),
    port: joi.number().required(),
    internalTopic: joi.string(),
    topic: joi.string().required(),
    auth: joi.object().default({}),
});

const joiSchema = joi.object({
    topic: joi.string(),
    monitorNotificationFailures: joi.boolean().default(true),
    notificationFailedTopic: joi.string().optional(),
    queueProcessor: {
        groupId: joi.string().required(),
        concurrency: joi.number().greater(0).default(10),
    },
    destinations: joi.array().items(destinationSchema).default([]),
});

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = configValidator;
