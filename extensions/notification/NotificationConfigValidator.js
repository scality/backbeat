const joi = require('joi');

const authSchema = joi.object({
    type: joi.string(),
    ssl: joi.boolean(),
    protocol: joi.string(),
    ca: joi.string(),
    client: joi.string(),
    key: joi.string(),
    keyPassword: joi.string(),
    keytab: joi.string(),
    principal: joi.string(),
    serviceName: joi.string(),
});

const destinationSchema = joi.object({
    resource: joi.string().required(),
    type: joi.string().required(),
    host: joi.string().required(),
    port: joi.number().required(),
    internalTopic: joi.string(),
    topic: joi.string().required(),
    auth: authSchema.default({}),
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
