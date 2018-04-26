const joi = require('joi');
const { zenkoAuthJoi } = require('../../lib/config/configItems.joi.js');

const joiSchema = joi.object({
    topic: joi.string().required(),
    auth: zenkoAuthJoi.required(),
    consumer: {
        groupId: joi.string().required(),
        retryTimeoutS: joi.number().default(300),
        concurrency: joi.number().greater(0).default(10),
    },
});

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = configValidator;
