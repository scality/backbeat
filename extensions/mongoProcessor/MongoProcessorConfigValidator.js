const joi = require('joi');

const joiSchema = {
    topic: joi.string().required(),
    groupId: joi.string().required(),
    retryTimeoutS: joi.number().default(300),
};

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = configValidator;
