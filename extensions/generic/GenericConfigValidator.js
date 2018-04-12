const joi = require('joi');

const joiSchema = {
    topic: joi.string().required(),
};

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = configValidator;
