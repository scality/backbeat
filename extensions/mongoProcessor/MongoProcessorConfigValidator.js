const joi = require('@hapi/joi');
const { retryParamsJoi } = require('../../lib/config/configItems.joi.js');

const joiSchema = {
    topic: joi.string().required(),
    groupId: joi.string().required(),
    retry: retryParamsJoi,
};

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = configValidator;
