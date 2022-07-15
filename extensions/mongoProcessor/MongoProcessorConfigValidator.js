const joi = require('joi');
const { retryParamsJoi, probeServerJoi } = require('../../lib/config/configItems.joi');

const joiSchema = joi.object({
    topic: joi.string().required(),
    groupId: joi.string().required(),
    retry: retryParamsJoi,
    probeServer: probeServerJoi.default(),
});

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = configValidator;
