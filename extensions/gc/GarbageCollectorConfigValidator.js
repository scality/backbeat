const joi = require('joi');
const {
    authJoi,
    retryParamsJoi,
    probeServerJoi,
    hostPortJoi,
} = require('../../lib/config/configItems.joi');

const joiSchema = joi.object({
    topic: joi.string().required(),
    auth: authJoi.required(),
    consumer: {
        groupId: joi.string().required(),
        retry: retryParamsJoi,
        concurrency: joi.number().greater(0).default(10),
    },
    probeServer: probeServerJoi.default(),
    vaultAdmin: hostPortJoi,
});

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = configValidator;
