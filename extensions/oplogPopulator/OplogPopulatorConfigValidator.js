const joi = require('joi');
const { probeServerJoi } = require('../../lib/config/configItems.joi');

const joiSchema = joi.object({
    topic: joi.string().required(),
    kafkaConnectHost: joi.string().required(),
    kafkaConnectPort: joi.number().required(),
    numberOfConnectors: joi.number().required().min(1),
    prefix: joi.string().optional(),
    probeServer: probeServerJoi.default(),
    connectorsUpdateCronRule: joi.string().default('*/1 * * * * *'),
});

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = configValidator;
