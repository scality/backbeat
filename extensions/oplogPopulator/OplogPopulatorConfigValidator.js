const joi = require('joi');

const connectorShema = joi.object({
    name: joi.string().required(),
});

const joiSchema = joi.object({
    topic: joi.string().required(),
    kafkaConnectHost: joi.string().required(),
    kafkaConnectPort: joi.number().required(),
    connectors: joi.array().items(connectorShema).length(1),
});

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = configValidator;
