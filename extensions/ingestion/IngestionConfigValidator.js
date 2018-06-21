const joi = require('joi');

const joiSchema = {
    topic: joi.string().required(),
    zookeeperPath: joi.string().required(),
    cronRule: joi.string().default('*/5 * * * * *'),
    sources: joi.array().required(),
};

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = configValidator;
