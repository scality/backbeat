const joi = require('joi');

const { hostPortJoi } = require('../../lib/config/configItems.joi.js');

const joiSchema = {
    s3: hostPortJoi.required(),
    topic: joi.string().required(),
    groupId: joi.string().required(),
};

function configValidator(backbeatConfig, extConfig) {
    return joi.attempt(extConfig, joiSchema);
}

module.exports = configValidator;
