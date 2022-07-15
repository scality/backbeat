// eslint-disable-next-line
const joi = require('joi');

const { hostPortJoi } = require('../../lib/config/configItems.joi');

const joiSchema = {
    zookeeperPath: joi.string().required(),
    fissionRouter: hostPortJoi.required(),
    s3: hostPortJoi.required(),
    topic: joi.string().required(),
    groupId: joi.string().required(),
};

function configValidator(backbeatConfig, extConfig) {
    return joi.attempt(extConfig, joiSchema);
}

module.exports = configValidator;
