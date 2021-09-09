// eslint-disable-next-line
const joi = require('@hapi/joi');

const joiSchema = {
    zookeeperPath: joi.string().required(),
    topic: joi.string().required(),
    groupId: joi.string().required(),
};

function configValidator(backbeatConfig, extConfig) {
    return joi.attempt(extConfig, joiSchema);
}

module.exports = configValidator;
