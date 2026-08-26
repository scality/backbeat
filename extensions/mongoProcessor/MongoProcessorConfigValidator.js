const joi = require('joi');
const { retryParamsJoi, probeServerJoi, logJoiOptional, mongoJoi } =
    require('../../lib/config/configItems.joi');
const { extensionConfigValidator } = require('../../lib/config/extensionConfigValidator');

const { MAX_QUEUED_DEFAULT }  = require('../../lib/constants').backbeatConsumer;

const joiSchema = joi.object({
    topic: joi.string().required(),
    groupId: joi.string().required(),
    mongodb: mongoJoi,
    retry: retryParamsJoi,
    concurrency: joi.number().greater(0).default(1),
    maxQueued: joi.number().greater(0).default(MAX_QUEUED_DEFAULT),
    probeServer: probeServerJoi.default(),
    circuitBreaker: joi.object().optional(),
    log: logJoiOptional,
});

module.exports = extensionConfigValidator('mongoProcessor', joiSchema);
