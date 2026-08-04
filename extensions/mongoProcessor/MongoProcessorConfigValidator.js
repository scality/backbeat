const joi = require('joi');
const { retryParamsJoi, probeServerJoi, logJoiOptional } = require('../../lib/config/configItems.joi');

const { MAX_QUEUED_DEFAULT }  = require('../../lib/constants').backbeatConsumer;

const joiSchema = joi.object({
    topic: joi.string().required(),
    groupId: joi.string().required(),
    retry: retryParamsJoi,
    concurrency: joi.number().greater(0).default(1),
    maxQueued: joi.number().greater(0).default(MAX_QUEUED_DEFAULT),
    probeServer: probeServerJoi.default(),
    circuitBreaker: joi.object().optional(),
    log: logJoiOptional,
});

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = configValidator;
