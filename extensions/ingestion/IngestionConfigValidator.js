const joi = require('joi');
const { probeServerJoi } = require('../../lib/config/configItems.joi');
const { KAFKA_PRODUCER_PARAMS_SCHEMA } = require('../../lib/config.joi');

const joiSchema = joi.object({
    auth: joi.object({
        type: joi.string().valid('service').required(),
        account: joi.string().required(),
    }).required(),
    topic: joi.string().required(),
    zookeeperPath: joi.string().required(),
    cronRule: joi.string().default('*/5 * * * * *'),
    maxParallelReaders: joi.number().greater(0).default(5),
    batchMaxRead: joi.number().greater(0).optional(),
    sources: joi.array().required(),
    probeServer: probeServerJoi.default(),
    circuitBreaker: joi.object().optional(),
    processor: joi.object({
        circuitBreaker: joi.object().optional(),
    }).optional(),
    producerParams: KAFKA_PRODUCER_PARAMS_SCHEMA,
});

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = configValidator;
