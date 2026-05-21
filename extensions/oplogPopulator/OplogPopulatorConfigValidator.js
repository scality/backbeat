const joi = require('joi');
const { probeServerJoi } = require('../../lib/config/configItems.joi');

const joiSchema = joi.object({
    topic: joi.string().required(),
    kafkaConnectHost: joi.string().required(),
    kafkaConnectPort: joi.number().required(),
    numberOfConnectors: joi.number().required().min(0),
    locationStrippingBytesThreshold: joi.number().min(0).default(0),
    prefix: joi.string().optional(),
    probeServer: probeServerJoi.default(),
    connectorsUpdateCronRule: joi.string().default('*/1 * * * * *'),
    heartbeatIntervalMs: joi.number().default(10000),
    // When true, oplog source connectors use the TransformObjectKey SMT to
    // key messages by the raw S3 object key (BB-768). Enable only once the
    // Kafka Connect image ships the TransformObjectKey plugin.
    transformObjectKey: joi.boolean().default(false),
});

function configValidator(backbeatConfig, extConfig) {
    const validatedConfig = joi.attempt(extConfig, joiSchema);
    return validatedConfig;
}

module.exports = {
    OplogPopulatorConfigJoiSchema: joiSchema,
    OplogPopulatorConfigValidator: configValidator
};
