const joi = require('joi');
const { probeServerJoi } = require('../../lib/config/configItems.joi');
const { extensionConfigValidator } = require('../../lib/config/extensionConfigValidator');

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
});

module.exports = {
    OplogPopulatorConfigJoiSchema: joiSchema,
    OplogPopulatorConfigValidator: extensionConfigValidator('oplogPopulator', joiSchema)
};
