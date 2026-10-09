const joi = require('joi');
const { probeServerJoi } = require('../../lib/config/configItems.joi');
const { extensionConfigValidator } = require('../../lib/config/extensionConfigValidator');

const joiSchema = joi.object({
    topic: joi.string().required(),
    kafkaConnectHost: joi.string().required(),
    kafkaConnectPort: joi.number().required(),
    ingestion: joi.string().valid('hashed', 'perBucket').default('hashed'),
    // number of hashed connectors in hashed ingestion (0 meaning 1);
    // per-bucket ingestion listens to all buckets with one connector when 0
    numberOfConnectors: joi.number().integer().min(0).required().when('ingestion', {
        is: 'hashed',
        then: joi.number().empty(0).default(1).optional(),
    }),
    locationStrippingBytesThreshold: joi.number().min(0).default(0),
    prefix: joi.string().optional(),
    probeServer: probeServerJoi.default(),
    // hashed connectors need no reaction to bucket changes, only upkeep
    connectorsUpdateCronRule: joi.string().when('ingestion', {
        is: 'hashed',
        then: joi.string().default('*/15 * * * * *'),
        otherwise: joi.string().default('*/1 * * * * *'),
    }),
    heartbeatIntervalMs: joi.number().default(10000),
});

module.exports = {
    OplogPopulatorConfigJoiSchema: joiSchema,
    OplogPopulatorConfigValidator: extensionConfigValidator('oplogPopulator', joiSchema)
};
