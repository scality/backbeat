const IngestionConfigValidator = require('./IngestionConfigValidator');
const IngestionQueuePopulator = require('./IngestionQueuePopulator');

module.exports = {
    name: 'ingestion',
    version: '1.0.0',
    configValidator: IngestionConfigValidator,
    queuePopulatorExtension: IngestionQueuePopulator,
};
