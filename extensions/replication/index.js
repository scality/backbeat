const ReplicationConfigValidator = require('./ReplicationConfigValidator');
const ReplicationQueuePopulator = require('./ReplicationQueuePopulator');
const IngestionProducer = require('./IngestionProducer');

module.exports = {
    name: 'replication',
    version: '1.0.0',
    configValidator: ReplicationConfigValidator,
    queuePopulatorExtension: IngestionProducer,
};
