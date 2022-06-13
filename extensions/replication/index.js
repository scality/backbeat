const ReplicationConfigValidator = require('./ReplicationConfigValidator');

module.exports = {
    name: 'replication',
    version: '1.0.0',
    configValidator: ReplicationConfigValidator,
    queuePopulatorExtension: () => require('./ReplicationQueuePopulator'),
};
