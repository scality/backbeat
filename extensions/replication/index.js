const ReplicationConfigValidator = require('./ReplicationConfigValidator');
const ReplicationOplogPopulatorUtils = require('./ReplicationOplogPopulatorUtils');

module.exports = {
    name: 'replication',
    version: '1.0.0',
    configValidator: ReplicationConfigValidator,
    queuePopulatorExtension: () => require('./ReplicationQueuePopulator'),
    oplogPopulatorUtils: ReplicationOplogPopulatorUtils,
};
