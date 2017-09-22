const ReplicationQueuePopulator = require('./ReplicationQueuePopulator');

module.exports = {
    name: 'replication',
    version: '1.0.0',
    queuePopulatorExtension: ReplicationQueuePopulator,
};
