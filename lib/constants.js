const constants = {
    statusReady: 'READY',
    statusUndefined: 'UNDEFINED',
    statusNotReady: 'NOT_READY',
    statusNotConnected: 'NOT_CONNECTED',
    services: {
        queuePopulator: 'QueuePopulator',
        replicationQueueProcessor: 'ReplicationQueueProcessor',
        replicationStatusProcessor: 'ReplicationStatusProcessor',
    },
    maxPollInterval: 300000,
};

module.exports = constants;
