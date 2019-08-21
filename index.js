module.exports = {
    Config: require('./lib/Config'),
    management: require('./lib/management/index'),
    BackbeatConsumer: require('./lib/BackbeatConsumer'),
    BackbeatProducer: require('./lib/BackbeatProducer'),
    clients: {
        zookeeper: require('./lib/clients/zookeeper'),
    },
    models: {
        QueueEntry: require('./lib/models/QueueEntry'),
        ObjectQueueEntry: require('./lib/models/ObjectQueueEntry'),
    },
    credentials: {
        AccountCredentials: require('./lib/credentials/AccountCredentials'),
    },
};
