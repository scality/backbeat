'use strict'; // eslint-disable-line
const werelogs = require('werelogs');
const { HealthProbeServer } = require('arsenal').network.probe;

const MongoQueueProcessor = require('./MongoQueueProcessor');

const config = require('../../conf/Config');
const kafkaConfig = config.kafka;
const mongoProcessorConfig = config.extensions.mongoProcessor;
// TODO: consider whether we would want a separate mongo config
// for the consumer side
const mongoClientConfig = config.queuePopulator.mongo;

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

const mongoQueueProcessor = new MongoQueueProcessor(kafkaConfig,
    mongoProcessorConfig, mongoClientConfig);

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

mongoQueueProcessor.start();
healthServer.onReadyCheck(log => {
    if (mongoQueueProcessor.isReady()) {
        return true;
    }
    log.error('MongoQueueProcessor is not ready!');
    return false;
});
healthServer.start();
