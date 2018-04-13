'use strict'; // eslint-disable-line
const werelogs = require('werelogs');

const QueueProcessor = require('./MongoQueueProcessor');

const config = require('../../conf/Config');
const zkConfig = config.zookeeper;
const mongoProcessorConfig = config.extensions.mongoProcessor;
// TODO: consider whether we would want a separate mongo config
// for the consumer side
const mongoClientConfig = config.queuePopulator.mongo;

const queueProcessor = new QueueProcessor(zkConfig,
    mongoProcessorConfig, mongoClientConfig);

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

queueProcessor.start();
