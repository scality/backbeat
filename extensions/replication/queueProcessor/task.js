'use strict'; // eslint-disable-line

const config = require('../../../conf/Config');
const zkConfig = config.zookeeper;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;
const destConfig = repConfig.destination;

const QueueProcessor = require('./QueueProcessor');

const queueProcessor = new QueueProcessor(zkConfig,
                                          sourceConfig, destConfig,
                                          repConfig, config.log);

queueProcessor.start();
