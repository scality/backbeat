'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const QueueProcessor = require('./QueueProcessor');

const config = require('../../../conf/Config');
const zkConfig = config.zookeeper;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;
const destConfig = repConfig.destination;

const queueProcessor = new QueueProcessor(zkConfig,
                                          sourceConfig, destConfig,
                                          repConfig);

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

queueProcessor.start();
