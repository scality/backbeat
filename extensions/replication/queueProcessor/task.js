'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const QueueProcessor = require('./QueueProcessor');

const config = require('../../../conf/Config');
const kafkaConfig = config.kafka;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;
const destConfig = repConfig.destination;

const queueProcessor = new QueueProcessor(kafkaConfig,
                                          sourceConfig, destConfig,
                                          repConfig);

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

queueProcessor.start();
