'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const ReplicationStatusProcessor = require('./ReplicationStatusProcessor');

const config = require('../../../conf/Config');
const zkConfig = config.zookeeper;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;
const internalHttpsConfig = config.internalHttps;

const replicationStatusProcessor = new ReplicationStatusProcessor(
    zkConfig, sourceConfig, repConfig, internalHttpsConfig);

werelogs.configure({ level: config.log.logLevel,
                     dump: config.log.dumpLevel });

replicationStatusProcessor.start();
