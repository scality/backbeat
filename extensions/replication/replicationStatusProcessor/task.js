'use strict'; // eslint-disable-line

const ReplicationStatusProcessor = require('./ReplicationStatusProcessor');

const config = require('../../../conf/Config');
const kafkaConfig = config.kafka;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;
const mConfig = config.metrics;

const replicationStatusProcessor = new ReplicationStatusProcessor(kafkaConfig,
    sourceConfig, repConfig, mConfig);

replicationStatusProcessor.start();
