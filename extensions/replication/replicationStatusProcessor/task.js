'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const ReplicationStatusProcessor = require('./ReplicationStatusProcessor');

const config = require('../../../conf/Config');
const kafkaConfig = config.kafka;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;

const { initManagement } = require('../../../lib/management');
const replicationStatusProcessor =
          new ReplicationStatusProcessor(kafkaConfig, sourceConfig, repConfig);

werelogs.configure({ level: config.log.logLevel,
     dump: config.log.dumpLevel });

const logger = new werelogs.Logger('replicationStatusProcessorInit');
function initAndStart() {
    initManagement(error => {
        if (error) {
            logger.error('could not load managment db', error);
            setTimeout(initAndStart, 5000);
            return;
        }
        logger.info('management init done');
        replicationStatusProcessor.start();
    });
}

initAndStart();
