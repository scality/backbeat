'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const ReplicationStatusProcessor = require('./ReplicationStatusProcessor');

const config = require('../../../conf/Config');
const kafkaConfig = config.kafka;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;

const { initManagement } = require('../../../lib/management/index');
const { HealthProbeServer } = require('arsenal').network.probe;

const replicationStatusProcessor =
          new ReplicationStatusProcessor(kafkaConfig, sourceConfig, repConfig);

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

werelogs.configure({ level: config.log.logLevel,
     dump: config.log.dumpLevel });

const logger = new werelogs.Logger('backbeat:ReplicationStatusProcessor:Init');
function initAndStart() {
    initManagement({
        serviceName: 'replication',
        serviceAccount: sourceConfig.auth.account,
    }, error => {
        if (error) {
            logger.error('could not load management db', error);
            setTimeout(initAndStart, 5000);
            return;
        }
        healthServer.onReadyCheck(log => {
            if (replicationStatusProcessor.isReady()) {
                return true;
            }
            log.error('ReplicationStatusProcessor is not ready!');
            return false;
        });
        logger.info('Starting HealthProbe server');
        healthServer.start();
        logger.info('management init done');
        replicationStatusProcessor.start();
    });
}

initAndStart();
