'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const LifecycleConductor = require('./LifecycleConductor');
const { HealthProbeServer } = require('arsenal').network.probe;

const config = require('../../../conf/Config');
const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const lcConfig = config.extensions.lifecycle;

const lcConductor = new LifecycleConductor(zkConfig, kafkaConfig, lcConfig);

werelogs.configure({ level: config.log.logLevel,
                     dump: config.log.dumpLevel });
const logger = new werelogs.Logger('Backbeat:Lifecycle:Conductor:service');
const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

lcConductor.start(err => {
    if (err) {
        logger.error('error during lifecycle conductor initialization',
                     { error: err.message });
        return undefined;
    }
    healthServer.onReadyCheck(log => {
        if (lcConductor.isReady()) {
            return true;
        }
        log.error('LifecycleConductor is not ready!');
        return false;
    });
    logger.info('Starting HealthProbe server');
    healthServer.start();
    logger.info('lifecycle conductor process is running');
    return undefined;
});

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    lcConductor.stop(() => {
        process.exit(0);
    });
});
