'use strict'; // eslint-disable-line
const werelogs = require('werelogs');

const { initManagement } = require('../../../lib/management/index');
const LifecycleObjectProcessor = require('./LifecycleObjectProcessor');
const { HealthProbeServer } = require('arsenal').network.probe;

const config = require('../../../conf/Config');
const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const lcConfig = config.extensions.lifecycle;
const s3Config = config.s3;
const transport = config.transport;

const log = new werelogs.Logger('Backbeat:Lifecycle:Consumer');

const objectProcessor = new LifecycleObjectProcessor(
    zkConfig, kafkaConfig, lcConfig, s3Config, transport);

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

function initAndStart() {
    initManagement({
        serviceName: 'lifecycle',
        serviceAccount: lcConfig.auth.account,
    }, error => {
        if (error) {
            log.error('could not load management db',
                      { error: error.message });
            setTimeout(initAndStart, 5000);
            return;
        }
        log.info('management init done');
        healthServer.onReadyCheck(log => {
            if (objectProcessor.isReady()) {
                return true;
            }
            log.error('LifecycleConductor is not ready!');
            return false;
        });
        log.info('Starting HealthProbe server');
        healthServer.start();

        objectProcessor.start();
    });
}

initAndStart();

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    objectProcessor.close(() => {
        process.exit(0);
    });
});
