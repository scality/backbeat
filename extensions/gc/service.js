'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const GarbageCollector = require('./GarbageCollector');

const config = require('../../lib/Config');
const kafkaConfig = config.kafka;
const s3Config = config.s3;
const gcConfig = config.extensions.gc;
const transport = config.transport;
const { HealthProbeServer } = require('arsenal').network.probe;

const { initManagement } = require('../../lib/management');
const garbageCollector = new GarbageCollector({
    kafkaConfig,
    s3Config,
    gcConfig,
    transport,
});

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

werelogs.configure({ level: config.log.logLevel,
                     dump: config.log.dumpLevel });
const logger = new werelogs.Logger('Backbeat:GC:service');

function initAndStart() {
    initManagement({
        serviceName: 'gc',
        serviceAccount: gcConfig.auth.account,
    }, error => {
        if (error) {
            logger.error('could not load management db', { error });
            setTimeout(initAndStart, 5000);
            return;
        }
        logger.info('management init done');
        healthServer.onReadyCheck(log => {
            if (garbageCollector.isReady()) {
                return true;
            }
            log.error('GarbageCollector is not ready!');
            return false;
        });
        logger.info('Starting HealthProbe server');
        healthServer.start();
        garbageCollector.start(err => {
            if (err) {
                logger.error('error during garbage collector initialization',
                             { error: err.message });
            } else {
                logger.info('garbage collector is running');
            }
        });
    });
}

initAndStart();

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    garbageCollector.stop(() => {
        process.exit(0);
    });
});
