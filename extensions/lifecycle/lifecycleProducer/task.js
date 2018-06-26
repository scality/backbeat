'use strict'; // eslint-disable-line
const werelogs = require('werelogs');
const { initManagement } = require('../../../lib/management/index');
const LifecycleProducer = require('./LifecycleProducer');
const { HealthProbeServer } = require('arsenal').network.probe;
const { applyBucketLifecycleWorkflows } = require('../management');
const { zookeeper, kafka, extensions, s3, transport, log, healthcheckServer } =
    require('../../../conf/Config');

werelogs.configure({ level: log.logLevel,
    dump: log.dumpLevel });

const logger = new werelogs.Logger('Backbeat:Lifecycle:Producer');

const lifecycleProducer =
    new LifecycleProducer(zookeeper, kafka, extensions.lifecycle,
        s3, transport);

const healthServer = new HealthProbeServer({
    bindAddress: healthcheckServer.bindAddress,
    port: healthcheckServer.port,
});

function initAndStart() {
    initManagement({
        serviceName: 'lifecycle',
        serviceAccount: extensions.lifecycle.auth.account,
        applyBucketWorkflows: applyBucketLifecycleWorkflows,
    }, error => {
        if (error) {
            logger.error('could not load management db',
                { error: error.message });
            setTimeout(initAndStart, 5000);
            return;
        }
        logger.info('management init done');
        healthServer.onReadyCheck(log => {
            if (lifecycleProducer.isReady()) {
                return true;
            }
            log.error('LifecycleProducer is not ready!');
            return false;
        });
        logger.info('Starting HealthProbe server');
        healthServer.start();

        lifecycleProducer.start();
    });
}

initAndStart();

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    lifecycleProducer.close(() => {
        process.exit(0);
    });
});
