'use strict'; // eslint-disable-line
const werelogs = require('werelogs');
const { initManagement } = require('../../../lib/management/index');
const LifecycleBucketProcessor = require('./LifecycleBucketProcessor');
const { HealthProbeServer } = require('arsenal').network.probe;
const { applyBucketLifecycleWorkflows } = require('../management');
const { zookeeper, kafka, extensions, s3, transport, log, healthcheckServer } =
    require('../../../conf/Config');

werelogs.configure({ level: log.logLevel,
    dump: log.dumpLevel });

const logger = new werelogs.Logger('Backbeat:Lifecycle:Producer');

const bucketProcessor =
    new LifecycleBucketProcessor(zookeeper, kafka, extensions.lifecycle,
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
            logger.error('could not load management db', { error });
            setTimeout(initAndStart, 5000);
            return;
        }
        logger.info('management init done');
        healthServer.onReadyCheck(log => {
            if (bucketProcessor.isReady()) {
                return true;
            }
            log.error('LifecycleBucketProcessor is not ready!');
            return false;
        });
        logger.info('Starting HealthProbe server');
        healthServer.start();

        bucketProcessor.start();
    });
}

initAndStart();

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    bucketProcessor.close(() => {
        process.exit(0);
    });
});
