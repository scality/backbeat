'use strict'; // eslint-disable-line
const werelogs = require('werelogs');
const { HealthProbeServer } = require('arsenal').network.probe;
const { initManagement } = require('../../../lib/management/index');
const LifecycleBucketProcessor = require('./LifecycleBucketProcessor');
const { applyBucketLifecycleWorkflows } = require('../management');
const config = require('./conf/Config');
const { zookeeper, kafka, extensions, s3, transport, log, healthcheckServer } =
    config;

werelogs.configure({ level: log.logLevel,
    dump: log.dumpLevel });

const logger = new werelogs.Logger('Backbeat:Lifecycle:Producer');

const bucketProcessor =
    new LifecycleBucketProcessor(zookeeper, kafka, extensions, s3, transport);

const healthServer = new HealthProbeServer({
    bindAddress: healthcheckServer.bindAddress,
    port: healthcheckServer.port,
});

function updateBootstrapList() {
    const { replication } = config.extensions;
    replication.destination.bootstrapList = config.getBootstrapList();

    config.on('bootstrap-list-update', () => {
        replication.destination.bootstrapList = config.getBootstrapList();
    });
}

function startHealthProbe() {
    healthServer.onReadyCheck(log => {
        if (bucketProcessor.isReady()) {
            return true;
        }
        log.error('LifecycleBucketProcessor is not ready!');
        return false;
    });
    logger.info('Starting HealthProbe server');
    healthServer.start();
}

function loadManagementDatabase(cb) {
    return initManagement({
        serviceName: 'lifecycle',
        serviceAccount: extensions.lifecycle.auth.account,
        applyBucketWorkflows: applyBucketLifecycleWorkflows,
    }, error => {
        if (error) {
            logger.error('could not load management db', { error });
            setTimeout(loadManagementDatabase, 5000);
            return;
        }
        logger.info('management init done');
        cb();
    });
}

loadManagementDatabase(() => {
    // Error is always caught and retried until the database is loaded.
    updateBootstrapList();
    startHealthProbe();
    bucketProcessor.start();
});

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    bucketProcessor.close(() => {
        process.exit(0);
    });
});
