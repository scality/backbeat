'use strict'; // eslint-disable-line

const async = require('async');
const werelogs = require('werelogs');
const { errors } = require('arsenal');
const {
    DEFAULT_LIVE_ROUTE,
    DEFAULT_READY_ROUTE,
    DEFAULT_METRICS_ROUTE,
} = require('arsenal').network.probe.ProbeServer;
const { sendSuccess, sendError } = require('arsenal').network.probe.Utils;
const { ZenkoMetrics } = require('arsenal').metrics;

const { initManagement } = require('../../../lib/management/index');
const LifecycleBucketProcessor = require('./LifecycleBucketProcessor');
const { applyBucketLifecycleWorkflows } = require('../management');
const { startProbeServer } = require('../../../lib/util/probe');
const config = require('../../../lib/Config');

const { zookeeper, kafka, extensions, s3, log, queuePopulator } = config;
const lcConfig = extensions.lifecycle;
const repConfig = extensions.replication;
const mongoConfig = queuePopulator.mongo;

werelogs.configure({
    level: log.logLevel,
    dump: log.dumpLevel,
});


const logger = new werelogs.Logger('Backbeat:Lifecycle:Producer');

const bucketProcessor = new LifecycleBucketProcessor(
    zookeeper, kafka, lcConfig, repConfig, s3, mongoConfig, lcConfig.transport,
);

function livenessCheck(res, log) {
    if (bucketProcessor.isReady()) {
        sendSuccess(res, log);
    } else {
        sendError(res, log, errors.ServiceUnavailable, 'unhealthy');
    }
}

/**
 * Handle ProbeServer metrics
 *
 * @param {http.HTTPServerResponse} res - HTTP Response to respond with
 * @param {Logger} log - Logger
 * @returns {undefined}
 */
 function handleMetrics(res, log) {
    log.debug('metrics requested');
    res.writeHead(200, {
        'Content-Type': ZenkoMetrics.asPrometheusContentType(),
    });
    res.end(ZenkoMetrics.asPrometheus());
}

function probeServerSetup(config, done) {
    startProbeServer(config, (err, probeServer) => {
        if (err) {
            return done(err);
        }

        if (!probeServer) {
            logger.info('Skipping lifecycle bucket processor server setup');
            return done();
        }

        probeServer.addHandler(DEFAULT_LIVE_ROUTE, livenessCheck);
        probeServer.addHandler(DEFAULT_READY_ROUTE, livenessCheck);
        // retaining the old route and adding support to new route, until
        // metrics handling is consolidated
        probeServer.addHandler(['/_/monitoring/metrics', DEFAULT_METRICS_ROUTE], handleMetrics);
        logger.info('Starting lifecycle bucket processor server');
        return done();
    });
}

function updateBootstrapList() {
    const { replication } = config.extensions;
    replication.destination.bootstrapList = config.getBootstrapList();

    config.on('bootstrap-list-update', () => {
        replication.destination.bootstrapList = config.getBootstrapList();
    });
}

function loadManagementDatabase(cb) {
    return initManagement({
        serviceName: 'lifecycle',
        serviceAccount: extensions.lifecycle.auth.account,
        applyBucketWorkflows: applyBucketLifecycleWorkflows,
    }, error => {
        if (error) {
            logger.error('could not load management db', { error });
            setTimeout(loadManagementDatabase, 5000, cb);
            return;
        }
        logger.info('management init done');
        updateBootstrapList();
        cb();
        return;
    });
}

async.waterfall([
    done => loadManagementDatabase(done),
    done => bucketProcessor.start(err => done(err)),
    done => probeServerSetup(lcConfig.bucketProcessor.probeServer, done),
], err => {
    if (err) {
        logger.error('error during lifecycle bucket processor initialization',
            { error: err.message });
        process.exit(1);
    }
    logger.info('lifecycle bucket processor running!');
});

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    bucketProcessor.close(() => {
        process.exit(0);
    });
});
