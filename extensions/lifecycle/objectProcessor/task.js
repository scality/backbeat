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
const LifecycleObjectProcessor = require('./LifecycleObjectProcessor');
const { startProbeServer } = require('../../../lib/util/probe');
const config = require('../../../lib/Config');

const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const lcConfig = config.extensions.lifecycle;
const s3Config = config.s3;
const transport = config.transport;

const logger = new werelogs.Logger('Backbeat:Lifecycle:Consumer');

const objectProcessor = new LifecycleObjectProcessor(
    zkConfig, kafkaConfig, lcConfig, s3Config, transport);

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

function livenessCheck(res, log) {
    if (objectProcessor.isReady()) {
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
 async function handleMetrics(res, log) {
    log.debug('metrics requested');
    res.writeHead(200, {
        'Content-Type': ZenkoMetrics.asPrometheusContentType(),
    });
    const metrics = await ZenkoMetrics.asPrometheus();
    res.end(metrics);
}

function probeServerSetup(config, done) {
    startProbeServer(config, (err, probeServer) => {
        if (err) {
            return done(err);
        }

        if (!probeServer) {
            logger.info('Skipping lifecycle object processor server setup');
            return done();
        }

        probeServer.addHandler(DEFAULT_LIVE_ROUTE, livenessCheck);
        probeServer.addHandler(DEFAULT_READY_ROUTE, livenessCheck);
        // retaining the old route and adding support to new route, until
        // metrics handling is consolidated
        probeServer.addHandler(['/_/monitoring/metrics', DEFAULT_METRICS_ROUTE], handleMetrics);
        logger.info('Starting lifecycle object processor server');
        return done();
    });
}

function initAndStart(done) {
    initManagement({
        serviceName: 'lifecycle',
        serviceAccount: lcConfig.auth.account,
    }, error => {
        if (error) {
            logger.error('could not load management db', { error });
            setTimeout(initAndStart, 5000, done);
            return;
        }
        logger.info('management init done');
        done();
        return;
    });
}


async.waterfall([
    done => initAndStart(done),
    done => objectProcessor.start(err => done(err)),
    done => probeServerSetup(lcConfig.objectProcessor.probeServer, done),
], err => {
    if (err) {
        logger.error('error during lifecycle object processor initialization',
            { error: err.message });
        process.exit(1);
    }
    logger.info('lifecycle object processor running!');
});

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    objectProcessor.close(() => {
        process.exit(0);
    });
});
