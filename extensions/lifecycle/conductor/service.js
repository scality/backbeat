'use strict'; // eslint-disable-line

const async = require('async');
const werelogs = require('werelogs');
const { errors } = require('arsenal');
const {
    DEFAULT_LIVE_ROUTE,
    DEFAULT_METRICS_ROUTE,
    DEFAULT_READY_ROUTE,
} = require('arsenal').network.probe.ProbeServer;
const { sendSuccess, sendError } = require('arsenal').network.probe.Utils;
const { ZenkoMetrics } = require('arsenal').metrics;

const LifecycleConductor = require('./LifecycleConductor');
const config = require('../../../lib/Config');
const { startProbeServer } = require('../../../lib/util/probe');

const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const lcConfig = config.extensions.lifecycle;
const repConfig = config.extensions.replication;


const lcConductor = new LifecycleConductor(zkConfig, kafkaConfig, lcConfig, repConfig);

werelogs.configure({
    level: 'trace',
    dump: 'error',
});
const logger = new werelogs.Logger('Backbeat:Lifecycle:Conductor:service');

/**
 * Handle ProbeServer liveness check
 *
 * @param {http.HTTPServerResponse} res - HTTP Response to respond with
 * @param {Logger} log - Logger
 * @returns {string} response
 */
function handleLiveness(res, log) {
    if (lcConductor.isReady()) {
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
 * @returns {undefined} response
 */
function handleMetrics(res, log) {
    log.debug('metrics requested');
    res.writeHead(200, {
        'Content-Type': ZenkoMetrics.asPrometheusContentType(),
    });
    res.end(ZenkoMetrics.asPrometheus());
}

async.waterfall([
    done => startProbeServer(lcConfig.conductor.probeServer, (err, probeServer) => {
        if (err) {
            logger.error('error starting probe server', { error: err });
            return done(err);
        }
        if (probeServer !== undefined) {
            // following the same pattern as other extensions, where liveness
            // and readiness are handled by the same handler
            probeServer.addHandler([DEFAULT_LIVE_ROUTE, DEFAULT_READY_ROUTE], handleLiveness);
            // retaining the old route and adding support to new route, until
            // metrics handling is consolidated
            probeServer.addHandler(['/_/monitoring/metrics', DEFAULT_METRICS_ROUTE], handleMetrics);
        }
        return done();
    }),
    done => lcConductor.start(done),
], err => {

    if (err) {
        logger.error('error during lifecycle conductor initialization', { error: err.message });
        process.exit(1);
    }
    logger.info('lifecycle conductor process is running');
});

process.on('SIGTERM', () => {
    logger.info('received SIGTERM, exiting');
    lcConductor.stop(() => {
        process.exit(0);
    });
});
