'use strict'; // eslint-disable-line

const werelogs = require('werelogs');
const { errors } = require('arsenal');
const {
    DEFAULT_LIVE_ROUTE,
    DEFAULT_METRICS_ROUTE,
    DEFAULT_READY_ROUTE,
} = require('arsenal').network.probe.ProbeServer;
const { sendSuccess, sendError } = require('arsenal').network.probe.Utils;
const { ZenkoMetrics } = require('arsenal').metrics;

const MongoQueueProcessor = require('./MongoQueueProcessor');
const config = require('../../lib/Config');
const { initManagement } = require('../../lib/management/index');
const { startProbeServer } = require('../../lib/util/probe');

const kafkaConfig = config.kafka;
const mConfig = config.metrics;
const mongoProcessorConfig = config.extensions.mongoProcessor;
// TODO: consider whether we would want a separate mongo config
// for the consumer side
const mongoClientConfig = config.queuePopulator.mongo;

const log = new werelogs.Logger('Backbeat:MongoProcessor:task');
werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

const mqp = new MongoQueueProcessor(kafkaConfig, mongoProcessorConfig,
    mongoClientConfig, mConfig);

/**
 * Handle ProbeServer liveness check
 *
 * @param {http.HTTPServerResponse} res - HTTP Response to respond with
 * @param {Logger} log - Logger
 * @returns {undefined}
 */
 function handleLiveness(res, log) {
    if (mqp.isReady()) {
        sendSuccess(res, log);
    } else {
        log.error('MongoQueueProcessor is not ready');
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

function loadManagementDatabase() {
    const ingestionServiceAuth = config.extensions.ingestion.auth;
    initManagement({
        serviceName: 'md-ingestion',
        serviceAccount: ingestionServiceAuth.account,
    }, error => {
        if (error) {
            log.error('could not load management db', { error });
            setTimeout(loadManagementDatabase, 5000);
            return;
        }
        log.info('management init done');
        mqp.start();
    });
}

startProbeServer(mongoProcessorConfig.probeServer, (err, probeServer) => {
    if (err) {
        log.error('error starting probe server', { error: err });
        process.exit(1);
    }
    if (probeServer !== undefined) {
        // following the same pattern as other extensions, where liveness
        // and readiness are handled by the same handler
        probeServer.addHandler([DEFAULT_LIVE_ROUTE, DEFAULT_READY_ROUTE], handleLiveness);
        // retaining the old route and adding support to new route, until
        // metrics handling is consolidated
        probeServer.addHandler(['/_/monitoring/metrics', DEFAULT_METRICS_ROUTE], handleMetrics);
    }
    loadManagementDatabase();
});

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    mqp.stop(error => {
        if (error) {
            log.error('failed to exit properly', {
                error,
            });
            process.exit(1);
        }
        process.exit(0);
    });
});
