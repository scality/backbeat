'use strict'; // eslint-disable-line

const { errors } = require('arsenal');
const werelogs = require('werelogs');
const {
    DEFAULT_LIVE_ROUTE,
    DEFAULT_READY_ROUTE,
} = require('arsenal').network.probe.ProbeServer;

const GarbageCollector = require('./GarbageCollector');
const { sendSuccess, sendError, startProbeServer } = require('../../lib/util/probe');
const { initManagement } = require('../../lib/management');
const config = require('../../lib/Config');

const kafkaConfig = config.kafka;
const s3Config = config.s3;
const gcConfig = config.extensions.gc;
const transport = config.transport;

const garbageCollector = new GarbageCollector({
    kafkaConfig,
    s3Config,
    gcConfig,
    transport,
});

werelogs.configure({ level: config.log.logLevel,
                     dump: config.log.dumpLevel });
const logger = new werelogs.Logger('Backbeat:GC:service');

/**
 * Handle ProbeServer liveness check
 *
 * @param {http.HTTPServerResponse} res - HTTP Response to respond with
 * @param {Logger} log - Logger
 * @returns {string} response
 */
 function handleLiveness(res, log) {
    if (garbageCollector.isReady()) {
        sendSuccess(res, log);
    } else {
        sendError(res, log, errors.ServiceUnavailable, 'unhealthy');
    }
}

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
        startProbeServer(gcConfig.probeServer, (err, probeServer) => {
            if (err) {
                logger.error('error starting probe server', { error: err });
            }
            if (probeServer !== undefined) {
                // following the same pattern as other extensions, where liveness
                // and readiness are handled by the same handler
                probeServer.addHandler([DEFAULT_LIVE_ROUTE, DEFAULT_READY_ROUTE], handleLiveness);
            }
            garbageCollector.start(err => {
                if (err) {
                    logger.error('error during garbage collector initialization', { error: err.message });
                } else {
                    logger.info('garbage collector is running');
                }
            });
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
