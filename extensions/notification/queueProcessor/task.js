'use strict'; // eslint-disable-line
const assert = require('assert');
const { errors } = require('arsenal');
const async = require('async');
const werelogs = require('werelogs');
const {
    DEFAULT_LIVE_ROUTE,
    DEFAULT_READY_ROUTE,
} = require('arsenal').network.probe.ProbeServer;
const { sendSuccess, sendError } = require('arsenal').network.probe.Utils;
const QueueProcessor = require('./QueueProcessor');
const { startProbeServer } = require('../../../lib/util/probe');

const config = require('../../../lib/Config');
const kafkaConfig = config.kafka;
const notifConfig = config.extensions.notification;
const mongoConfig = config.queuePopulator.mongo;

const log = new werelogs.Logger('Backbeat:NotificationProcessor:task');
werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

const destination = process.argv[2];
assert(destination, 'task must be started with a destination as argument');
// get destination auth config from environment variables
let destinationAuth = {
    type: process.env.TYPE,
    ssl: process.env.SSL === 'true',
    protocol: process.env.PROTOCOL,
    ca: process.env.CA,
    client: process.env.CLIENT,
    key: process.env.KEY,
    keyPassword: process.env.KEY_PASSWORD,
    keytab: process.env.KEYTAB,
    principal: process.env.PRINCIPAL,
    serviceName: process.env.SERVICE_NAME,
};
const isDestinationAuthEmpty = Object.values(destinationAuth)
    .every(x => !x);
if (isDestinationAuthEmpty) {
    destinationAuth = null;
}
const queueProcessor = new QueueProcessor(
    mongoConfig, kafkaConfig, notifConfig, destination, destinationAuth);

/**
 * Handle ProbeServer liveness check
 *
 * @param {http.HTTPServerResponse} res - HTTP Response to respond with
 * @param {Logger} log - Logger
 * @returns {undefined}
 */
 function handleLiveness(res, log) {
    if (queueProcessor.isReady()) {
        sendSuccess(res, log);
    } else {
        log.error('Notification Queue Processor is not ready');
        sendError(res, log, errors.ServiceUnavailable, 'unhealthy');
    }
}

async.series([
    next => queueProcessor.start(null, next),
    next => startProbeServer(notifConfig.probeServer, (err, probeServer) => {
        if (err) {
            log.error('error starting probe server', { error: err });
            return next(err);
        }
        if (probeServer !== undefined) {
            // following the same pattern as other extensions, where liveness
            // and readiness are handled by the same handler
            probeServer.addHandler([DEFAULT_LIVE_ROUTE, DEFAULT_READY_ROUTE], handleLiveness);
        }
        return next();
    })
], err => {
    if (err) {
        log.error('error starting notification queue processor task', {
            method: 'notification.task.queueProcessor',
            error: err,
        });
        process.emit('SIGTERM');
    }
});

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    queueProcessor.stop(error => {
        if (error) {
            log.error('failed to exit properly', {
                error,
            });
            process.exit(1);
        }
        process.exit(0);
    });
});

