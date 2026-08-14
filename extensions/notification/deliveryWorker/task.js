'use strict';
const assert = require('assert');
const { errors, jsutil } = require('arsenal');
const async = require('async');
const werelogs = require('werelogs');
const {
    DEFAULT_LIVE_ROUTE,
    DEFAULT_READY_ROUTE,
    DEFAULT_METRICS_ROUTE,
} = require('arsenal').network.probe.ProbeServer;
const { sendSuccess, sendError } = require('arsenal').network.probe.Utils;
const DeliveryWorker = require('./DeliveryWorker');
const { resolveProbeServerConfig } = require('./probeConfig');
const { startProbeServer } = require('../../../lib/util/probe');

const config = require('../../../lib/Config');
const kafkaConfig = config.kafka;
const notifConfig = config.extensions.notification;

const log = new werelogs.Logger('Backbeat:NotificationDeliveryWorker:task');
werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

assert(notifConfig && notifConfig.deliveryPool && notifConfig.deliveryPool.enabled,
    'delivery worker requires extensions.notification.deliveryPool.enabled ' +
    'to be set');

// no destination argument: the destination and the notification
// configuration id are carried by each record of the delivery topic
const deliveryWorker = new DeliveryWorker(kafkaConfig, notifConfig);

/**
 * Handle ProbeServer liveness check
 *
 * @param {http.HTTPServerResponse} res - HTTP Response to respond with
 * @param {Logger} log - Logger
 * @returns {undefined}
 */
function handleLiveness(res, log) {
    if (deliveryWorker.isReady()) {
        sendSuccess(res, log);
    } else {
        log.error('Notification Delivery Worker is not ready');
        sendError(res, log, errors.ServiceUnavailable, 'unhealthy');
    }
}

const probeServerConfig = resolveProbeServerConfig(
    notifConfig.deliveryPool, process.env, log);

async.series([
    next => deliveryWorker.start(null, next),
    next => startProbeServer(probeServerConfig, jsutil.once((err, probeServer) => {
        if (err) {
            // a worker that cannot serve its probe routes still delivers
            // notifications, so keep going rather than taking the process
            // down: workers sharing a config file also share a port, and
            // only the first of them can bind it
            log.error('probe server not started, continuing without it', {
                error: err.message,
                port: probeServerConfig && probeServerConfig.port,
            });
            return next();
        }
        if (probeServer !== undefined) {
            // following the same pattern as other extensions, where liveness
            // and readiness are handled by the same handler
            probeServer.addHandler([DEFAULT_LIVE_ROUTE, DEFAULT_READY_ROUTE], handleLiveness);
            probeServer.addHandler(DEFAULT_METRICS_ROUTE,
                (res, log) => deliveryWorker.handleMetrics(res, log)
            );
        }
        return next();
    }))
], err => {
    if (err) {
        log.error('error starting notification delivery worker task', {
            method: 'notification.task.deliveryWorker',
            error: err,
        });
        process.emit('SIGTERM');
    }
});

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    deliveryWorker.stop(error => {
        if (error) {
            log.error('failed to exit properly', {
                error,
            });
            process.exit(1);
        }
        process.exit(0);
    });
});
