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
const { resolveWorkgroupId } = require('./workgroupConfig');
const { assertSeededOffsets } = require('./seededOffsets');
const { buildGroupId, createSliceFilter } = require('../utils/workgroups');
const { startProbeServer } = require('../../../lib/util/probe');

const config = require('../../../lib/Config');
const kafkaConfig = config.kafka;
const zkConfig = config.zookeeper;
const notifConfig = config.extensions.notification;

const log = new werelogs.Logger('Backbeat:NotificationDeliveryWorker:task');
werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

assert(notifConfig && notifConfig.deliveryPool && notifConfig.deliveryPool.enabled,
    'delivery worker requires extensions.notification.deliveryPool.enabled ' +
    'to be set');

// the worker cannot be built before the workgroups document is loaded: the
// document carries the consumer group it joins and the slice it serves
let deliveryWorker = null;
let workgroupLoader = null;
let workgroup = null;

/**
 * Handle ProbeServer liveness check
 *
 * @param {http.HTTPServerResponse} res - HTTP Response to respond with
 * @param {Logger} log - Logger
 * @returns {undefined}
 */
function handleLiveness(res, log) {
    if (deliveryWorker && deliveryWorker.isReady()) {
        sendSuccess(res, log);
    } else {
        log.error('Notification Delivery Worker is not ready');
        sendError(res, log, errors.ServiceUnavailable, 'unhealthy');
    }
}

const probeServerConfig = resolveProbeServerConfig(
    notifConfig.deliveryPool, process.env, log);
const workgroupId = resolveWorkgroupId(
    notifConfig.deliveryPool, process.env, log);

/**
 * Loads the workgroups document and derives everything the worker needs
 * from it. Does nothing at all when no workgroups block is configured: the
 * pool then runs as the single consumer group it is today.
 *
 * @param {Function} done - callback
 * @return {undefined}
 */
function setupWorkgroup(done) {
    const workgroupsConfig = notifConfig.deliveryPool.workgroups;
    if (!workgroupsConfig) {
        return process.nextTick(done);
    }
    if (!workgroupId) {
        return process.nextTick(() => done(
            errors.InternalError.customizeDescription(
                'workgroups are configured but this worker has no workgroup ' +
                'id: set the DELIVERY_POOL_WORKGROUP_ID environment variable ' +
                'or extensions.notification.deliveryPool.workgroups.id')));
    }
    // required here rather than at the top of the file so that a worker
    // running without workgroups does not expose the loader's metrics
    const WorkgroupConfigLoader = require('./WorkgroupConfigLoader');
    workgroupLoader = new WorkgroupConfigLoader({
        zkConfig,
        workgroupsConfig,
        topic: notifConfig.deliveryPool.topic,
        workgroupId,
        logger: log,
    });
    return workgroupLoader.load((err, loaded) => {
        if (err) {
            return done(err);
        }
        workgroupLoader.startWatch();
        workgroup = {
            id: workgroupId,
            generation: loaded.doc.generation,
            groupId: buildGroupId(notifConfig.deliveryPool.groupId,
                workgroupId, loaded.doc.generation),
            filter: createSliceFilter({ doc: loaded.doc, workgroupId }),
        };
        return done();
    });
}

/**
 * Fails the process rather than joining a consumer group that lost its
 * pre-seeded offsets: fromOffset is 'earliest', so an expired pre-seed
 * would replay the whole delivery topic instead of resuming at the barrier.
 *
 * @param {Function} done - callback
 * @return {undefined}
 */
function assertOffsetsAreSeeded(done) {
    const doc = workgroupLoader && workgroupLoader.getConfig();
    if (!doc || !(doc.barriers || doc.generation >= 2)) {
        return process.nextTick(done);
    }
    return assertSeededOffsets({
        kafkaConfig,
        topic: notifConfig.deliveryPool.topic,
        groupId: workgroup.groupId,
        barriers: doc.barriers,
        logger: log,
    }, done);
}

async.series([
    next => setupWorkgroup(next),
    next => assertOffsetsAreSeeded(next),
    next => {
        // no destination argument: the destination and the notification
        // configuration id are carried by each record of the delivery topic
        deliveryWorker = new DeliveryWorker(kafkaConfig, notifConfig,
            workgroup);
        return deliveryWorker.start(null, next);
    },
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
    async.series([
        next => (workgroupLoader ? workgroupLoader.stop(next) : next()),
        next => (deliveryWorker ? deliveryWorker.stop(next) : next()),
    ], error => {
        if (error) {
            log.error('failed to exit properly', {
                error,
            });
            process.exit(1);
        }
        process.exit(0);
    });
});
