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
const SelfSeeder = require('./SelfSeeder');
const { buildGroupId, createSliceFilter } = require('../utils/workgroups');
const { startProbeServer } = require('../../../lib/util/probe');

const config = require('../../../lib/Config');
const kafkaConfig = config.kafka;
const zkConfig = config.zookeeper;
const mongoConfig = config.queuePopulator && config.queuePopulator.mongo;
const notifConfig = config.extensions.notification;

// 'internal': the worker reads today's internal topic and matches events
// against the bucket rules itself; 'delivery': it reads the addressed
// delivery topic
const isInternalSource = notifConfig.deliveryPool &&
    notifConfig.deliveryPool.source !== 'delivery';
const consumedTopic = isInternalSource ?
    notifConfig.topic : notifConfig.deliveryPool.topic;

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
let watermarks = null;

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
        topic: consumedTopic,
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
 * Seeds this worker's consumer group when it has none, so that replacing
 * the containers is the whole deployment: no command runs between the run's
 * stop and its start.
 *
 * One worker of the generation takes a zookeeper lock and seeds every group
 * of the document, including the watermarks this start then reads; the
 * others wait for their own offsets to appear. A group that already has
 * committed offsets is left alone, so a restart, a rolling replacement and
 * an operator who seeded ahead with the CLI all behave the same.
 *
 * @param {Function} done - callback
 * @return {undefined}
 */
function seedOnStart(done) {
    if (!workgroupLoader || !SelfSeeder.isEnabled(notifConfig)) {
        return process.nextTick(done);
    }
    const zkClient = workgroupLoader.getZkClient();
    if (!zkClient) {
        // the document came from the on-disk cache, so there is no session
        // to hold a lock in and no way to tell another worker apart from
        // this one: leave the group to the assertion below
        log.warn('the workgroups document was not read from zookeeper, so ' +
            'this worker cannot seed its own group', {
            method: 'notification.task.deliveryWorker.seedOnStart',
        });
        return process.nextTick(done);
    }
    const seeder = new SelfSeeder({
        kafkaConfig,
        zkConfig,
        notifConfig,
        doc: workgroupLoader.getConfig(),
        groupId: workgroup.groupId,
        workgroupId: workgroup.id,
        zkClient,
        logger: log,
    });
    return seeder.seed((err, outcome) => {
        log.info('seed on start finished', {
            method: 'notification.task.deliveryWorker.seedOnStart',
            groupId: workgroup.groupId,
            seeded: outcome.seeded,
            reason: outcome.reason,
        });
        return done();
    });
}

/**
 * Loads the per destination watermarks of the generation, when the worker
 * reads the internal topic: a matching record below a destination's
 * watermark was already delivered to it before this generation took over
 *
 * @param {Function} done - callback
 * @return {undefined}
 */
function loadWatermarks(done) {
    if (!isInternalSource || !workgroupLoader) {
        return process.nextTick(done);
    }
    return workgroupLoader.loadWatermarks((err, loaded) => {
        if (err) {
            return done(err);
        }
        watermarks = loaded;
        return done();
    });
}

/**
 * Fails the process rather than joining a consumer group with no committed
 * offsets: fromOffset is 'earliest', so a group that was never seeded, or
 * whose seed expired, would replay the whole topic.
 *
 * On the delivery topic only a generation past its first is checked, since
 * the first one starts on an empty topic. On the internal topic every group
 * is checked: the topic already holds everything the processors delivered.
 *
 * @param {Function} done - callback
 * @return {undefined}
 */
function assertOffsetsAreSeeded(done) {
    const doc = workgroupLoader && workgroupLoader.getConfig();
    if (isInternalSource) {
        return assertSeededOffsets({
            kafkaConfig,
            topic: consumedTopic,
            groupId: workgroup ? workgroup.groupId :
                notifConfig.deliveryPool.groupId,
            barriers: doc && doc.barriers,
            seedCommand: 'notificationDeliverySeed seed-from-processors ' +
                '(or seed-from-generation)',
            logger: log,
        }, done);
    }
    if (!doc || !(doc.barriers || doc.generation >= 2)) {
        return process.nextTick(done);
    }
    return assertSeededOffsets({
        kafkaConfig,
        topic: consumedTopic,
        groupId: workgroup.groupId,
        barriers: doc.barriers,
        logger: log,
    }, done);
}

async.series([
    next => setupWorkgroup(next),
    // before the watermarks are read: a self seeding start writes them
    next => seedOnStart(next),
    next => loadWatermarks(next),
    next => assertOffsetsAreSeeded(next),
    next => {
        // no destination argument: the worker serves every destination it
        // owns, matched per record on the internal topic or carried by the
        // record on the delivery topic
        deliveryWorker = new DeliveryWorker(kafkaConfig, notifConfig,
            workgroup, { mongoConfig, zkConfig, watermarks });
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
