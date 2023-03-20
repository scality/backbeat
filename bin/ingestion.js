const async = require('async');
const schedule = require('node-schedule');
const zookeeper = require('node-zookeeper-client');

const werelogs = require('werelogs');
const { errors } = require('arsenal');
const {
    DEFAULT_LIVE_ROUTE,
    DEFAULT_METRICS_ROUTE,
    DEFAULT_READY_ROUTE,
} = require('arsenal').network.probe.ProbeServer;
const { ZenkoMetrics } = require('arsenal').metrics;
const { sendSuccess, sendError } = require('arsenal').network.probe.Utils;
const { reshapeExceptionError } = require('arsenal').errorUtils;

const IngestionPopulator = require('../lib/queuePopulator/IngestionPopulator');
const config = require('../lib/Config');
const { initManagement } = require('../lib/management/index');
const zookeeperWrapper = require('../lib/clients/zookeeper');
const { zookeeperNamespace, zkStatePath } =
    require('../extensions/ingestion/constants');
const { startProbeServer } = require('../lib/util/probe');

const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const ingestionExtConfigs = config.extensions.ingestion;
const qpConfig = config.queuePopulator;
const mConfig = config.metrics;
const rConfig = config.redis;
const s3Config = config.s3;
const { connectionString, autoCreateNamespace } = zkConfig;

const RESUME_NODE = 'scheduledResume';

const log = new werelogs.Logger('Backbeat:IngestionPopulator');
werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

let scheduler;
let ingestionPopulator;
// memoize
const configuredLocations = {};

function getIngestionZkPath() {
    return `${zookeeperNamespace}${zkStatePath}`;
}

function queueBatch(ingestionPopulator, log) {
    log.debug('start queueing ingestion batch');
    const maxRead = qpConfig.batchMaxRead;
    // apply updates to Ingestion Readers
    ingestionPopulator.applyUpdates();
    ingestionPopulator.processLogEntries({ maxRead }, err => {
        if (err) {
            log.fatal('an error occurred during ingestion', {
                method: 'bin::ingestion.queueBatch',
                error: err,
            });
            scheduler.cancel();
            process.exit(1);
        }
    });
}

/**
 * Remove zookeeper state for a location that has been removed
 * @param {node-zookeeper-client.Client} zkClient - zookeeper client
 * @param {String} location - location constraint name
 * @param {Function} cb - callback(error)
 * @return {undefined}
 */
function removeZkState(zkClient, location, cb) {
    const path = `${getIngestionZkPath()}/${location}`;
    zkClient.remove(path, err => {
        if (err && err.name !== 'NO_NODE') {
            log.fatal('failed to remove zookeeper state node', {
                method: 'bin::ingestion.removeZkState',
                zookeeperPath: path,
                error: err.message,
            });
            return cb(err);
        }
        ingestionPopulator.removePausedLocationState(location);
        return cb();
    });
}

/**
 * A scheduled resume is where consumers for a given location are paused and are
 * scheduled to be resumed at a later date.
 * If any scheduled resumes exist for a given location, the date is saved within
 * zookeeper. On startup, we schedule resume jobs to renew prior state.
 * If date in zookeeper has not expired, schedule the job. If date expired,
 * resume automatically for the location, and update the "status" node.
 * @param {node-zookeeper-client.Client} zkClient - zookeeper client
 * @param {Object} data - zookeeper state json data
 * @param {String} location - name of location constraint
 * @param {Function} cb - callback(error, updatedState) where updatedState is
 *   an optional, set as true if the schedule resume date has expired
 * @return {undefined}
 */
function checkAndApplyScheduleResume(zkClient, data, location, cb) {
    const scheduleDate = new Date(data[RESUME_NODE]);
    const hasExpired = new Date() >= scheduleDate;
    if (hasExpired) {
        // if date expired, resume automatically for the location.
        // remove schedule resume data in zookeeper
        const path = `${getIngestionZkPath()}/${location}`;
        const d = JSON.stringify({ paused: false });
        return zkClient.setData(path, Buffer.from(d), err => {
            if (err) {
                log.fatal('could not set zookeeper status node', {
                    method: 'bin::ingestion.checkAndApplyScheduleResume',
                    zookeeperPath: path,
                    error: err.message,
                });
                return cb(err);
            }
            ingestionPopulator.removePausedLocationState(location);
            return cb();
        });
    }
    ingestionPopulator.scheduleResume(location, scheduleDate);
    return cb();
}

/**
 * On startup and when location constraints change, create necessary zookeeper
 * status node to save persistent state.
 * @param {node-zookeeper-client.Client} zkClient - zookeeper client
 * @param {String} location - location constraint
 * @param {Function} done - callback(error)
 * @return {undefined}
 */
function setupZkLocationNode(zkClient, location, done) {
    const path = `${getIngestionZkPath()}/${location}`;
    // set initial ingestion `paused` state of a new location to `true`
    const data = JSON.stringify({ paused: true });
    zkClient.create(path, Buffer.from(data), err => {
        if (err && err.name === 'NODE_EXISTS') {
            return zkClient.getData(path, (err, data) => {
                if (err) {
                    log.fatal('could not check location status in zookeeper',
                        { method: 'bin::ingestion.setupZkLocationNode',
                          zookeeperPath: path,
                          error: err.message });
                    return done(err);
                }
                let d;
                try {
                    d = JSON.parse(data.toString());
                } catch (e) {
                    log.fatal('error setting state for queue processor', {
                        method: 'bin::ingestion.setupZkLocationNode',
                        location,
                        error: reshapeExceptionError(e),
                    });
                    return done(e);
                }
                if (d[RESUME_NODE]) {
                    return checkAndApplyScheduleResume(zkClient, d, location,
                        done);
                }
                if (d.paused === true) {
                    ingestionPopulator.setPausedLocationState(location);
                }
                return done();
            });
        }
        if (err) {
            log.fatal('could not setup zookeeper node', {
                method: 'bin::ingestion.setupZkLocationNode',
                zookeeperPath: path,
                error: err.message,
            });
            return done(err);
        }
        // add the new location to paused locations
        ingestionPopulator.setPausedLocationState(location);
        return done();
    });
}

function updateProcessors(zkClient, bootstrapList) {
    const active = Object.keys(configuredLocations);
    const update = bootstrapList.map(i => i.site);
    const allLocations = [...new Set(active.concat(update))];

    async.each(allLocations, (location, next) => {
        if (!update.includes(location)) {
            // stop tracking location and remove zookeeper path
            return removeZkState(zkClient, location, err => {
                if (err) {
                    return next(err);
                }
                delete configuredLocations[location];
                return next();
            });
        } else if (!active.includes(location)) {
            // track new location and create zookeeper path if not exists
            return setupZkLocationNode(zkClient, location, err => {
                if (err) {
                    return next(err);
                }
                configuredLocations[location] = true;
                return next();
            });
        }
        // else, existing location and has already been setup
        return next();
    }, err => {
        if (err) {
            log.fatal('error setting location state in zookeeper', {
                method: 'bin::ingestion.updateProcessors',
            });
            process.exit(1);
        }
    });
}

function loadProcessors(zkClient) {
    let bootstrapList = config.getBootstrapList();
    updateProcessors(zkClient, bootstrapList);

    config.on('bootstrap-list-update', () => {
        bootstrapList = config.getBootstrapList();
        updateProcessors(zkClient, bootstrapList);
    });
}

/**
 * Handle ProbeServer liveness check
 *
 * @param {http.HTTPServerResponse} res - HTTP Response to respond with
 * @param {Logger} log - Logger
 * @returns {undefined}
 */
 function handleLiveness(res, log) {
    const state = ingestionPopulator.zkStatus();
    if (state.code === zookeeper.State.SYNC_CONNECTED.code) {
        sendSuccess(res, log);
    } else {
        log.error(`Zookeeper is not connected! ${state}`);
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

function initAndStart(zkClient) {
    initManagement({
        serviceName: 'md-ingestion',
        serviceAccount: ingestionExtConfigs.auth.account,
        enableIngestionUpdates: true,
    }, error => {
        if (error) {
            log.error('could not load management db', { error });
            setTimeout(initAndStart, 5000);
            return;
        }
        log.info('management init done');

        ingestionPopulator = new IngestionPopulator(zkClient, zkConfig,
            kafkaConfig, qpConfig, mConfig, rConfig, ingestionExtConfigs,
            s3Config);

        loadProcessors(zkClient);

        async.series([
            done => ingestionPopulator.open(done),
            done => {
                scheduler = schedule.scheduleJob(ingestionExtConfigs.cronRule,
                    () => queueBatch(ingestionPopulator, log));
                return done();
            },
            done => startProbeServer(ingestionExtConfigs.probeServer, (err, probeServer) => {
                if (err) {
                    log.error('error starting probe server', { error: err });
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
        ], err => {
            if (err) {
                log.fatal('error during ingestion populator initialization', {
                    method: 'bin::ingestion.initAndStart',
                    error: err,
                });
                process.exit(1);
            }
        });
    });
}

const zkClient = zookeeperWrapper.createClient(connectionString, {
    autoCreateNamespace,
});
zkClient.connect();
zkClient.once('error', err => {
    log.fatal('error connecting to zookeeper', {
        error: err.message,
    });
    // error occurred at startup trying to start internal clients,
    // fail immediately
    process.exit(1);
});
zkClient.once('ready', () => {
    zkClient.removeAllListeners('error');
    const path = getIngestionZkPath();
    zkClient.mkdirp(path, err => {
        if (err) {
            log.fatal('could not create path in zookeeper', {
                method: 'bin::ingestion',
                zookeeperPath: path,
                error: err.message,
            });
            // error occurred at startup trying to start internal clients,
            // fail immediately
            process.exit(1);
        }
        return initAndStart(zkClient);
    });
});

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    scheduler.cancel();
    ingestionPopulator.close(error => {
        if (error) {
            log.error('failed to exit properly', {
                error,
            });
            process.exit(1);
        }
        process.exit(0);
    });
});
