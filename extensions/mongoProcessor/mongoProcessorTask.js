'use strict'; // eslint-disable-line

const async = require('async');
const werelogs = require('werelogs');
const { HealthProbeServer } = require('arsenal').network.probe;
const { reshapeExceptionError } = require('arsenal').errorUtils;

const MongoQueueProcessor = require('./MongoQueueProcessor');
const config = require('../../conf/Config');
const { initManagement } = require('../../lib/management/index');
const zookeeper = require('../../lib/clients/zookeeper');
const { zookeeperNamespace, zkStatePath } = require('../ingestion/constants');

const kafkaConfig = config.kafka;
const s3Config = config.s3;
const mongoProcessorConfig = config.extensions.mongoProcessor;
// TODO: consider whether we would want a separate mongo config
// for the consumer side
const mongoClientConfig = config.queuePopulator.mongo;
const ingestionServiceAuth = config.extensions.ingestion.auth;
const zkConfig = config.zookeeper;
const redisConfig = config.redis;
const { connectionString, autoCreateNamespace } = zkConfig;

const RESUME_NODE = 'scheduledResume';

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

const log = new werelogs.Logger('Backbeat:MongoProcessor:task');
werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

const activeProcessors = {};

function getStateZkPath() {
    return `${zookeeperNamespace}${zkStatePath}`;
}

/**
 * A scheduled resume is where consumers for a given site are paused and are
 * scheduled to be resumed at a later date.
 * If any scheduled resumes exist for a given site, the date is saved within
 * zookeeper. On startup, we schedule resume jobs to renew prior state.
 * If date in zookeeper has not expired, schedule the job. If date expired,
 * resume automatically for the site, and update the "status" node.
 * @param {MongoQueueProcessor} qp - queue processor instance
 * @param {Object} data - zookeeper state json data
 * @param {node-zookeeper-client.Client} zkClient - zookeeper client
 * @param {String} site - name of location site
 * @param {Function} cb - callback(error, updatedState) where updatedState is
 *   an optional, set as true if the schedule resume date has expired
 * @return {undefined}
 */
function checkAndApplyScheduleResume(qp, data, zkClient, site, cb) {
    const scheduleDate = new Date(data[RESUME_NODE]);
    const hasExpired = new Date() >= scheduleDate;
    if (hasExpired) {
        // if date expired, resume automatically for the site.
        // remove schedule resume data in zookeeper
        const path = `${getStateZkPath()}/${site}`;
        const d = JSON.stringify({ paused: false });
        return zkClient.setData(path, Buffer.from(d), err => {
            if (err) {
                log.fatal('could not set zookeeper status node', {
                    method: 'MongoProcessor:task',
                    zookeeperPath: path,
                    error: err.message,
                });
                return cb(err);
            }
            return cb(null, { paused: false });
        });
    }
    qp.scheduleResume(scheduleDate);
    return cb(null, { paused: true });
}

/**
 * On startup and when replication sites change, create necessary zookeeper
 * status node to save persistent state.
 * @param {QueueProcessor} qp - mongo queue processor instance
 * @param {node-zookeeper-client.Client} zkClient - zookeeper client
 * @param {String} site - replication site name
 * @param {Function} done - callback(error, status) where status is a boolean
 * @return {undefined}
 */
function setupZkSiteNode(qp, zkClient, site, done) {
    const path = `${getStateZkPath()}/${site}`;
    const data = JSON.stringify({ paused: { processor: false } });
    zkClient.create(path, Buffer.from(data), err => {
        if (err && err.name === 'NODE_EXISTS') {
            return zkClient.getData(path, (err, data) => {
                if (err) {
                    log.fatal('could not check site status in zookeeper',
                        { method: 'MongoProcessor:task',
                          zookeeperPath: path,
                          error: err.message });
                    return done(err);
                }
                let d;
                try {
                    d = JSON.parse(data.toString());
                } catch (e) {
                    log.fatal('error setting state for queue processor', {
                        method: 'MongoProcessor:task',
                        site,
                        error: reshapeExceptionError(e),
                    });
                    return done(e);
                }
                if (d[RESUME_NODE]) {
                    return checkAndApplyScheduleResume(qp, d, zkClient,
                        site, done);
                }
                return done(null, d);
            });
        }
        if (err) {
            log.fatal('could not setup zookeeper node', {
                method: 'MongoProcessor:task',
                zookeeperPath: path,
                error: err.message,
            });
            return done(err);
        }
        return done(null, { paused: false });
    });
}

function updateProcessors(zkClient, bootstrapList) {
    const active = Object.keys(activeProcessors);
    const update = bootstrapList.map(i => i.site);
    const allSites = [...new Set(active.concat(update))];

    async.each(allSites, (site, next) => {
        if (!update.includes(site)) {
            // remove processor, site is no longer active
            activeProcessors[site].removeZkState(err => {
                if (err) {
                    return next(err);
                }
                activeProcessors[site].stop(() => {});
                delete activeProcessors[site];
                return next();
            });
        } else if (!active.includes(site)) {
            // add new processor, site is new and requires setup
            const mqp = new MongoQueueProcessor(zkClient, kafkaConfig, s3Config,
                mongoProcessorConfig, mongoClientConfig, ingestionServiceAuth,
                redisConfig, site);
            activeProcessors[site] = mqp;
            setupZkSiteNode(mqp, zkClient, site, (err, data) => {
                if (err) {
                    return next(err);
                }
                mqp.start({ paused: data.paused });
                return next();
            });
        }
        // else, existing site and has already been setup
    }, err => {
        if (err) {
            process.exit(1);
        }
    });
}

function loadProcessors(zkClient) {
    let bootstrapList = config.getBootstrapList();
    config.on('bootstrap-list-update', () => {
        bootstrapList = config.getBootstrapList();

        updateProcessors(zkClient, bootstrapList);
    });

    // Start Processors for each site
    const siteNames = bootstrapList.map(i => i.site);
    async.each(siteNames, (site, next) => {
        const mqp = new MongoQueueProcessor(zkClient, kafkaConfig, s3Config,
            mongoProcessorConfig, mongoClientConfig, ingestionServiceAuth,
            redisConfig, site);
        activeProcessors[site] = mqp;
        return setupZkSiteNode(mqp, zkClient, site, (err, data) => {
            if (err) {
                return next(err);
            }
            mqp.start({ paused: data.paused });
            return next();
        });
    }, err => {
        if (err) {
            process.exit(1);
        }
    });
}

function loadHealthcheck() {
    healthServer.onReadyCheck(() => {
        let passed = true;
        Object.keys(activeProcessors).forEach(site => {
            if (!activeProcessors[site].isReady()) {
                passed = false;
                log.error(`MongoQueueProcessor for ${site} is not ready`);
            }
        });
        return passed;
    });
    log.info('Starting HealthProbe server');
    healthServer.start();
}

function loadManagementDatabase(zkClient) {
    initManagement({
        serviceName: 'md-ingestion',
        serviceAccount: ingestionServiceAuth.account,
    }, error => {
        if (error) {
            log.error('could not load management db', { error });
            setTimeout(() => {
                loadManagementDatabase(zkClient);
            }, 5000);
            return;
        }
        log.info('management init done');

        loadProcessors(zkClient);
        loadHealthcheck();
    });
}

const zkClient = zookeeper.createClient(connectionString, {
    autoCreateNamespace,
});
zkClient.connect();
zkClient.once('error', err => {
    log.fatal('error connecting to zookeeper', {
        error: err.message,
    });
    process.exit(1);
});
zkClient.once('ready', () => {
    zkClient.removeAllListeners('error');
    const path = getStateZkPath();
    zkClient.mkdirp(path, err => {
        if (err) {
            log.fatal('could not create path in zookeeper', {
                method: 'MongoProcessor:task',
                zookeeperPath: path,
                error: err.message,
            });
            process.exit(1);
        }
        return loadManagementDatabase(zkClient);
    });
});
