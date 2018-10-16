'use strict'; // eslint-disable-line

const async = require('async');
const werelogs = require('werelogs');

const QueueProcessor = require('./QueueProcessor');
const config = require('../../../conf/Config');
const { initManagement } = require('../../../lib/management/index');
const { applyBucketReplicationWorkflows } = require('../management');
const { HealthProbeServer } = require('arsenal').network.probe;
const { reshapeExceptionError } = require('arsenal').errorUtils;
const zookeeper = require('../../../lib/clients/zookeeper');
const { zookeeperReplicationNamespace, zkCRRStatePath } =
    require('../constants');

const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;
const redisConfig = config.redis;
const httpsConfig = config.https;
const internalHttpsConfig = config.internalHttps;
const mConfig = config.metrics;
const { connectionString, autoCreateNamespace } = zkConfig;

const RESUME_NODE = 'scheduledResume';

const log = new werelogs.Logger('Backbeat:QueueProcessor:task');
werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

const activeQProcessors = {};

function getCRRStateZkPath() {
    return `${zookeeperReplicationNamespace}${zkCRRStatePath}`;
}

/**
 * A scheduled resume is where consumers for a given site are paused and are
 * scheduled to be resumed at a later date.
 * If any scheduled resumes exist for a given site, the date is saved within
 * zookeeper. On startup, we schedule resume jobs to renew prior state.
 * If date in zookeeper has not expired, schedule the job. If date expired,
 * resume automatically for the site, and update the "status" node.
 * @param {QueueProcessor} qp - queue processor instance
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
        const path = `${getCRRStateZkPath()}/${site}`;
        const d = JSON.stringify({ paused: false });
        return zkClient.setData(path, Buffer.from(d), err => {
            if (err) {
                log.fatal('could not set zookeeper status node', {
                    method: 'QueueProcessor:task',
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
 * @param {QueueProcessor} qp - queue processor instance
 * @param {node-zookeeper-client.Client} zkClient - zookeeper client
 * @param {String} site - replication site name
 * @param {Function} done - callback(error, status) where status is a boolean
 * @return {undefined}
 */
function setupZkSiteNode(qp, zkClient, site, done) {
    const path = `${getCRRStateZkPath()}/${site}`;
    const data = JSON.stringify({ paused: false });
    zkClient.create(path, Buffer.from(data), err => {
        if (err && err.name === 'NODE_EXISTS') {
            return zkClient.getData(path, (err, data) => {
                if (err) {
                    log.fatal('could not check site status in zookeeper',
                        { method: 'QueueProcessor:task',
                          zookeeperPath: path,
                          error: err.message });
                    return done(err);
                }
                let d;
                try {
                    d = JSON.parse(data.toString());
                } catch (e) {
                    log.fatal('error setting state for queue processor', {
                        method: 'QueueProcessor:task',
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
                method: 'QueueProcessor:task',
                zookeeperPath: path,
                error: err.message,
            });
            return done(err);
        }
        return done(null, { paused: false });
    });
}

function initAndStart(zkClient) {
    initManagement({
        serviceName: 'replication',
        serviceAccount: sourceConfig.auth.account,
        applyBucketWorkflows: applyBucketReplicationWorkflows,
    }, error => {
        if (error) {
            log.error('could not load management db', { error });
            setTimeout(initAndStart, 5000);
            return;
        }
        log.info('management init done');

        const bootstrapList = config.getBootstrapList();

        const destConfig = Object.assign({}, repConfig.destination);
        destConfig.bootstrapList = bootstrapList;

        config.on('bootstrap-list-update', () => {
            destConfig.bootstrapList = config.getBootstrapList();

            const activeSites = Object.keys(activeQProcessors);
            const updatedSites = destConfig.bootstrapList.map(i => i.site);
            const allSites = [...new Set(activeSites.concat(updatedSites))];

            async.each(allSites, (site, next) => {
                if (updatedSites.includes(site)) {
                    if (!activeSites.includes(site)) {
                        const qp = new QueueProcessor(
                            zkClient, kafkaConfig, sourceConfig, destConfig,
                            repConfig, redisConfig, mConfig, httpsConfig,
                            internalHttpsConfig, site);
                        activeQProcessors[site] = qp;
                        setupZkSiteNode(qp, zkClient, site, (err, data) => {
                            if (err) {
                                return next(err);
                            }
                            qp.start({ paused: data.paused });
                            return next();
                        });
                    }
                } else {
                    // this site is no longer in bootstrapList
                    activeQProcessors[site].removeZkState(err => {
                        if (err) {
                            return next(err);
                        }
                        activeQProcessors[site].stop(() => {});
                        delete activeQProcessors[site];
                        return next();
                    });
                }
            }, err => {
                if (err) {
                    process.exit(1);
                }
            });
        });

        // Start QueueProcessor for each site
        const siteNames = bootstrapList.map(i => i.site);
        async.each(siteNames, (site, next) => {
            const qp = new QueueProcessor(zkClient,
                kafkaConfig, sourceConfig, destConfig, repConfig,
                redisConfig, mConfig, httpsConfig,
                internalHttpsConfig, site);
            activeQProcessors[site] = qp;
            return setupZkSiteNode(qp, zkClient, site, (err, data) => {
                if (err) {
                    return next(err);
                }
                qp.start({ paused: data.paused });
                return next();
            });
        }, err => {
            if (err) {
                // already logged error in prior function calls
                process.exit(1);
            }
        });
        healthServer.onReadyCheck(() => {
            let passed = true;
            Object.keys(activeQProcessors).forEach(site => {
                if (!activeQProcessors[site].isReady()) {
                    passed = false;
                    log.error(`QueueProcessor for ${site} is not ready!`);
                }
            });
            return passed;
        });
        log.info('Starting HealthProbe server');
        healthServer.start();
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
    // error occurred at startup trying to start internal clients,
    // fail immediately
    process.exit(1);
});
zkClient.once('ready', () => {
    zkClient.removeAllListeners('error');
    const path = getCRRStateZkPath();
    zkClient.mkdirp(path, err => {
        if (err) {
            log.fatal('could not create path in zookeeper', {
                method: 'QueueProcessor:task',
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
