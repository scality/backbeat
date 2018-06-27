'use strict'; // eslint-disable-line

const async = require('async');
const werelogs = require('werelogs');

const QueueProcessor = require('./QueueProcessor');
const config = require('../../../conf/Config');
const { initManagement } = require('../../../lib/management/index');
const { applyBucketReplicationWorkflows } = require('../management');
const { HealthProbeServer } = require('arsenal').network.probe;

const MetricsProducer = require('../../../lib/MetricsProducer');
const zookeeper = require('../../../lib/clients/zookeeper');

const { zookeeperReplicationNamespace } = require('../constants');
const ZK_CRR_STATE_PATH = '/state';

const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;
const mConfig = config.metrics;
const redisConfig = config.redis;

const log = new werelogs.Logger('Backbeat:QueueProcessor:task');
werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

function getCRRStateZkPath() {
    return `${zookeeperReplicationNamespace}${ZK_CRR_STATE_PATH}`;
}

/**
 * On startup and when replication sites change, create necessary zookeeper
 * status node to save persistent state.
 * @param {node-zookeeper-client.Client} zkClient - zookeeper client
 * @param {String} site - replication site name
 * @param {Function} done - callback(error, status) where status is a boolean
 * @return {undefined}
 */
function setupZkSiteNode(zkClient, site, done) {
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
                try {
                    const paused = JSON.parse(data.toString()).paused;
                    return done(null, paused);
                } catch (e) {
                    log.fatal('error setting state for queue processor', {
                        method: 'QueueProcessor:task',
                        site,
                        error: e,
                    });
                    return done(e);
                }
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
        // paused is false
        return done(null, false);
    });
}

const metricsProducer = new MetricsProducer(kafkaConfig, mConfig);
let zkClient;
async.series([
    done => metricsProducer.setupProducer(err => {
        if (err) {
            log.fatal('error starting metrics producer for queue ' +
            'processor', {
                error: err,
                method: 'MetricsProducer::setupProducer',
            });
        }
        return done(err);
    }),
    done => {
        const { connectionString, autoCreateNamespace } = zkConfig;
        log.info('opening zookeeper connection for replication processors');
        zkClient = zookeeper.createClient(connectionString, {
            autoCreateNamespace,
        });
        zkClient.connect();
        zkClient.once('error', err => {
            log.fatal('error connecting to zookeeper', {
                error: err.message,
            });
            return done(err);
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
                    return done(err);
                }
                return done();
            });
        });
    },
], err => {
    if (err) {
        // error occurred at startup trying to start internal clients,
        // fail immediately
        process.exit(1);
    }
    const activeQProcessors = {};

    function initAndStart() {
        initManagement({
            serviceName: 'replication',
            serviceAccount: sourceConfig.auth.account,
            applyBucketWorkflows: applyBucketReplicationWorkflows,
        }, error => {
            if (error) {
                log.error('could not load management db',
                    { error: error.message });
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
                            activeQProcessors[site] = new QueueProcessor(
                                zkClient, kafkaConfig, sourceConfig, destConfig,
                                repConfig, redisConfig, metricsProducer, site);
                            setupZkSiteNode(zkClient, site, (err, paused) => {
                                if (err) {
                                    return next(err);
                                }
                                activeQProcessors[site].start({ paused });
                                return next();
                            });
                        }
                    } else {
                        // this site is no longer in bootstrapList
                        activeQProcessors[site].stop(() => {});
                        delete activeQProcessors[site];
                        next();
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
                activeQProcessors[site] = new QueueProcessor(zkClient,
                    kafkaConfig, sourceConfig, destConfig, repConfig,
                    redisConfig, metricsProducer, site);
                return setupZkSiteNode(zkClient, site, (err, paused) => {
                    if (err) {
                        return next(err);
                    }
                    activeQProcessors[site].start({ paused });
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
    return initAndStart();
});
