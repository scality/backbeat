'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const QueueProcessor = require('./QueueProcessor');
const config = require('../../../conf/Config');
const { initManagement } = require('../../../lib/management/index');
const { applyBucketReplicationWorkflows } = require('../management');

const zkConfig = config.zookeeper;
const MetricsProducer = require('../../../lib/MetricsProducer');

const kafkaConfig = config.kafka;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;
const mConfig = config.metrics;
const redisConfig = config.redis;

const log = new werelogs.Logger('Backbeat:QueueProcessor:task');
werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

const activeQProcessors = {};

const metricsProducer = new MetricsProducer(kafkaConfig, mConfig);
metricsProducer.setupProducer(err => {
    if (err) {
        log.error('error starting metrics producer for queue processor', {
            error: err,
            method: 'MetricsProducer::setupProducer',
        });
        return undefined;
    }
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

                allSites.forEach(site => {
                    if (updatedSites.includes(site)) {
                        if (!activeSites.includes(site)) {
                            activeQProcessors[site] = new QueueProcessor(
                                zkConfig, kafkaConfig, sourceConfig, destConfig,
                                repConfig, redisConfig, metricsProducer, site);
                            activeQProcessors[site].start();
                        }
                    } else {
                        // this site is no longer in bootstrapList
                        activeQProcessors[site].stop(() => {});
                        delete activeQProcessors[site];
                    }
                });
            });

            // Start QueueProcessor for each site
            const siteNames = bootstrapList.map(i => i.site);
            siteNames.forEach(site => {
                activeQProcessors[site] = new QueueProcessor(zkConfig,
                    kafkaConfig, sourceConfig, destConfig, repConfig,
                    redisConfig, metricsProducer, site);
                activeQProcessors[site].start();
            });
        });
    }
    return initAndStart();
});
