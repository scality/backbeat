'use strict'; // eslint-disable-line

const async = require('async');
const werelogs = require('werelogs');
const { HealthProbeServer } = require('arsenal').network.probe;

const MongoQueueProcessor = require('./MongoQueueProcessor');
const config = require('../../conf/Config');
const { initManagement } = require('../../lib/management/index');

const kafkaConfig = config.kafka;
const mongoProcessorConfig = config.extensions.mongoProcessor;
// TODO: consider whether we would want a separate mongo config
// for the consumer side
const mongoClientConfig = config.queuePopulator.mongo;

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

const log = new werelogs.Logger('Backbeat:MongoProcessor:task');
werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

const activeProcessors = {};

function initAndStart() {
    // NOTE: using replication service account
    const sourceConfig = config.extensions.replication.source;
    initManagement({
        serviceName: 'replication',
        serviceAccount: sourceConfig.auth.account,
    }, error => {
        if (error) {
            log.error('could not load management db', { error });
            setTimeout(initAndStart, 5000);
            return;
        }
        log.info('management init done');

        let bootstrapList = config.getBootstrapList();
        config.on('bootstrap-list-update', () => {
            bootstrapList = config.getBootstrapList();

            const active = Object.keys(activeProcessors);
            const update = bootstrapList.map(i => i.site);
            const allSites = [...new Set(active.concat(update))];

            async.each(allSites, (site, next) => {
                if (update.includes(site)) {
                    if (!active.includes(site)) {
                        // new site to setup
                        const mqp = new MongoQueueProcessor(kafkaConfig,
                            mongoProcessorConfig, mongoClientConfig, site);
                        mqp.start();
                        activeProcessors[site] = mqp;
                    }
                } else {
                    // site no longer in bootstrapList
                    activeProcessors[site].stop(() => {});
                    delete activeProcessors[site];
                }
                return next();
            }, err => {
                if (err) {
                    process.exit(1);
                }
            });
        });

        // Start Processors for each site
        const siteNames = bootstrapList.map(i => i.site);
        async.each(siteNames, (site, next) => {
            const mqp = new MongoQueueProcessor(kafkaConfig,
                mongoProcessorConfig, mongoClientConfig, site);
            mqp.start();
            activeProcessors[site] = mqp;
            return next();
        }, err => {
            if (err) {
                process.exit(1);
            }
        });
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
    });
}

initAndStart();
