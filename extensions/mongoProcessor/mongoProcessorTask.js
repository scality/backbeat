'use strict'; // eslint-disable-line

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
const ingestionServiceAuth = config.extensions.ingestion.auth;
const s3Config = config.s3;

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

const log = new werelogs.Logger('Backbeat:MongoProcessor:task');
werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

const activeProcessors = {};

function updateProcessors(bootstrapList) {
    const active = Object.keys(activeProcessors);
    const update = bootstrapList.map(i => i.site);
    const allSites = [...new Set(active.concat(update))];

    allSites.forEach(site => {
        if (!update.includes(site)) {
            // remove processor, site is no longer active
            activeProcessors[site].stop(() => {});
            delete activeProcessors[site];
        } else if (!active.includes(site)) {
            // add new processor, site is new and requires setup
            const mqp = new MongoQueueProcessor(kafkaConfig, s3Config,
                mongoProcessorConfig, mongoClientConfig,
                ingestionServiceAuth, site);
            mqp.start();
            activeProcessors[site] = mqp;
        }
        // else, existing site and has already been setup
    });
}

function loadProcessors() {
    let bootstrapList = config.getBootstrapList();
    config.on('bootstrap-list-update', () => {
        bootstrapList = config.getBootstrapList();

        updateProcessors(bootstrapList);
    });

    // Start Processors for each site
    const siteNames = bootstrapList.map(i => i.site);
    siteNames.forEach(site => {
        const mqp = new MongoQueueProcessor(kafkaConfig, s3Config,
            mongoProcessorConfig, mongoClientConfig,
            ingestionServiceAuth, site);
        mqp.start();
        activeProcessors[site] = mqp;
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

function loadManagementDatabase() {
    initManagement({
        serviceName: 'md-ingestion',
        serviceAccount: ingestionServiceAuth.account,
    }, error => {
        if (error) {
            log.error('could not load management db', { error });
            setTimeout(loadManagementDatabase, 5000);
            return;
        }
        log.info('management init done');

        loadProcessors();
        loadHealthcheck();
    });
}

loadManagementDatabase();
