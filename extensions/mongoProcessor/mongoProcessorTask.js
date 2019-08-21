'use strict'; // eslint-disable-line

const werelogs = require('werelogs');
const { HealthProbeServer } = require('arsenal').network.probe;

const MongoQueueProcessor = require('./MongoQueueProcessor');
const config = require('../../lib/Config');
const { initManagement } = require('../../lib/management/index');

const kafkaConfig = config.kafka;
const mConfig = config.metrics;
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

const mqp = new MongoQueueProcessor(kafkaConfig, mongoProcessorConfig,
    mongoClientConfig, mConfig);

function loadHealthcheck() {
    healthServer.onReadyCheck(() => {
        let passed = true;
        if (!mqp.isReady()) {
            passed = false;
            log.error('MongoQueueProcessor is not ready');
        }
        return passed;
    });
    log.info('Starting HealthProbe server');
    healthServer.start();
}

function loadManagementDatabase() {
    const ingestionServiceAuth = config.extensions.ingestion.auth;
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

        mqp.start();

        loadHealthcheck();
    });
}

loadManagementDatabase();

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    mqp.stop(error => {
        if (error) {
            log.error('failed to exit properly', {
                error,
            });
            process.exit(1);
        }
        process.exit(0);
    });
});
