const async = require('async');
const schedule = require('node-schedule');
const zookeeper = require('node-zookeeper-client');

const werelogs = require('werelogs');
const { HealthProbeServer } = require('arsenal').network.probe;

const IngestionPopulator = require('../lib/queuePopulator/IngestionPopulator');
const config = require('../conf/Config');
const { initManagement } = require('../lib/management/index');

const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const ingestionExtConfigs = config.extensions.ingestion;
const qpConfig = config.queuePopulator;
const mConfig = config.metrics;
const rConfig = config.redis;
const s3Config = config.s3;

const log = new werelogs.Logger('Backbeat:IngestionPopulator');

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

let scheduler;

/* eslint-disable no-param-reassign */
function queueBatch(ingestionPopulator, taskState, log) {
    if (taskState.batchInProgress) {
        log.warn('skipping ingestion batch: previous one still in progress');
        return undefined;
    }
    log.debug('start queueing ingestion batch');
    taskState.batchInProgress = true;

    return ingestionPopulator.applyUpdates(err => {
        if (err) {
            log.fatal('failed to update ingestion readers', {
                method: 'IngestionPopulator.applyUpdates',
                error: err,
            });
            scheduler.cancel();
            process.exit(1);
        }
        log.debug('updated active ingestion readers');
        const maxRead = qpConfig.batchMaxRead;
        ingestionPopulator.processLogEntries({ maxRead }, err => {
            taskState.batchInProgress = false;
            if (err) {
                log.error('an error occurred during ingestion', {
                    method: 'IngestionPopulator::task.queueBatch',
                    error: err,
                });
                // exit process and let Kubernetes respawn the pod
                process.exit(1);
            }
        });
        return undefined;
    });
}
/* eslint-enable no-param-reassign */

const ingestionPopulator = new IngestionPopulator(zkConfig, kafkaConfig,
    qpConfig, mConfig, rConfig, ingestionExtConfigs, s3Config);

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

function initAndStart() {
    // TODO: change to using ingestion service account
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

        async.series([
            done => ingestionPopulator.open(done),
            done => {
                const taskState = {
                    batchInProgress: false,
                };
                scheduler = schedule.scheduleJob(ingestionExtConfigs.cronRule,
                    () => {
                        queueBatch(ingestionPopulator, taskState, log);
                    });
                done();
            },
        ], err => {
            if (err) {
                log.fatal('error during ingestion populator initialization', {
                    method: 'IngestionPopulator::task',
                    error: err,
                });
                process.exit(1);
            }
            healthServer.onReadyCheck(() => {
                const state = ingestionPopulator.zkStatus();
                if (state.code === zookeeper.State.SYNC_CONNECTED.code) {
                    return true;
                }
                log.error(`Zookeeper is not connected! ${state}`);
                return false;
            });
            log.info('Starting HealthProbe server');
            healthServer.start();
        });
    });
}

initAndStart();

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    ingestionPopulator.close(() => {
        scheduler.cancel();
        process.exit(0);
    });
});
