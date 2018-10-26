const async = require('async');
const schedule = require('node-schedule');

const werelogs = require('werelogs');

const config = require('../conf/Config');
const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const extConfigs = config.extensions;
const qpConfig = config.queuePopulator;
const mConfig = config.metrics;
const rConfig = config.redis;
const s3Config = config.s3;
const IngestionPopulator = require('../lib/queuePopulator/IngestionPopulator');
const zookeeper = require('node-zookeeper-client');

const { HealthProbeServer } = require('arsenal').network.probe;
const log = new werelogs.Logger('Backbeat:IngestionPopulator');

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

/* eslint-disable no-param-reassign */
function queueBatch(queuePopulator, taskState, qConfig, log) {
    if (taskState.batchInProgress) {
        log.warn('skipping replication batch: previous one still in progress');
        return undefined;
    }
    log.debug('start queueing replication batch');
    taskState.batchInProgress = true;
    const maxRead = qpConfig.batchMaxRead;
    queuePopulator.processLogEntries({ maxRead }, err => {
        taskState.batchInProgress = false;
        if (err) {
            log.error('an error occurred during replication', {
                method: 'QueuePopulator::task.queueBatch',
                error: err,
            });
        }
    });
    return undefined;
}
/* eslint-enable no-param-reassign */

const ingestionPopulator = new IngestionPopulator(zkConfig, kafkaConfig,
    qpConfig, mConfig,
    rConfig, extConfigs, s3Config);

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});


async.waterfall([
    done => ingestionPopulator.open(done),
    done => {
        const taskState = {
            batchInProgress: false,
        };
        schedule.scheduleJob(qpConfig.cronRule, () => {
            queueBatch(ingestionPopulator, taskState, qpConfig, log);
        });
        done();
    },
    done => {
        healthServer.onReadyCheck(log => {
            const state = ingestionPopulator.zkStatus();
            if (state.code === zookeeper.State.SYNC_CONNECTED.code) {
                return true;
            }
            log.error(`Zookeeper is not connected! ${state}`);
            return false;
        });
        log.info('Starting HealthProbe server');
        healthServer.start();
        done();
    },
], err => {
    if (err) {
        log.error('error during queue populator initialization', {
            method: 'QueuePopulator::task',
            error: err,
        });
        process.exit(1);
    }
});

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    ingestionPopulator.close(() => {
        process.exit(0);
    });
});
