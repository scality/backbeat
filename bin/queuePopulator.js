const async = require('async');
const schedule = require('node-schedule');

const werelogs = require('werelogs');

const config = require('../conf/Config');
const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const extConfigs = config.extensions;
const qpConfig = config.queuePopulator;
const httpsConfig = config.internalHttps;
const mConfig = config.metrics;
const rConfig = config.redis;
const QueuePopulator = require('../lib/queuePopulator/QueuePopulator');

const { HealthProbeServer } = require('arsenal').network.probe;
const log = new werelogs.Logger('Backbeat:QueuePopulator');

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

/* eslint-disable no-param-reassign */
function queueBatch(queuePopulator, taskState) {
    if (taskState.batchInProgress) {
        log.debug('skipping replication batch: previous one still in progress');
        return undefined;
    }
    const onTimeout = () => {
        // reset the flag to allow a new batch to start in case the
        // previous batch timed out
        taskState.batchInProgress = false;
    };
    log.debug('start queueing replication batch');
    taskState.batchInProgress = true;
    const maxRead = qpConfig.batchMaxRead;
    queuePopulator.processAllLogEntries({ maxRead, onTimeout }, err => {
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

const queuePopulator = new QueuePopulator(
    zkConfig, kafkaConfig, qpConfig, httpsConfig, mConfig, rConfig, extConfigs);

const healthServer = new HealthProbeServer({
    bindAddress: config.healthcheckServer.bindAddress,
    port: config.healthcheckServer.port,
});

async.waterfall([
    done => queuePopulator.open(done),
    done => {
        const taskState = {
            batchInProgress: false,
        };
        schedule.scheduleJob(qpConfig.cronRule, () => {
            queueBatch(queuePopulator, taskState);
        });
        done();
    },
    done => {
        healthServer.onReadyCheck(log => {
            const state = queuePopulator.zkStatus();
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
    queuePopulator.close(() => {
        process.exit(0);
    });
});
