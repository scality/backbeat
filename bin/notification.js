const async = require('async');
const schedule = require('node-schedule');

const werelogs = require('werelogs');

const config = require('../conf/Config');
const zkConfig = config.zookeeper;
const kafkaConfig = config.kafka;
const httpsConfig = config.internalHttps;
const mConfig = config.metrics;
const rConfig = config.redis;
const qpConfig = config.queuePopulator;
const extConfigs = config.extensions;

const QueuePopulator = require('../lib/queuePopulator/QueuePopulator');

const log = new werelogs.Logger('Backbeat:NotificationQueuePopulator');

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

/* eslint-disable no-param-reassign */
function queueBatch(queuePopulator, taskState) {
    if (taskState.batchInProgress) {
        log.debug('skipping notification batch: previous one still in progress');
        return undefined;
    }
    log.debug('start queueing notification batch');
    taskState.batchInProgress = true;
    const maxRead = qpConfig.batchMaxRead;
    queuePopulator.processAllLogEntries({ maxRead }, err => {
        taskState.batchInProgress = false;
        if (err) {
            log.error('an error occurred during notification', {
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
], err => {
    if (err) {
        log.error('error during queue populator initialization', {
            method: 'bin.notification',
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
