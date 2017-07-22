const async = require('async');
const schedule = require('node-schedule');

const Logger = require('werelogs').Logger;

const config = require('../../../conf/Config');
const zkConfig = config.zookeeper;
const repConfig = config.extensions.replication;
const sourceConfig = config.extensions.replication.source;
const QueuePopulator = require('./QueuePopulator');

const logger = new Logger('Backbeat:Replication:task',
                          { level: config.log.logLevel,
                            dump: config.log.dumpLevel });
const log = logger.newRequestLogger();

/* eslint-disable no-param-reassign */
function queueBatch(queuePopulator, taskState) {
    if (taskState.batchInProgress) {
        log.warn('skipping replication batch: ' +
                 'previous one still in progress');
        return undefined;
    }
    log.debug('start queueing replication batch');
    taskState.batchInProgress = true;
    queuePopulator.processAllLogEntries(
        { maxRead: repConfig.queuePopulator.batchMaxRead },
        (err, counters) => {
            if (err) {
                if (!err.ServiceUnavailable) {
                    log.error('an error occurred during replication',
                              { error: err, errorStack: err.stack });
                }
            } else {
                const logFunc = (counters.readRecords > 0 ?
                                 log.info : log.debug)
                          .bind(log);
                logFunc('replication batch finished', { counters });
            }
            taskState.batchInProgress = false;
        });
    return undefined;
}
/* eslint-enable no-param-reassign */

const queuePopulator = new QueuePopulator(zkConfig, sourceConfig,
                                          repConfig, config.log);

async.waterfall([
    done => {
        queuePopulator.open(done);
    },
    done => {
        const taskState = {
            batchInProgress: false,
        };
        schedule.scheduleJob(repConfig.queuePopulator.cronRule, () => {
            queueBatch(queuePopulator, taskState);
        });
        done();
    },
], err => {
    if (err) {
        log.error('error during queue populator initialization',
                  { error: err });
        process.exit(1);
    }
});

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    queuePopulator.close(() => {
        process.exit(0);
    });
});
