const async = require('async');
const schedule = require('node-schedule');

const werelogs = require('werelogs');

const config = require('../conf/Config');
const zkConfig = config.zookeeper;
const extConfigs = config.extensions;
const qpConfig = config.queuePopulator;
const QueuePopulator = require('../lib/queuePopulator/QueuePopulator');

const log = new werelogs.Logger('Backbeat:QueuePopulator');

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

/* eslint-disable no-param-reassign */
function queueBatch(queuePopulator, taskState) {
    if (taskState.batchInProgress) {
        log.warn('skipping replication batch: previous one still in progress');
        return undefined;
    }
    log.debug('start queueing replication batch');
    taskState.batchInProgress = true;
    const maxRead = qpConfig.batchMaxRead;
    queuePopulator.processAllLogEntries({ maxRead }, (err, counters) => {
        taskState.batchInProgress = false;
        if (err) {
            log.error('an error occurred during replication', {
                method: 'QueuePopulator::task.queueBatch',
                error: err,
            });
            return undefined;
        }
        const logFunc = (counters.some(counter => counter.readRecords > 0) ?
            log.info : log.debug).bind(log);
        logFunc('replication batch finished', { counters });
        return undefined;
    });
    return undefined;
}
/* eslint-enable no-param-reassign */

const queuePopulator = new QueuePopulator(zkConfig, qpConfig, extConfigs);

console.log(queuePopulator);

async.waterfall([
    done => queuePopulator.open(done),
    done => queuePopulator.getRaftSessionBuckets(done, res => {
        return done(null, res);
    }),
    (bucketList, done) => queuePopulator.getRaftSessionBucketObjects(bucketList, done),
    (bucketList, done) => {
        console.log('getting list for each bucket');
        return async.mapLimit(bucketList, 1, (bucket, cb) => {
            const bucketName = bucket.bucket;
            return async.mapLimit(bucket.objects, 10, (object, cb) => {
                console.log('MAPPING');
                return queuePopulator.getObjectMetadata(bucketName, object, (err, res) => {
                    console.log('we got data');
                    return cb(null, res);
                });
            }, (err, res) => {
                return cb(err, { bucket: bucketName, objects: res });
            });
        }, (err, res) => {
            console.log('we got this far');
            console.log(res);
            return done(err);
        });
    },
    done => {
        console.log('scheduling job');
        const taskState = {
            batchInProgress: false,
        };
        schedule.scheduleJob(qpConfig.cronRule, () => {
            console.log('queue populator');
            console.log('task state');
            queueBatch(queuePopulator, taskState);
        });
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
