const async = require('async');
const schedule = require('node-schedule');

const werelogs = require('werelogs');

const config = require('../conf/Config');
const zkConfig = config.zookeeper;
const extConfigs = config.extensions;
const qpConfig = config.queuePopulator;
const QueuePopulator = require('../lib/queuePopulator/QueuePopulator');
const RaftLogEntry = require('../extensions/replication/utils/RaftLogEntry');
const ObjectQueueEntry = require('../extensions/replication/utils/ObjectQueueEntry');
const BackbeatProducer = require('../lib/BackbeatProducer');

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
const backbeatProducer = new BackbeatProducer({
    zookeeper: { connectionString: 'localhost:2181/backbeat' },
    topic: 'backbeat-replication',
});
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
                const objectKey = object.key;
                return queuePopulator.getObjectMetadata(bucketName, object, (err, res) => {
                    console.log('we got data', objectKey);
                    return cb(null, { res, objectKey });
                });
            }, (err, res) => {
                console.log(typeof res);
                console.log(typeof res);
                console.log(typeof res);
                console.log(typeof res);
                console.log(typeof res);
                console.log(typeof res);
                console.log(res);
                console.log('LENGTH OF RES', res.length);
                console.log('LENGTH OF RES', res.length);
                console.log('LENGTH OF RES', res.length);
                console.log('LENGTH OF RES', res.length);
                console.log('LENGTH OF RES', res.length);
                console.log('LENGTH OF RES', res.length);
                if (res.length > 0) {
                    console.log('bucketName, ', bucketName);
                    console.log('objectKey', res[0].objectKey);
                    const queueEntry = new RaftLogEntry();
                    console.log('THIS IS THE QUEUE ENTRY', queueEntry.createPutEntry(bucketName, res[0].objectKey, res[0].res));
                    backbeatProducer.send([(queueEntry.createPutEntry(bucketName, res[0].objectKey, res[0].res))], () => {});
                }
                return cb(err, { bucket: bucketName, objects: res });
            });
        }, err => {
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
