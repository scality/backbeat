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
const QueuePopulator = require('../lib/queuePopulator/IngestionProducer');
const queueBatch = require('./utils').queueBatch;

const log = new werelogs.Logger('Backbeat:QueuePopulator');

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

const queuePopulator = new QueuePopulator(zkConfig, kafkaConfig,
    qpConfig, mConfig,
    rConfig, extConfigs);

async.waterfall([
    done => queuePopulator.open(done),
    done => queuePopulator.getBuckets(done),
    (bucketList, done) => queuePopulator.getBucketMd(bucketList, done),
    (bucketList, done) => queuePopulator.getBucketObjects(bucketList, done),
    (bucketList, done) => {
        queuePopulator.getBucketObjectsMetadata(bucketList, done);
    },
    done => {
        const taskState = {
            batchInProgress: false,
        };
        schedule.scheduleJob(qpConfig.cronRule, () => {
            queueBatch(queuePopulator, taskState, qpConfig, log);
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
