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
const IngestionProducer = require('../lib/queuePopulator/IngestionProducer');
const queueBatch = require('./utils').queueBatch;

const log = new werelogs.Logger('Backbeat:IngestionProducer');

werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

const ingestionProducer = new IngestionProducer(zkConfig, kafkaConfig,
    qpConfig, mConfig,
    rConfig, extConfigs);

async.waterfall([
    done => ingestionProducer.open(done),
    done => ingestionProducer.getBuckets(done),
    (bucketList, done) => ingestionProducer.getBucketMd(bucketList, done),
    (bucketList, done) => ingestionProducer.getBucketObjects(bucketList, done),
    (bucketList, done) => {
        ingestionProducer.getBucketObjectsMetadata(bucketList, done);
    },
    done => {
        const taskState = {
            batchInProgress: false,
        };
        schedule.scheduleJob(qpConfig.cronRule, () => {
            queueBatch(ingestionProducer, taskState, qpConfig, log);
        });
        done();
    },
], err => {
    if (err) {
        log.error('error during queue populator initialization', {
            method: 'IngestionPopulator::task',
            error: err,
        });
        process.exit(1);
    }
});

process.on('SIGTERM', () => {
    log.info('received SIGTERM, exiting');
    ingestionProducer.close(() => {
        process.exit(0);
    });
});
