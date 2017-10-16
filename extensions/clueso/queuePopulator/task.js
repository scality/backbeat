const async = require('async');
const schedule = require('node-schedule');
const AWS = require('aws-sdk');

const errors = require('arsenal').errors;
const config = require('../../../conf/Config');
const zkConfig = config.zookeeper;
const cluesoConfig = config.extensions.clueso;
const sourceConfig = config.extensions.clueso.source;
const QueuePopulator = require('./QueuePopulator');

const werelogs = require('werelogs');
werelogs.configure({ level: config.log.logLevel,
    dump: config.log.dumpLevel });

const log = new werelogs.Logger('Backbeat:Clueso:task');

const sparkBucket = 'METADATA';
const sparkMetadataPrefix = 'alluxio/landing/_spark_metadata/';

const s3port = cluesoConfig.s3.port === 80 ?
    '' : `:${cluesoConfig.s3.port}`;
// TODO: update to use credentials in unified way
const s3Client = new AWS.S3({
    endpoint: `${cluesoConfig.s3.transport}://` +
        `${cluesoConfig.s3.host}${s3port}`,
    credentials: new AWS.Credentials(cluesoConfig.s3.accessKeyId,
        cluesoConfig.s3.secretKeyId),
    s3ForcePathStyle: true,
    signatureVersion: 'v4',
});

// TODO: have this configurable
const postBatchParams = {
    file: 'file:///apps/spark-modules/clueso-1.0-SNAPSHOT-all.jar',
    className: 'com.scality.clueso.MetadataIngestionPipeline',
    name: 'Clueso Metadata Ingestion Pipeline',
    executorCores: 1,
    executorMemory: '512m',
    driverCores: 1,
    driverMemory: '512m',
    queue: 'default',
    args: ['/apps/spark-modules/application.conf'],
    conf: {
        'spark.driver.port': '38600',
        'spark.cores.max': '2',
        'spark.metrics.conf': '/apps/spark-modules/metrics.properties',
        'spark.sql.streaming.metricsEnabled': 'true',
    },
};

const WAIT_BEFORE_CHECKING_BATCH_STATE = 5000;

const LivyClient = require('arsenal').LivyClient;
const useHttps = !!cluesoConfig.livy.transport.https;
const livyClient = new LivyClient(cluesoConfig.livy.host,
    cluesoConfig.livy.port, log, useHttps);

/* eslint-disable no-param-reassign */
function queueBatch(queuePopulator, taskState) {
    if (taskState.batchInProgress) {
        log.warn('skipping clueso batch: ' +
                 'previous one still in progress');
        return undefined;
    }
    log.debug('start queueing clueso batch');
    taskState.batchInProgress = true;
    queuePopulator.processAllLogEntries(
        { maxRead: cluesoConfig.queuePopulator.batchMaxRead },
        (err, counters) => {
            if (err) {
                log.error('an error occurred during clueso',
                          { error: err, errorStack: err.stack });
            } else {
                const logFunc = (counters.readRecords > 0 ?
                                 log.info : log.debug)
                          .bind(log);
                logFunc('clueso batch finished', { counters });
            }
            taskState.batchInProgress = false;
        });
    return undefined;
}

function cleanUp(batchId) {
    if (batchId !== undefined) {
        log.info('attempting to delete livy batch job before exiting');
        return livyClient.deleteBatch(batchId, err => {
            if (err) {
                log.info('error deleting livy batch job',
                { error: err, batchId });
            }
            log.info('deleted livy batch', { batchId });
            throw new Error('backbeat-clueso exiting');
        });
    }
    throw new Error('backbeat-clueso exiting');
}

/* eslint-enable no-param-reassign */

const queuePopulator = new QueuePopulator(zkConfig, sourceConfig,
                                          cluesoConfig, config.log);

async.waterfall([
    done => {
        log.info('attempting to create METADATA bucket');
        s3Client.createBucket({ Bucket: sparkBucket }, err => {
            // note, will not get this error with legacy AWS behavior
            if (err && err.code === 'BucketAlreadyOwnedByYou') {
                log.info('bucket METADATA already existed');
                return done();
            }
            if (!err) {
                log.info('bucket METADATA created');
            }
            return done(err);
        });
    },
    done => {
        log.info('cleaning up spark metadata');
        s3Client.listObjects({ Bucket: sparkBucket,
            Prefix: sparkMetadataPrefix }, (err, res) => {
            if (err) {
                log.info('err listing objects in METADATA bucket');
                return done(err);
            }
            const keysToDelete = res.Contents.map(item => item.Key);
            log.info('keys to delete in METADATA bucket', { keysToDelete });
            if (keysToDelete.length === 0) {
                // no spark metadata files to clean up
                return done();
            }
            return async.eachLimit(keysToDelete, 10, (key, next) => {
                return s3Client.deleteObject({ Bucket: sparkBucket, Key: key },
                err => {
                    if (err) {
                        log.info('error deleting spark metadata',
                        { error: err });
                        return next(err);
                    }
                    return next();
                });
            }, done);
        });
    },
    done => {
        log.info('submitting streaming job to livy/spark');
        livyClient.postBatch(postBatchParams, (err, res) => {
            if (err) {
                return done(err);
            }
            log.info('response from livy on creating streaming job',
                { response: res });
            return done(null, res.id);
        });
    },
    (batchId, done) => {
        log.info('checking streaming batch job state');
        setTimeout(() => {
            livyClient.getSessionOrBatchState('batch', batchId, (err, res) => {
                if (err) {
                    return done(err, batchId);
                }
                if (res.state !== 'running') {
                    log.info('batch state is not running',
                    { batchState: res.state });
                    const error = errors.InternalError
                    .customizeDescription('Livy streaming job failed to start');
                    return done(error, batchId);
                }
                log.info('batch state is running', { batchState: res.state });
                return done(null, batchId);
            });
        }, WAIT_BEFORE_CHECKING_BATCH_STATE);
    },
    (batchId, done) => {
        queuePopulator.open(err => {
            done(err, batchId);
        });
    },
    (batchId, done) => {
        const taskState = {
            batchInProgress: false,
        };
        schedule.scheduleJob(cluesoConfig.queuePopulator.cronRule, () => {
            queueBatch(queuePopulator, taskState);
        });
        done(null, batchId);
    },
], (err, batchId) => {
    if (err) {
        log.error('error during clueso initialization',
                  { error: err });
        return cleanUp(batchId);
    }
    process.once('SIGINT', cleanUp.bind(this, batchId));
    process.once('SIGHUP', cleanUp.bind(this, batchId));
    process.once('SIGQUIT', cleanUp.bind(this, batchId));
    process.once('SIGTERM', cleanUp.bind(this, batchId));
    return undefined;
});
