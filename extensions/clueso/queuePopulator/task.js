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
        'spark.sql.parquet.cacheMetadata': 'false' },
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
/* eslint-enable no-param-reassign */

const queuePopulator = new QueuePopulator(zkConfig, sourceConfig,
                                          cluesoConfig, config.log);

async.waterfall([
    done => {
        log.info('attempting to create METADATA bucket');
        s3Client.createBucket({ Bucket: 'METADATA' }, err => {
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
                    return done(err);
                }
                if (res.state !== 'running') {
                    log.info('batch state is not running',
                    { batchState: res.state });
                    const error = errors.InternalError
                    .customizeDescription('Livy streaming job failed to start');
                    // TODO: delete the batch job
                    return done(error);
                }
                log.info('batch state is running', { batchState: res.state });
                return done();
            });
        }, WAIT_BEFORE_CHECKING_BATCH_STATE);
    },
    done => {
        queuePopulator.open(done);
    },
    done => {
        const taskState = {
            batchInProgress: false,
        };
        schedule.scheduleJob(cluesoConfig.queuePopulator.cronRule, () => {
            queueBatch(queuePopulator, taskState);
        });
        done();
    },
], err => {
    if (err) {
        log.error('error during clueso initialization',
                  { error: err });
        process.exit(1);
    }
});
