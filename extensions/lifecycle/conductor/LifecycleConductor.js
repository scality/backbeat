'use strict'; // eslint-disable-line

const async = require('async');
const schedule = require('node-schedule');

const Logger = require('werelogs').Logger;

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const zookeeperHelper = require('../../../lib/clients/zookeeper');
const ProvisionDispatcher =
          require('../../../lib/provisioning/ProvisionDispatcher');

const DEFAULT_CRON_RULE = '* * * * *';
const DEFAULT_CONCURRENCY = 10;

/**
 * @class LifecycleConductor
 *
 * @classdesc Background task that periodically reads the lifecycled
 * buckets list on Zookeeper and creates bucket listing tasks on
 * Kafka.
 */
class LifecycleConductor {

    /**
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {String} zkConfig.connectionString - zookeeper connection string
     *  as "host:port[/chroot]"
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} lcConfig - lifecycle configuration object
     * @param {String} lcConfig.bucketTasksTopic - lifecycle
     *   bucket tasks topic name
     * @param {String} lcConfig.conductor - config object specific to
     *   lifecycle conductor
     * @param {String} [lcConfig.conductor.cronRule="* * * * *"] -
     *   cron rule for bucket processing periodic task
     * @param {Number} [lcConfig.conductor.concurrency=10] - maximum
     *   number of concurrent bucket-to-kafka operations allowed
     */
    constructor(zkConfig, kafkaConfig, lcConfig) {
        this.zkConfig = zkConfig;
        this.kafkaConfig = kafkaConfig;
        this.lcConfig = lcConfig;
        this._cronRule =
            this.lcConfig.conductor.cronRule || DEFAULT_CRON_RULE;
        this._concurrency =
            this.lcConfig.conductor.concurrency || DEFAULT_CONCURRENCY;
        this._producer = null;
        this._zkClient = null;
        this._started = false;
        this._isActive = false;
        this._jobDispatcher = null;
        this._cronJob = null;

        this.logger = new Logger('Backbeat:Lifecycle:Conductor');
    }

    getBucketsZkPath() {
        return `${this.lcConfig.zookeeperPath}/data/buckets`;
    }

    getJobDispatcherZkPath() {
        return `${this.lcConfig.zookeeperPath}/data/conductor-job-dispatcher`;
    }

    getQueuedBucketsZkPath() {
        return `${this.lcConfig.zookeeperPath}/run/queuedBuckets`;
    }

    initZkPaths(cb) {
        async.each([this.getBucketsZkPath(),
                    this.getQueuedBucketsZkPath()],
                   (path, done) => this._zkClient.mkdirp(path, done), cb);
    }

    _processBucket(ownerId, bucketName, done) {
        this.logger.debug('processing bucket', { ownerId, bucketName });
        return process.nextTick(() => done(null, [{
            message: JSON.stringify({
                action: 'processObjects',
                target: {
                    bucket: bucketName,
                    owner: ownerId,
                },
                details: {},
            }),
        }]));
    }

    processBuckets() {
        this.logger.info('starting queue-buckets job');
        const zkBucketsPath = this.getBucketsZkPath();
        async.waterfall([
            next => this._zkClient.getChildren(
                zkBucketsPath,
                null,
                (err, buckets) => {
                    if (err) {
                        this.logger.error(
                            'error getting list of buckets from zookeeper',
                            { zkPath: zkBucketsPath, error: err.message });
                    }
                    return next(err, buckets);
                }),
            (buckets, next) => async.concatLimit(
                buckets, this._concurrency,
                (bucket, done) => {
                    const [ownerId, bucketName] = bucket.split(':');
                    if (!ownerId || !bucketName) {
                        this.logger.error(
                            'malformed zookeeper bucket entry, skipping',
                            { zkPath: zkBucketsPath, bucket });
                        return process.nextTick(done);
                    }
                    return this._processBucket(ownerId, bucketName, done);
                }, next),
            (entries, next) => {
                this.logger.debug(
                    'producing kafka entries',
                    { topic: this.lcConfig.bucketTasksTopic,
                      entries });
                if (entries.length === 0) {
                    return next();
                }
                return this._producer.send(entries, next);
            },
        ], () => {});
    }

    /**
     * Initialize kafka producer and zookeeper client
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    init(done) {
        if (this._zkClient) {
            // already initialized
            return process.nextTick(done);
        }
        return async.series([
            next => {
                const producer = new BackbeatProducer({
                    kafka: { hosts: this.kafkaConfig.hosts },
                    topic: this.lcConfig.bucketTasksTopic,
                });
                producer.once('error', next);
                producer.once('ready', () => {
                    this.logger.debug(
                        'producer is ready',
                        { kafkaConfig: this.kafkaConfig,
                          topic: this.lcConfig.bucketTasksTopic });
                    producer.removeAllListeners('error');
                    producer.on('error', err => {
                        this.logger.error('error from backbeat producer', {
                            topic: this.lcConfig.bucketTasksTopic,
                            error: err,
                        });
                    });
                    this._producer = producer;
                    next();
                });
            },
            next => {
                this._zkClient = zookeeperHelper.createClient(
                    this.zkConfig.connectionString);
                this._zkClient.connect();
                this._zkClient.once('error', next);
                this._zkClient.once('ready', () => {
                    // just in case there would be more 'error' events
                    // emitted
                    this._zkClient.removeAllListeners('error');
                    this._zkClient.on('error', err => {
                        this.logger.error(
                            'error from lifecycle conductor zookeeper client',
                            { error: err });
                    });
                    next();
                });
            },
        ], done);
    }

    /**
     * Start the cron jobkafka producer
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    start(done) {
        if (this._started) {
            // already started
            return process.nextTick(done);
        }
        this._started = true;
        return this.init(err => {
            if (err) {
                return done(err);
            }
            this.logger.debug('connecting to job dispatcher');
            const jobDispatcherZkPath = this.getJobDispatcherZkPath();
            this._jobDispatcher = new ProvisionDispatcher({
                connectionString:
                `${this.zkConfig.connectionString}${jobDispatcherZkPath}`,
            });
            this._jobDispatcher.subscribe((err, items) => {
                if (err) {
                    this.logger.error('error during job provisioning',
                                      { error: err.message });
                    return undefined;
                }
                this._isActive = false;
                items.forEach(job => {
                    this.logger.info('conductor job provisioned',
                                     { job });
                    if (job === 'queue-buckets') {
                        this._isActive = true;
                    }
                });
                if (this._isActive && !this._cronJob) {
                    this.logger.info('starting bucket queueing cron job',
                                     { cronRule: this._cronRule });
                    this._cronJob = schedule.scheduleJob(
                        this._cronRule,
                        this.processBuckets.bind(this));
                } else if (!this._isActive && this._cronJob) {
                    this.logger.info('stopping bucket queueing cron job');
                    this._cronJob.cancel();
                    this._cronJob = null;
                }
                if (!this._isActive) {
                    this.logger.info('no active job provisioned, idling');
                }
                return undefined;
            });
            return done();
        });
    }

    /**
     * Stop cron task (if started), stop kafka consumer and commit
     * current offset
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    stop(done) {
        if (this._cronJob) {
            this.logger.info('stopping bucket queueing cron job');
            this._cronJob.cancel();
            this._cronJob = null;
        }
        async.series([
            next => {
                if (!this._jobDispatcher) {
                    return process.nextTick(next);
                }
                this.logger.debug('unsubscribing to job dispatcher');
                return this._jobDispatcher.unsubscribe(next);
            },
            next => {
                if (!this._producer) {
                    return process.nextTick(next);
                }
                this.logger.debug('closing producer');
                return this._producer.close(() => {
                    this._producer = null;
                    this._zkClient = null;
                    next();
                });
            },
        ], err => {
            this._started = false;
            return done(err);
        });
    }
}

module.exports = LifecycleConductor;
