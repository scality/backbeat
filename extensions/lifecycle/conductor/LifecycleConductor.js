'use strict'; // eslint-disable-line

const async = require('async');
const schedule = require('node-schedule');
const zookeeper = require('node-zookeeper-client');

const errors = require('arsenal').errors;
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
     * @param {String} lcConfig.zookeeperPath - base path for
     * lifecycle nodes in zookeeper
     * @param {String} lcConfig.bucketTasksTopic - lifecycle
     *   bucket tasks topic name
     * @param {Object} lcConfig.backlogControl - lifecycle backlog
     * control params
     * @param {Boolean} [lcConfig.backlogControl.enabled] - enable
     * lifecycle backlog control
     * @param {String} [lcConfig.backlogMetrics] - backlog metrics
     * config object to hold on if backlog is non-null (see
     * {@link BackbeatConsumer} constructor for params)
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
        this._totalProcessingCycles = 0;

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

    _getPartitionsOffsetsZkPath(topic) {
        return `${this.lcConfig.backlogMetrics.zkPath}/${topic}`;
    }

    _getOffsetZkPath(topic, partition, offsetType, groupId) {
        const basePath =
                  `${this._getPartitionsOffsetsZkPath(topic)}/${partition}`;
        return (offsetType === 'topic' ?
                `${basePath}/topic` :
                `${basePath}/consumers/${groupId}`);
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
        const zkBucketsPath = this.getBucketsZkPath();
        async.waterfall([
            next => this._controlBacklog(next),
            next => {
                this.logger.info('starting new lifecycle batch');
                this._zkClient.getChildren(
                    zkBucketsPath,
                    null,
                    (err, buckets) => {
                        if (err) {
                            this.logger.error(
                                'error getting list of buckets from zookeeper',
                                { zkPath: zkBucketsPath, error: err.message });
                        }
                        return next(err, buckets);
                    });
            },
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
        ], () => {
            ++this._totalProcessingCycles;
        });
    }

    _controlBacklog(done) {
        /* skip backlog control step in the following cases:
         * - disabled in config
         * - backlog metrics not configured
         * - on first processing cycle (to guarantee progress in case
         *   of restarts)
         */
        if (!this.lcConfig.conductor.backlogControl.enabled ||
            !this.lcConfig.backlogMetrics ||
            this._totalProcessingCycles === 0) {
            return process.nextTick(done);
        }
        return async.series([
            next => this._controlBacklogForTopic(
                this.lcConfig.bucketTasksTopic,
                this.lcConfig.producer.groupId, next),
            next => this._controlBacklogForTopic(
                this.lcConfig.objectTasksTopic,
                this.lcConfig.consumer.groupId, next),
        ], err => done(err));
    }

    _readZkOffset(topic, partition, offsetType, groupId, done) {
        const zkPath = this._getOffsetZkPath(topic, partition, offsetType,
                                             groupId);
        this._zkClient.getData(zkPath, (err, offsetData) => {
            if (err) {
                this.logger.error('error reading offset from zookeeper',
                                  { topic, partition, offsetType,
                                    error: err.message });
                return done(err);
            }
            let offset;
            try {
                offset = JSON.parse(offsetData);
            } catch (err) {
                this.logger.error('malformed JSON data for offset',
                                  { topic, partition, offsetType,
                                    error: err.message });
                return done(errors.InternalError);
            }
            if (!Number.isInteger(offset)) {
                this.logger.error('offset not a number',
                                  { topic, partition, offsetType });
                return done(errors.InternalError);
            }
            return done(null, offset);
        });
    }

    _controlBacklogForTopic(topic, groupId, done) {
        const partitionsZkPath = this._getPartitionsOffsetsZkPath(topic);
        this._zkClient.getChildren(partitionsZkPath, (err, partitions) => {
            if (err) {
                if (err.getCode() === zookeeper.Exception.NO_NODE) {
                    // no consumer has yet published his offset
                    return done();
                }
                this.logger.error(
                    'error getting list of consumer offsets from zookeeper',
                    { topic, error: err.message });
                return done(err);
            }
            return async.eachSeries(partitions, (partition, partitionDone) => {
                let consumerOffset;
                let topicOffset;
                async.waterfall([
                    next => this._readZkOffset(topic, partition,
                                               'consumer', groupId, next),
                    (offset, next) => {
                        consumerOffset = offset;
                        this._readZkOffset(topic, partition,
                                           'topic', null, next);
                    },
                    (offset, next) => {
                        topicOffset = offset;
                        const backlog = topicOffset - consumerOffset;
                        this.logger.debug(
                            'backlog computed for consumer/topic',
                            { topic, partition,
                              consumerOffset, topicOffset, backlog });
                        // No need to allow any backlog because the
                        // conductor expects everything to be
                        // processed in a single cycle before starting
                        // another one.
                        if (backlog <= 0) {
                            // partition has been consumed timely enough
                            return next();
                        }
                        this.logger.info(
                            'skipping lifecycle batch due to ' +
                                'operation backlog still in progress',
                            { topic, partition,
                              consumerOffset, topicOffset, backlog });
                        return next(errors.Throttling);
                    },
                ], partitionDone);
            }, done);
        });
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
     * Stop cron task (if started), stop kafka producer
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
