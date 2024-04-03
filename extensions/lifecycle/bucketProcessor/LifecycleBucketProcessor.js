'use strict'; // eslint-disable-line

const async = require('async');
const { Logger } = require('werelogs');
const { errors } = require('arsenal');

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const KafkaBacklogMetrics = require('../../../lib/KafkaBacklogMetrics');
const LifecycleTask = require('../tasks/LifecycleTask');
const LifecycleTaskV2 = require('../tasks/LifecycleTaskV2');
const safeJsonParse = require('../util/safeJsonParse');
const ClientManager = require('../../../lib/clients/ClientManager');
const { authTypeAssumeRole } = require('../../../lib/constants');
const LocationStatusStream = require('../../utils/LocationStatusStream');
const {
    getFormattedSupportedLifecycleRules,
    isExpirationRule
} = require('../util/rules');
const {
    updateCircuitBreakerConfigForImplicitOutputQueue,
} = require('../../../lib/CircuitBreaker');
const {
    extractBucketProcessorCircuitBreakerConfigs,
} = require('../CircuitBreakerGroup');
const { lifecycleTaskVersions } = require('../../../lib/constants');
const locations = require('../../../conf/locationConfig.json');

const PROCESS_OBJECTS_ACTION = 'processObjects';

/**
 * @class LifecycleBucketProcessor
 *
 * @classdesc Handles consuming entries from the bucket tasks topic
 * and producing lifecycle messages to trigger actions for objects
 * that qualify to lifecycle rules.
 *
 * If another listing is required on the bucket (i.e., the listing is
 * truncated), an entry is pushed to the bucket topic for the next
 * round of processing.
 */
class LifecycleBucketProcessor {
    /**
     * Constructor of LifecycleBucketProcessor
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper config
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} [kafkaConfig.backlogMetrics] - param object to
     * publish kafka topic metrics to zookeeper (see {@link
     * BackbeatConsumer} constructor)
     * @param {Object} lcConfig - lifecycle config
     * @param {Object} lcConfig.lifecycle.auth - authentication info
     * @param {String} lcConfig.lifecycle.bucketTasksTopic - lifecycle bucket
     * topic name
     * @param {Object} lcConfig.lifecycle.bucketProcessor - kafka consumer
     * object
     * @param {String} lcConfig.lifecycle.bucketProcessor.groupId - kafka
     * consumer group id
     * @param {Object} repConfig - replication config
     * @param {String} repConfig.topic - kafka replication topic
     * @param {Object} repConfig.source - replication source
     * @param {Number} [lcConfig.bucketProcessor.concurrency] - number
     *  of max allowed concurrent operations
     * @param {Object} s3Config - s3 config
     * @param {String} s3Config.host - host ip
     * @param {String} s3Config.port - port
     * @param {Object} mongoConfig - MongoDb Connection Config
     * @param {String} transport - http or https
     * @param {Object} lcOptions - lifecycle options
     * @param {boolean} lcOptions.expireOneDayEarlier - moves lifecycle expiration deadlines 1 day earlier
     * @param {boolean} lcOptions.transitionOneDayEarlier - moves lifecycle transition deadlines 1 day earlier
     * @param {Number} lcOptions.timeProgressionFactor - decrease the weight attributed to a day in order to
     * expedite the lifecycle of objects.
     */

    constructor(zkConfig, kafkaConfig, lcConfig, repConfig, s3Config, mongoConfig, transport = 'http', lcOptions) {
        this._log = new Logger('Backbeat:Lifecycle:BucketProcessor');
        this._zkConfig = zkConfig;
        this._kafkaConfig = kafkaConfig;
        this._lcConfig = lcConfig;
        this._repConfig = repConfig;
        this._mongoConfig = mongoConfig;
        this._lcOptions = lcOptions;
        this._producer = null;
        this._kafkaBacklogMetrics = null;

        this._supportedRules = getFormattedSupportedLifecycleRules();

        this._producerReady = false;
        this._consumerReady = false;

        this._pausedLocations = new Set();
        this._locationStatusStream = null;

        this.clientManager = new ClientManager({
            id: 'lifecycle',
            authConfig: lcConfig.bucketProcessor.auth || lcConfig.auth,
            s3Config,
            transport,
        }, this._log);

        this.retryWrapper = new BackbeatTask();

        // helper object to facilitate the tracking of the the latest x
        // noncurrent versions of an object when the field
        // `NewerNoncurrentVersions` is present.
        //
        // Structure:
        // ...
        // [bucket]: {
        //      ...
        //      [object]: {
        //          ...
        //          [rule-id]: Heap of <version objects>
        //      }
        // }
        // ...
        //
        // Bucket level entries are removed when the listing results are not
        // truncated and after the listing results have been processed.
        //
        // Object level entries are removed when their bucket level entries are
        // removed or when:
        // * the NextMarker field in the results is different key from the entry's
        // * all the object/version of each listing request have been processed
        //
        this.ncvHeap = new Map();

        // The task scheduler for processing lifecycle tasks concurrently.
        this._internalTaskScheduler = async.queue((ctx, cb) => {
            const { task, rules, value, s3target, backbeatMetadataProxy } = ctx;
            return this.retryWrapper.retry({
                actionDesc: 'process bucket lifecycle entry',
                logFields: { value },
                actionFunc: (done, nbRetries) => task.processBucketEntry(
                    rules, value, s3target, backbeatMetadataProxy, nbRetries, done),
                shouldRetryFunc: err => err.retryable,
                log: this._log,
            }, cb);
        }, this._lcConfig.bucketProcessor.concurrency);

        // Listen for errors from any task being processed.
        this._internalTaskScheduler.drain(err => {
            if (err) {
                this._log.error('error occurred during task processing', {
                    error: err,
                });
            }
        });

        const globalCircuitBreakerConfig = extractBucketProcessorCircuitBreakerConfigs(
                this._lcConfig.bucketProcessor.circuitBreaker,
                this._lcConfig,
                this._repConfig,
                locations,
                this._log,
        );

        this._circuitBreakerConfig = updateCircuitBreakerConfigForImplicitOutputQueue(
            globalCircuitBreakerConfig.global,
            this._lcConfig.objectProcessor.groupId,
            this._lcConfig.objectTasksTopic,
        );

        this._circuitBreakers = globalCircuitBreakerConfig.circuitBreakerGroup;
    }

    /**
     * Get the state variables of the current instance.
     * @return {Object} Object containing the state variables
     */
    getStateVars() {
        return {
            producer: this._producer,
            bootstrapList: this._repConfig.destination.bootstrapList,
            bucketTasksTopic: this._lcConfig.bucketTasksTopic,
            objectTasksTopic: this._lcConfig.objectTasksTopic,
            transitionTasksTopic: this._lcConfig.transitionTasksTopic,
            kafkaBacklogMetrics: this._kafkaBacklogMetrics,
            ncvHeap: this.ncvHeap,
            pausedLocations: this._pausedLocations,
            lcOptions: this._lcOptions,
            circuitBreakers: this._circuitBreakers,
            log: this._log,
        };
    }

    /**
     * Pause bucket(s) based on given location
     * @param {String} location - location
     * @return {undefined}
     */
    _pauseServiceForLocation(location) {
        const paused = this._pausedLocations.has(location);
        if (!paused) {
            this._pausedLocations.add(location);
            this._log.info('location paused', {
                method: 'LifecycleBucketProcessor._pauseServiceForLocation',
                location,
            });
        }
    }

    /**
     * Resume lifecycle operations for a location
     * @param {String} location - location
     * @param {Date} [date] - optional date object for scheduling resume
     * @return {undefined}
     */
    _resumeServiceForLocation(location) {
        const paused = this._pausedLocations.has(location);
        if (paused) {
            this._pausedLocations.delete(location);
            this._log.info('location resumed', {
                method: 'LifecycleBucketProcessor._resumeServiceForLocation',
                location,
            });
        }
    }

    /**
     * Determine whether the given config should be processed.
     * @param {Object} config - The bucket lifecycle configuration
     * @return {Boolean} Whether the config should be processed
     */
    _shouldProcessConfig(config) {
        return config.Rules.some(rule => {
            if (rule.Status === 'Disabled') {
                return false;
            }
            return Object.keys(rule).some(key => {
                if (!this._supportedRules.includes(key)) {
                    return false;
                }
                if (isExpirationRule(key)) {
                    return true;
                }
                return rule[key].some(transitionRule =>
                    !this._pausedLocations.has(transitionRule.StorageClass));
            });
        });
    }

    /**
     * Process the given bucket entry, get the bucket's lifecycle configuration,
     * and schedule a task with the lifecycle configuration rules, if
     * applicable.
     * @param {Object} entry - The kafka entry containing the information for
     * performing a listing of the bucket's objects
     * @param {Object} entry.value - The value of the entry object
     * (see format of messages in lifecycle topic for bucket tasks)
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _processBucketEntry(entry, cb) {
        const { error, result } = safeJsonParse(entry.value);
        if (error) {
            this._log.error('could not parse bucket entry',
                            { value: entry.value, error });
            return process.nextTick(() => cb(error));
        }
        if (result.action !== PROCESS_OBJECTS_ACTION) {
            return process.nextTick(cb);
        }
        if (typeof result.target !== 'object') {
            this._log.error('malformed kafka bucket entry', {
                method: 'LifecycleBucketProcessor._processBucketEntry',
                entry: result,
            });
            return process.nextTick(() => cb(errors.InternalError));
        }
        const { bucket, owner, accountId, taskVersion } = result.target;
        if (!bucket || !owner || (!accountId && this._authConfig.type === authTypeAssumeRole)) {
            this._log.error('kafka bucket entry missing required fields', {
                method: 'LifecycleBucketProcessor._processBucketEntry',
                bucket,
                owner,
                accountId,
            });
            return process.nextTick(() => cb(errors.InternalError));
        }
        this._log.debug('processing bucket entry', {
            method: 'LifecycleBucketProcessor._processBucketEntry',
            bucket,
            owner,
            accountId,
        });

        const s3 = this.clientManager.getS3Client(accountId);
        if (!s3) {
            return cb(errors.InternalError
                .customizeDescription('failed to obtain a s3 client'));
        }

        const backbeatMetadataProxy =
            this.clientManager.getBackbeatMetadataProxy(accountId);
        if (!backbeatMetadataProxy) {
            return cb(errors.InternalError
                .customizeDescription('failed to obtain a backbeat client'));
        }

        const params = { Bucket: bucket };
        return this._getBucketLifecycleConfiguration(s3, params, (err, config) => {
            if (err) {
                if (err.code === 'NoSuchLifecycleConfiguration') {
                    this._log.debug('skipping non-lifecycled bucket', { bucket });
                    return cb();
                }

                if (err.code === 'NoSuchBucket') {
                    this._log.error('skipping non-existent bucket', { bucket });
                    return cb();
                }

                this._log.error('error getting bucket lifecycle config', {
                    method: 'LifecycleBucketProcessor._processBucketEntry',
                    bucket,
                    owner,
                    error: err,
                });
                return cb(err);
            }
            if (!this._shouldProcessConfig(config)) {
                return cb();
            }

            let task;

            if (taskVersion === lifecycleTaskVersions.v1 || !taskVersion) {
                task = new LifecycleTask(this);
            } else {
                task = new LifecycleTaskV2(this);
            }

            this._log.info('scheduling new task for bucket lifecycle', {
                method: 'LifecycleBucketProcessor._processBucketEntry',
                bucket,
                owner,
                details: result.details,
                taskName: task.constructor.name,
            });
            return this._internalTaskScheduler.push({
                task,
                rules: config.Rules,
                value: result,
                s3target: s3,
                backbeatMetadataProxy,
            }, cb);
        });
    }

    /**
     * Call AWS.S3.GetBucketLifecycleConfiguration in a retry wrapper.
     * @param {AWS.S3} s3 - the s3 client
     * @param {object} params - the parameters to pass to getBucketLifecycleConfiguration
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _getBucketLifecycleConfiguration(s3, params, cb) {
        return this.retryWrapper.retry({
            actionDesc: 'get bucket lifecycle',
            logFields: { params },
            actionFunc: done => s3.getBucketLifecycleConfiguration(params, done),
            shouldRetryFunc: err => err.retryable,
            log: this._log,
        }, cb);
    }

    /**
     * Set up the backbeat producer with the given topic.
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _setupProducer(cb) {
        const producer = new BackbeatProducer({
            kafka: { hosts: this._kafkaConfig.hosts },
            maxRequestSize: this._kafkaConfig.maxRequestSize,
            topic: this._lcConfig.objectTasksTopic,
        });
        producer.once('error', err => {
            this._log.error('error setting up kafka producer', {
                error: err,
                method: 'LifecycleBucketProcesso::_setupProducer',
            });
            process.exit(1);
        });
        producer.once('ready', () => {
            this._log.debug('producer is ready',
                { kafkaConfig: this.kafkaConfig });
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this._log.error('error from backbeat producer', {
                    error: err,
                });
            });
            this._producerReady = true;
            this._producer = producer;
            return cb();
        });
    }

    /**
     * Set up the lifecycle consumer.
     * @param {function} cb - callback
     * @return {undefined}
     */
    _setupConsumer(cb) {
        this._consumer = new BackbeatConsumer({
            zookeeper: {
                connectionString: this._zkConfig.connectionString,
            },
            kafka: {
                hosts: this._kafkaConfig.hosts,
                site: this._kafkaConfig.site,
                backlogMetrics: this._kafkaConfig.backlogMetrics,
            },
            topic: this._lcConfig.bucketTasksTopic,
            groupId: this._lcConfig.bucketProcessor.groupId,
            concurrency: this._lcConfig.bucketProcessor.concurrency,
            queueProcessor: this._processBucketEntry.bind(this),
            circuitBreaker: this._circuitBreakerConfig,
            circuitBreakerMetrics: {
                type: 'lifecycle_bucket_processor',
            },
        });
        this._consumer.on('error', err => {
            if (!this._consumerReady) {
                this._log.fatal('unable to start lifecycle consumer', {
                    error: err,
                    method: 'LifecycleBucketProcessor._setupConsumer',
                });
                process.exit(1);
            }
        });
        this._consumer.on('ready', () => {
            this._consumerReady = true;
            this._consumer.subscribe();
            cb();
        });
    }

    /**
     * Set up the producers and consumers needed for lifecycle.
     * @param {function} done - callback
     * @return {undefined}
     */
    start(done) {
        this.clientManager.initSTSConfig();
        this.clientManager.initCredentialsManager();
        async.series([
            done => this._setupProducer(done),
            done => this._initKafkaBacklogMetrics(done),
            done => this._setupConsumer(done),
            done => {
                this._locationStatusStream = new LocationStatusStream('lifecycle',
                    this._mongoConfig,
                    this._pauseServiceForLocation.bind(this),
                    this._resumeServiceForLocation.bind(this),
                    this._log);
                this._locationStatusStream.start(done);
            }
        ], done);
    }

    _initKafkaBacklogMetrics(cb) {
        this._kafkaBacklogMetrics = new KafkaBacklogMetrics(
            this._zkConfig.connectionString, this._kafkaConfig.backlogMetrics);
        this._kafkaBacklogMetrics.init();
        this._kafkaBacklogMetrics.once('ready', () => {
            this._kafkaBacklogMetrics.removeAllListeners('error');
            cb();
        });
        this._kafkaBacklogMetrics.once('error', err => {
            this._log.error('error setting up kafka topic metrics', {
                error: err,
                method: 'LifecycleBucketProcessor._initKafkaBacklogMetrics',
            });
            process.exit(1);
        });
    }

    /**
     * Close the lifecycle bucket processor
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        if (this._deleteInactiveCredentialsInterval) {
            clearInterval(this._deleteInactiveCredentialsInterval);
        }

        async.parallel([
            done => {
                this._log.debug('closing bucket tasks consumer');
                this._consumer.close(done);
            },
            done => {
                this._log.debug('closing producer');
                this._producer.close(done);
            },
        ], () => cb());
    }

    isReady() {
        return this._producer && this._producer.isReady() &&
               this._consumer && this._consumer.isReady();
    }
}

module.exports = LifecycleBucketProcessor;
