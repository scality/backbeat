'use strict'; // eslint-disable-line

const async = require('async');
const AWS = require('aws-sdk');
const http = require('http');
const Redis = require('ioredis');
const schedule = require('node-schedule');

const Logger = require('werelogs').Logger;
const errors = require('arsenal').errors;
const MongoClient = require('arsenal').storage
    .metadata.mongoclient.MongoClientInterface;

const BackbeatConsumer = require('../../lib/BackbeatConsumer');
const QueueEntry = require('../../lib/models/QueueEntry');
const DeleteOpQueueEntry = require('../../lib/models/DeleteOpQueueEntry');
const BucketQueueEntry = require('../../lib/models/BucketQueueEntry');
const BucketMdQueueEntry = require('../../lib/models/BucketMdQueueEntry');
const ObjectQueueEntry = require('../../lib/models/ObjectQueueEntry');
const { getAccountCredentials } =
    require('../../lib/credentials/AccountCredentials');

const {
    zookeeperNamespace,
    zkStatePath,
    zkStateProperties,
} = require('../ingestion/constants');

// TODO - ADD PREFIX BASED ON SOURCE
// april 6, 2018

/**
 * @class MongoQueueProcessor
 *
 * @classdesc Background task that processes entries from the
 * ingestion for kafka queue and pushes entries to mongo
 */
class MongoQueueProcessor {

    /**
     * @constructor
     * @param {node-zookeeper-client.Client} zkClient - zookeeper client
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {String} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} s3Config - s3 config
     * @param {String} s3Config.host - host ip
     * @param {String} s3Config.port - port
     * @param {Object} mongoProcessorConfig - mongo processor configuration
     *   object
     * @param {String} mongoProcessorConfig.topic - topic name
     * @param {String} mongoProcessorConfig.groupId - kafka
     *   consumer group ID
     * @param {number} [mongoProcessorConfig.retry.timeoutS] -
     *  retry timeout in secs.
     * @param {number} [mongoProcessorConfig.retry.maxRetries] -
     *  max retries before giving up
     * @param {Object} [mongoProcessorConfig.retry.backoff] -
     *  backoff params
     * @param {number} [mongoProcessorConfig.retry.backoff.min] -
     *  min. backoff in ms.
     * @param {number} [mongoProcessorConfig.retry.backoff.max] -
     *  max. backoff in ms.
     * @param {number} [mongoProcessorConfig.retry.backoff.jitter] -
     *  randomness
     * @param {number} [mongoProcessorConfig.retry.backoff.factor] -
     *  backoff factor
     * @param {Object} mongoClientConfig - config for connecting to mongo
     * @param {Object} serviceAuth - ingestion service auth
     * @param {Object} redisConfig - redis configuration
     * @param {String} site - site name
     */
    constructor(zkClient, kafkaConfig, s3Config, mongoProcessorConfig,
                mongoClientConfig, serviceAuth, redisConfig, site) {
        this.zkClient = zkClient;
        this.kafkaConfig = kafkaConfig;
        this.mongoProcessorConfig = mongoProcessorConfig;
        this.mongoClientConfig = mongoClientConfig;
        this._serviceAuth = serviceAuth;
        this.site = site;

        this._s3Endpoint = `http://${s3Config.host}:${s3Config.port}`;
        this._s3Client = null;
        this._consumer = null;
        this.scheduledResume = null;

        this.logger =
            new Logger(`Backbeat:Ingestion:MongoProcessor:${this.site}`);
        this.mongoClientConfig.logger = this.logger;
        this._mongoClient = new MongoClient(this.mongoClientConfig);

        this._setupRedis(redisConfig);
    }

    /**
     * Setup the Redis Subscriber which listens for actions from other processes
     * (i.e. BackbeatAPI for pause/resume)
     * @param {object} redisConfig - redis config
     * @return {undefined}
     */
    _setupRedis(redisConfig) {
        // redis pub/sub for pause/resume
        const redis = new Redis(redisConfig);
        // redis subscribe to site specific channel
        const channelName = `${this.mongoProcessorConfig.topic}-${this.site}`;
        redis.subscribe(channelName, err => {
            if (err) {
                this.logger.fatal('mongo queue processor failed to subscribe ' +
                                  'to ingestion redis channel for location ' +
                                  `${this.site}`,
                                  { method: 'MongoQueueProcessor.constructor',
                                    error: err });
                process.exit(1);
            }
            redis.on('message', (channel, message) => {
                const validActions = {
                    pauseService: this._pauseService.bind(this),
                    resumeService: this._resumeService.bind(this),
                    deleteScheduledResumeService:
                        this._deleteScheduledResumeService.bind(this),
                };
                try {
                    const { action, date } = JSON.parse(message);
                    const cmd = validActions[action];
                    if (channel === channelName && typeof cmd === 'function') {
                        cmd(date);
                    }
                } catch (e) {
                    this.logger.error('error parsing redis sub message', {
                        method: 'MongoQueueProcessor._setupRedis',
                        error: e,
                    });
                }
            });
        });
    }

    /**
     * Return an S3 client instance using the given account credentials.
     * @param {Object} accountCreds - Object containing account credentials
     * @param {String} accountCreds.accessKeyId - The account access key
     * @param {String} accountCreds.secretAccessKey - The account secret key
     * @return {AWS.S3} The S3 client instance to make requests with
     */
    _getS3Client(accountCreds) {
        return new AWS.S3({
            endpoint: this._s3Endpoint,
            credentials: {
                accessKeyId: accountCreds.accessKeyId,
                secretAccessKey: accountCreds.secretAccessKey,
            },
            sslEnabled: false,
            s3ForcePathStyle: true,
            signatureVersion: 'v4',
            httpOptions: {
                agent: new http.Agent({ keepAlive: true }),
                timeout: 0,
            },
            maxRetries: 0,
        });
    }

    /**
     * Start kafka consumer
     *
     * @param {object} [options] options object
     * @param {boolean} [options.paused] - if true, kafka consumer is paused
     * @return {undefined}
     */
    start(options) {
        this.logger.info('starting mongo queue processor');
        const credentials = getAccountCredentials(this._serviceAuth,
                                                  this.logger);
        this._s3Client = this._getS3Client(credentials);
        this._mongoClient.setup(err => {
            if (err) {
                this.logger.error('could not connect to MongoDB', { err });
                process.exit(1);
            }
            let consumerReady = false;
            this._consumer = new BackbeatConsumer({
                topic: this.mongoProcessorConfig.topic,
                groupId: `${this.mongoProcessorConfig.groupId}-${this.site}`,
                kafka: { hosts: this.kafkaConfig.hosts },
                queueProcessor: this.processKafkaEntry.bind(this),
            });
            this._consumer.on('error', () => {
                if (!consumerReady) {
                    this.logger.fatal('error starting mongo queue processor');
                    process.exit(1);
                }
            });
            this._consumer.on('ready', () => {
                consumerReady = true;
                const paused = options && options.paused;
                this._consumer.subscribe(paused);
                this.logger.info('mongo queue processor is ready');
            });
        });
    }

    /**
     * Pause consumers
     * @return {undefined}
     */
    _pauseService() {
        const enabled = this._consumer.getServiceStatus();
        if (enabled) {
            // if currently resumed/active, attempt to pause
            this._updateZkStateNode('paused', true, err => {
                if (err) {
                    this.logger.trace('error occurred saving state to ' +
                    'zookeeper', {
                        method: 'MongoQueueProcessor._pauseService',
                    });
                } else {
                    this._consumer.pause(this.site);
                    this.logger.info('paused ingestion for location: ' +
                        `${this.site}`);
                    this._deleteScheduledResumeService();
                }
            });
        }
    }

    /**
     * Resume consumers
     * @param {Date} [date] - optional date object for scheduling resume
     * @return {undefined}
     */
    _resumeService(date) {
        const enabled = this._consumer.getServiceStatus();
        const now = new Date();

        if (enabled) {
            this.logger.info(`cannot resume, site ${this.site} is not paused`);
            return;
        }

        if (date && now < new Date(date)) {
            // if date is in the future, attempt to schedule job
            this._scheduleResume(date);
        } else {
            this._updateZkStateNode('paused', false, err => {
                if (err) {
                    this.logger.trace('error occurred saving state to ' +
                    'zookeeper', {
                        method: 'MongoQueueProcessor._resumeService',
                    });
                } else {
                    this._consumer.resume(this.site);
                    this.logger.info('resumed ingestion for location: ' +
                        `${this.site}`);
                    this._deleteScheduledResumeService();
                }
            });
        }
    }

    /**
     * Delete scheduled resume (if any)
     * @return {undefined}
     */
    _deleteScheduledResumeService() {
        this._updateZkStateNode('scheduledResume', null, err => {
            if (err) {
                this.logger.trace('error occurred saving state to zookeeper', {
                    method: 'MongoQueueProcessor._deleteScheduledResumeService',
                });
            } else if (this.scheduledResume) {
                this.scheduledResume.cancel();
                this.scheduledResume = null;
                this.logger.info('deleted scheduled resume for location:' +
                    ` ${this.site}`);
            }
        });
    }

    _getZkSiteNode() {
        return `${zookeeperNamespace}${zkStatePath}/${this.site}`;
    }

    /**
     * Update zookeeper state node for this site-defined MongoQueueProcessor
     * @param {String} key - key name to store in zk state node
     * @param {String|Boolean} value - value
     * @param {Function} cb - callback(error)
     * @return {undefined}
     */
    _updateZkStateNode(key, value, cb) {
        if (!zkStateProperties.includes(key)) {
            const errorMsg = 'incorrect zookeeper state property given';
            this.logger.error(errorMsg, {
                method: 'MongoQueueProcessor._updateZkStateNode',
            });
            return cb(new Error('incorrect zookeeper state property given'));
        }
        const path = this._getZkSiteNode();
        return async.waterfall([
            next => this.zkClient.getData(path, (err, data) => {
                if (err) {
                    this.logger.error('could not get state from zookeeper', {
                        method: 'MongoQueueProcessor._updateZkStateNode',
                        zookeeperPath: path,
                        error: err.message,
                    });
                    return next(err);
                }
                try {
                    const state = JSON.parse(data.toString());
                    // set revised status
                    state[key] = value;
                    const bufferedData = Buffer.from(JSON.stringify(state));
                    return next(null, bufferedData);
                } catch (err) {
                    this.logger.error('could not parse state data from ' +
                    'zookeeper', {
                        method: 'MongoQueueProcessor._updateZkStateNode',
                        zookeeperPath: path,
                        error: err,
                    });
                    return next(err);
                }
            }),
            (data, next) => this.zkClient.setData(path, data, err => {
                if (err) {
                    this.logger.error('could not save state data in ' +
                    'zookeeper', {
                        method: 'MongoQueueProcessor._updateZkStateNode',
                        zookeeperPath: path,
                        error: err,
                    });
                    return next(err);
                }
                return next();
            }),
        ], cb);
    }

    _scheduleResume(date) {
        function triggerResume() {
            this._updateZkStateNode('scheduledResume', null, err => {
                if (err) {
                    this.logger.error('error occurred saving state ' +
                    'to zookeeper for resuming a scheduled resume. Retry ' +
                    'again in 1 minute', {
                        method: 'MongoQueueProcessor._scheduleResume',
                        error: err,
                    });
                    // if an error occurs, need to retry
                    // for now, schedule minute from now
                    const date = new Date();
                    date.setMinutes(date.getMinutes() + 1);
                    this.scheduledResume = schedule.scheduleJob(date,
                        triggerResume.bind(this));
                } else {
                    if (this.scheduledResume) {
                        this.scheduledResume.cancel();
                    }
                    this.scheduledResume = null;
                    this._resumeService();
                }
            });
        }

        this._updateZkStateNode('scheduledResume', date, err => {
            if (err) {
                this.logger.trace('error occurred saving state to zookeeper', {
                    method: 'MongoQueueProcessor._scheduleResume',
                });
            } else {
                this.scheduledResume = schedule.scheduleJob(date,
                    triggerResume.bind(this));
                this.logger.info('scheduled ingestion resume', {
                    scheduleTime: date.toString(),
                });
            }
        });
    }

    /**
     * Cleanup zookeeper node if the site has been removed as a location
     * @param {function} cb - callback(error)
     * @return {undefined}
     */
    removeZkState(cb) {
        const path = this._getZkSiteNode();
        this.zkClient.remove(path, err => {
            if (err && err.name !== 'NO_NODE') {
                this.logger.error('failed removing zookeeper state node', {
                    method: 'MongoQueueProcessor.removeZkState',
                    zookeeperPath: path,
                    error: err,
                });
                return cb(err);
            }
            return cb();
        });
    }

    /**
     * Stop kafka consumer and commit current offset
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    stop(done) {
        if (!this._consumer) {
            return setImmediate(done);
        }
        return this._consumer.close(done);
    }

    /**
     * Update ingested entry metadata fields: owner-id, owner-display-name
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {object} ownerInfo - owner details
     * @return {undefined}
     */
    _updateOwnerMD(entry, ownerInfo) {
        // zenko bucket owner information is being set on ingested md
        entry.setOwnerDisplayName(ownerInfo.ownerDisplayName);
        entry.setOwnerId(ownerInfo.ownerId);
    }

    /**
     * Update ingested entry metadata fields: dataStoreName
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {string} location - owner details
     * @return {undefined}
     */
    _updateObjectDataStoreName(entry, location) {
        entry.setDataStoreName(location);
    }

    /**
     * Update ingested entry metadata location field. Each location change
     * includes: key, dataStoreName, dataStoreType, dataStoreVersionId
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {string} location - zenko storage location name
     * @return {undefined}
     */
    _updateLocations(entry, location) {
        const locations = entry.getLocation();
        const editLocations = locations.map(l => {
            const newValues = {
                key: entry.getObjectKey(),
                dataStoreName: location,
                dataStoreType: 'aws_s3',
            };
            if (entry.getVersionId()) {
                newValues.dataStoreVersionId = entry.getEncodedVersionId();
            }
            return Object.assign({}, l, newValues);
        });
        entry.setLocation(editLocations);
    }

    /**
     * Process a delete object entry
     * @param {DeleteOpQueueEntry} sourceEntry - delete object entry
     * @param {function} done - callback(error)
     * @return {undefined}
     */
    _processDeleteOpQueueEntry(sourceEntry, done) {
        const bucket = sourceEntry.getBucket();
        const key = sourceEntry.getObjectVersionedKey();

        // Always call deleteObject with version params undefined so
        // that mongoClient will use deleteObjectNoVer which just deletes
        // the object without further manipulation/actions.
        // S3 takes care of the versioning logic so consuming the queue
        // is sufficient to replay the version logic in the consumer.
        return this._mongoClient.deleteObject(bucket, key, undefined,
            this.logger, err => {
                if (err) {
                    this.logger.error('error deleting object metadata ' +
                    'from mongo', { bucket, key, error: err.message });
                    return done(err);
                }
                this.logger.info('object metadata deleted from mongo',
                { bucket, key });
                return done();
            });
    }

    /**
     * Process an object entry
     * @param {ObjectQueueEntry} sourceEntry - object metadata entry
     * @param {string} location - zenko storage location name
     * @param {object} ownerInfo - owner details
     * @param {function} done - callback(error)
     * @return {undefined}
     */
    _processObjectQueueEntry(sourceEntry, location, ownerInfo, done) {
        const bucket = sourceEntry.getBucket();
        // always use versioned key so putting full version state to mongo
        const key = sourceEntry.getObjectVersionedKey();

        // update necessary metadata fields before saving to Zenko MongoDB
        this._updateOwnerMD(sourceEntry, ownerInfo);
        this._updateObjectDataStoreName(sourceEntry, location);
        this._updateLocations(sourceEntry, location);

        const objVal = sourceEntry.getValue();
        // Always call putObject with version params undefined so
        // that mongoClient will use putObjectNoVer which just puts
        // the object without further manipulation/actions.
        // S3 takes care of the versioning logic so consuming the queue
        // is sufficient to replay the version logic in the consumer.
        return this._mongoClient.putObject(bucket, key, objVal, undefined,
            this.logger, err => {
                if (err) {
                    this.logger.error('error putting object metadata ' +
                    'to mongo', { error: err });
                    return done(err);
                }
                this.logger.info('object metadata put to mongo',
                { key });
                return done();
            });
    }

    /**
     * Put kafka queue entry into mongo
     *
     * @param {object} kafkaEntry - entry generated by ingestion populator
     * @param {string} kafkaEntry.key - kafka entry key
     * @param {string} kafkaEntry.value - kafka entry value
     * @param {function} done - callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, done) {
        const sourceEntry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        if (sourceEntry.error) {
            this.logger.error('error processing source entry',
                              { error: sourceEntry.error });
            return process.nextTick(() => done(errors.InternalError));
        }
        if (sourceEntry instanceof BucketMdQueueEntry) {
            this.logger.warn('skipping bucket md queue entry', {
                method: 'MongoQueueProcessor.processKafkaEntry',
                entry: sourceEntry.getLogInfo(),
            });
            return process.nextTick(done);
        } else if (sourceEntry instanceof BucketQueueEntry) {
            this.logger.warn('skipping bucket queue entry', {
                method: 'MongoQueueProcessor.processKafkaEntry',
                entry: sourceEntry.getLogInfo(),
            });
            return process.nextTick(done);
        }

        const bucketName = sourceEntry.getBucket();
        return async.series([
            next => this._mongoClient.getBucketAttributes(bucketName,
                this.logger, (err, data) => {
                    if (err) {
                        this.logger.error('error getting bucket owner ' +
                        'details', {
                            method: 'MongoQueueProcessor.processKafkaEntry',
                            entry: sourceEntry.getLogInfo(),
                        });
                        return done(err);
                    }
                    const ownerInfo = {
                        ownerId: data._owner,
                        ownerDisplayName: data._ownerDisplayName,
                    };
                    return next(null, ownerInfo);
                }),
            next => this._s3Client.getBucketLocation({ Bucket: bucketName },
                (err, data) => {
                    if (err) {
                        this.logger.error('error getting bucket location ' +
                        'constraint', {
                            method: 'MongoQueueProcessor.processKafkaEntry',
                            error: err,
                        });
                        return done(err);
                    }
                    const location = data.LocationConstraint;
                    return next(null, location);
                }),
        ], (err, results) => {
            if (err) {
                return done(err);
            }
            const ownerInfo = results[0];
            const location = results[1];
            // if entry is for another site, simply skip/ignore
            if (this.site !== location) {
                return process.nextTick(done);
            }

            if (sourceEntry instanceof DeleteOpQueueEntry) {
                return this._processDeleteOpQueueEntry(sourceEntry, done);
            }
            if (sourceEntry instanceof ObjectQueueEntry) {
                return this._processObjectQueueEntry(sourceEntry, location,
                    ownerInfo, done);
            }
            this.logger.warn('skipping unknown source entry',
                            { entry: sourceEntry.getLogInfo() });
            return process.nextTick(done);
        });
    }

    isReady() {
        return this._consumer && this._consumer.isReady();
    }
}

module.exports = MongoQueueProcessor;
