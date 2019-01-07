'use strict'; // eslint-disable-line

const util = require('util');

const async = require('async');
const AWS = require('aws-sdk');
const http = require('http');

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

// TODO - ADD PREFIX BASED ON SOURCE
// april 6, 2018

// Temp testing
const RESET = '\x1b[0m';
// magenta
const COLORME = '\x1b[35m';

function logMe(str) {
    console.log(COLORME, str, RESET);
}

/**
 * @class MongoQueueProcessor
 *
 * @classdesc Background task that processes entries from the
 * ingestion for kafka queue and pushes entries to mongo
 */
class MongoQueueProcessor {

    /**
     * @constructor
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
     * @param {String} site - site name
     */
    constructor(kafkaConfig, s3Config, mongoProcessorConfig, mongoClientConfig,
                serviceAuth, site) {
        this.kafkaConfig = kafkaConfig;
        this.mongoProcessorConfig = mongoProcessorConfig;
        this.mongoClientConfig = mongoClientConfig;
        this._serviceAuth = serviceAuth;
        this.site = site;

        this._s3Endpoint = `http://${s3Config.host}:${s3Config.port}`;
        this._s3Client = null;
        this._accountInfo = null;
        this._consumer = null;
        this.logger =
            new Logger(`Backbeat:Ingestion:MongoProcessor:${this.site}`);
        this.mongoClientConfig.logger = this.logger;
        this._mongoClient = new MongoClient(this.mongoClientConfig);
    }

    /**
     * Return an S3 client instance using the given account credentials.
     * @param {Object} accountCreds - Object containing account credentials
     * @param {String} accountCreds.accessKeyId - The account access key
     * @param {String} accountCreds.secretAccessKey - The account secret key
     * @return {AWS.S3} The S3 client instance to make requests with
     */
    _getS3Client(accountCreds) {
        console.log(`ACCOUNT CREDS: ${accountCreds.accessKeyId} | ${accountCreds.secretAccessKey}`)
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

    _manipulateLocations(entry, location) {
        const locations = entry.getLocation();
        const editLocations = locations.map(l => {
            const newValues = {
                key: entry.getObjectKey(),
                dataStoreName: location,
                dataStoreType: 'aws_s3',
            }
            if (entry.getVersionId()) {
                console.log(`has versionid.. ${entry.getEncodedVersionId()}`)
                newValues.dataStoreVersionId = entry.getEncodedVersionId();
            }
            return Object.assign({}, l, newValues);
        });
        entry.setLocation(editLocations);
    }

    _manipulateOwner(entry, ownerInfo) {
        // entry.setOwnerDisplayName(this._accountInfo.ownerDisplayName)
        // entry.setOwnerId(this._accountInfo.ownerId);
        entry.setOwnerDisplayName(ownerInfo.ownerDisplayName)
        entry.setOwnerId(ownerInfo.ownerId);
    }

    _manipulateObjectDataStoreName(entry, location) {
        entry.setDataStoreName(location);
    }

    _setupAccountCredentials() {
        // const c = getAccountCredentials(repconfig, this.logger);
        const credentials = getAccountCredentials(this._serviceAuth,
                                                  this.logger);
        // service account owner to set on objects
        // this._accountInfo = {
        //     ownerDisplayName: credentials._displayName,
        //     ownerId: getOwnerId(credentials._canonicalID),
        // };
        this._s3Client = this._getS3Client(credentials);
        // util.inspect(credentials, { depth: 3 })
        // util.inspect(this._s3Client, { depth: 3 })

        // console.log(`REPLICATION ACCOUNT CREDS: ${c.accessKeyId} | ${c.secretAccessKey}`)
        // console.log(util.inspect(c, { depth: 3 }))
    }

    /**
     * Start kafka consumer
     *
     * @return {undefined}
     */
    start() {
        this.logger.info('starting mongo queue processor');

        this._setupAccountCredentials();
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
                this._consumer.subscribe();
                this.logger.info('mongo queue processor is ready');
            });
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

    _processDeleteOpQueueEntry(sourceEntry, location, done) {
        const bucket = sourceEntry.getBucket();
        const key = sourceEntry.getObjectVersionedKey();

        // TODO: manipulate? Need to delete zenko entry?

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

    _processObjectQueueEntry(sourceEntry, location, ownerInfo, done) {
        const bucket = sourceEntry.getBucket();
        // always use versioned key so putting full version state to mongo
        const key = sourceEntry.getObjectVersionedKey();

        // TODO: versioned vs non-versioned? What does the entry look like
        // in locations field
        this._manipulateOwner(sourceEntry, ownerInfo);
        this._manipulateObjectDataStoreName(sourceEntry, location);
        this._manipulateLocations(sourceEntry, location);

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
            logMe('BucketMdQueueEntry')
            this.logger.warn('skipping bucket md queue entry', {
                method: 'MongoQueueProcessor.processKafkaEntry',
                entry: sourceEntry.getLogInfo(),
            });
            return process.nextTick(done);
        } else if (sourceEntry instanceof BucketQueueEntry) {
            logMe('BucketQueueEntry')
            this.logger.warn('skipping bucket queue entry', {
                method: 'MongoQueueProcessor.processKafkaEntry',
                entry: sourceEntry.getLogInfo(),
            });
            return process.nextTick(done);
        }

        const bucketName = sourceEntry.getBucket();
        if (!bucketName) {
            return process.nextTick(done);
        }

        async.series([
            next => this._mongoClient.getBucketAttributes(bucketName,
            this.logger, (err, data) => {
                if (err) {
                    this.logger.error('ERROR IN MONGOCLIENT CALL');
                    return next(err);
                }
                let ownerInfo = {
                    ownerId: data._owner,
                    ownerDisplayName: data._ownerDisplayName,
                }
                return next(null, ownerInfo);
            }),
            next => this._s3Client.getBucketLocation({ Bucket: bucketName },
            (err, data) => {
                if (err) {
                    logMe(`ERROR: ${err}`)
                    this.logger.error('error getting bucket location constraint', {
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
                logMe('DeleteOpQueueEntry')
                return this._processDeleteOpQueueEntry(sourceEntry,
                    location, done);
            }
            if (sourceEntry instanceof ObjectQueueEntry) {
                logMe('ObjectQueueEntry')
                return this._processObjectQueueEntry(sourceEntry,
                    location, ownerInfo, done);
            }
            // the following are entries that we do not expect
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
