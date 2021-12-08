'use strict'; // eslint-disable-line

const async = require('async');

const Logger = require('werelogs').Logger;
const errors = require('arsenal').errors;
const { replicationBackends, emptyFileMd5 } = require('arsenal').constants;
const MongoClient = require('arsenal').storage
    .metadata.mongoclient.MongoClientInterface;
const ObjectMD = require('arsenal').models.ObjectMD;
const { encode } = require('arsenal').versioning.VersionID;

const Config = require('../../lib/Config');
const BackbeatConsumer = require('../../lib/BackbeatConsumer');
const QueueEntry = require('../../lib/models/QueueEntry');
const DeleteOpQueueEntry = require('../../lib/models/DeleteOpQueueEntry');
const ObjectQueueEntry = require('../../lib/models/ObjectQueueEntry');
const MetricsProducer = require('../../lib/MetricsProducer');
const { metricsExtension, metricsTypeCompleted, metricsTypePendingOnly } =
    require('../ingestion/constants');
const getContentType = require('./utils/contentTypeHelper');
const BucketMemState = require('./utils/BucketMemState');
const MongoProcessorMetrics = require('./MongoProcessorMetrics');

// batch metrics by location and send to kafka metrics topic every 5 seconds
const METRIC_REPORT_INTERVAL_MS = process.env.CI === 'true' ? 1000 : 5000;

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
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {String} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
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
     * @param {Object} mConfig - metrics config
     */
    constructor(kafkaConfig, mongoProcessorConfig, mongoClientConfig, mConfig) {
        this.kafkaConfig = kafkaConfig;
        this.mongoProcessorConfig = mongoProcessorConfig;
        this.mongoClientConfig = mongoClientConfig;
        this._mConfig = mConfig;

        this._consumer = null;
        this._bootstrapList = null;
        this.logger = new Logger('Backbeat:Ingestion:MongoProcessor');
        this.mongoClientConfig.logger = this.logger;
        this._mongoClient = new MongoClient(this.mongoClientConfig);
        this._bucketMemState = new BucketMemState(Config);

        // in-mem batch of metrics, we only track total entry count by location
        // this._accruedMetrics = { zenko-location: 10 }
        this._accruedMetrics = {};

        setInterval(() => {
            this._sendMetrics();
        }, METRIC_REPORT_INTERVAL_MS);
    }

    _setupMetricsClients(cb) {
        // Metrics Producer
        this._mProducer = new MetricsProducer(this.kafkaConfig, this._mConfig);
        this._mProducer.setupProducer(cb);
    }

    /**
     * Start kafka consumer
     *
     * @return {undefined}
     */
    start() {
        this.logger.info('starting mongo queue processor');
        async.series([
            next => this._setupMetricsClients(err => {
                if (err) {
                    this.logger.error('error setting up metrics client', {
                        method: 'MongoQueueProcessor.start',
                        error: err,
                    });
                }
                return next(err);
            }),
            next => this._mongoClient.setup(err => {
                if (err) {
                    this.logger.error('could not connect to MongoDB', {
                        method: 'MongoQueueProcessor.start',
                        error: err.message,
                    });
                }
                return next(err);
            }),
        ], error => {
            if (error) {
                this.logger.fatal('error starting mongo queue processor');
                process.exit(1);
            }

            this._bootstrapList = Config.getBootstrapList();
            Config.on('bootstrap-list-update', () => {
                this._bootstrapList = Config.getBootstrapList();
            });

            let consumerReady = false;
            this._consumer = new BackbeatConsumer({
                topic: this.mongoProcessorConfig.topic,
                groupId: `${this.mongoProcessorConfig.groupId}`,
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
        async.parallel([
            next => {
                if (this._consumer) {
                    this.logger.debug('closing kafka consumer', {
                        method: 'MongoQueueProcessor.stop',
                    });
                    return this._consumer.close(next);
                }
                this.logger.debug('no kafka consumer to close', {
                    method: 'MongoQueueProcessor.stop',
                });
                return next();
            },
            next => {
                if (this._mProducer) {
                    this.logger.debug('closing metrics producer', {
                        method: 'MongoQueueProcessor.stop',
                    });
                    return this._mProducer.close(next);
                }
                this.logger.debug('no metrics producer to close', {
                    method: 'MongoQueueProcessor.stop',
                });
                return next();
            },
        ], done);
    }

    _getZenkoObjectMetadata(log, entry, bucketInfo, done) {
        // NOTE: This is only used for updating replication info. If the Zenko
        //   bucket does not have repInfo set, then we can ignore fetching
        const bucketRepInfo = bucketInfo.getReplicationConfiguration();
        if (!bucketRepInfo || !bucketRepInfo.rules ||
            !bucketRepInfo.rules[0].enabled) {
            return done();
        }

        const bucket = entry.getBucket();
        const key = entry.getObjectKey();
        const params = {};
        if (entry.getVersionId()) {
            params.versionId = entry.getVersionId();
        }

        return this._mongoClient.getObject(bucket, key, params, log,
        (err, data) => {
            if (err && err.NoSuchKey) {
                return done();
            }
            if (err) {
                log.error('error getting zenko object metadata', {
                    method: 'MongoQueueProcessor._getZenkoObjectMetadata',
                    error: err.message,
                    entry: entry.getLogInfo(),
                });
                return done(err);
            }
            return done(null, data);
        });
    }

    /**
     * get dataStoreVersionId, if exists
     * @param {Object} objMd - object md fetched from mongo
     * @param {String} site - storage location name
     * @return {String} dataStoreVersionId
     */
    _getDataStoreVersionId(objMd, site) {
        let dataStoreVersionId = '';
        if (objMd.replicationInfo && objMd.replicationInfo.backends) {
            const backend = objMd.replicationInfo.backends
                                                 .find(l => l.site === site);
            if (backend && backend.dataStoreVersionId) {
                dataStoreVersionId = backend.dataStoreVersionId;
            }
        }
        return dataStoreVersionId;
    }

    /**
     * Update ingested entry metadata fields: owner-id, owner-display-name
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {BucketInfo} bucketInfo - bucket info object
     * @return {undefined}
     */
    _updateOwnerMD(entry, bucketInfo) {
        // zenko bucket owner information is being set on ingested md
        entry.setOwnerDisplayName(bucketInfo.getOwnerDisplayName());
        entry.setOwnerId(bucketInfo.getOwner());
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
     * @param {string} zenkoLocation - zenko storage location name
     * @return {undefined}
     */
    _updateLocations(entry, zenkoLocation) {
        const locations = entry.getLocation();
        // if version id is undefined, we have a single null object.
        // To hold reference to this null object, we need to encode "null"
        // as its dataStoreVersionId
        const dataStoreVersionId = entry.getVersionId() ?
            entry.getEncodedVersionId() : encode('null');
        let zenkoDataLocations;
        if (!locations || locations.length === 0) {
            zenkoDataLocations = [{
                key: entry.getObjectKey(),
                size: 0,
                start: 0,
                dataStoreName: zenkoLocation,
                dataStoreType: 'aws_s3',
                dataStoreETag: `1:${emptyFileMd5}`,
                dataStoreVersionId,
            }];
        } else {
            zenkoDataLocations = [{
                key: entry.getObjectKey(),
                size: entry.getContentLength(),
                start: 0,
                dataStoreName: zenkoLocation,
                dataStoreType: 'aws_s3',
                dataStoreETag: `1:${entry.getContentMd5()}`,
                dataStoreVersionId,
            }];
        }
        entry.setLocation(zenkoDataLocations);
    }

    /**
     * Update acl info on ingested object MD
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @return {undefined}
     */
    _updateAcl(entry) {
        // reset acl info
        const objectMDModel = new ObjectMD();
        entry.setAcl(objectMDModel.getAcl());
    }

    /**
     * Update replication info on ingested object MD to match Zenko defined
     * replication info.
     * @param {ObjectQueueEntry} entry - object queue entry object
     * @param {BucketInfo} bucketInfo - bucket info object
     * @param {Array} content - replication info content field
     * @param {Object|undefined} zenkoObjMd - metadata fetched from mongo
     * @return {undefined}
     */
    _updateReplicationInfo(entry, bucketInfo, content, zenkoObjMd) {
        const bucketRepInfo = bucketInfo.getReplicationConfiguration();

        // reset first before attempting any other updates
        const objectMDModel = new ObjectMD();
        entry.setReplicationInfo(objectMDModel.getReplicationInfo());

        // TODO: refactor based off cloudserver getReplicationInfo
        if (bucketRepInfo) {
            const { role, destination, rules } = bucketRepInfo;
            const rule = rules.find(r =>
                (entry.getObjectKey().startsWith(r.prefix) && r.enabled));

            if (rule) {
                const replicationInfo = {};
                const storageTypes = [];
                const backends = [];
                const storageClasses = rule.storageClass.split(',');

                storageClasses.forEach(storageClass => {
                    const storageClassName =
                        storageClass.endsWith(':preferred_read') ?
                        storageClass.split(':')[0] : storageClass;
                    const location = this._bootstrapList.find(l =>
                        (l.site === storageClassName));
                    if (location && replicationBackends[location.type]) {
                        storageTypes.push(location.type);
                    }
                    let dataStoreVersionId = '';
                    if (zenkoObjMd) {
                        dataStoreVersionId = this._getDataStoreVersionId(
                            zenkoObjMd, storageClassName);
                    }
                    backends.push({
                        site: storageClassName,
                        status: 'PENDING',
                        dataStoreVersionId,
                    });
                });

                // save updated replication info
                replicationInfo.status = 'PENDING';
                replicationInfo.backends = backends;
                replicationInfo.content = content;
                replicationInfo.destination = destination;
                replicationInfo.storageClass = storageClasses.join(',');
                replicationInfo.role = role;
                replicationInfo.storageType = storageTypes.join(',');
                replicationInfo.isNFS = bucketInfo.isNFS();

                // apply changes
                entry.setReplicationInfo(replicationInfo);
            }
        }
    }

    /**
     * Process a delete object entry
     * @param {Logger.newRequestLogger} log - request logger object
     * @param {DeleteOpQueueEntry} sourceEntry - delete object entry
     * @param {string} location - zenko storage location name
     * @param {function} done - callback(error)
     * @return {undefined}
     */
    _processDeleteOpQueueEntry(log, sourceEntry, location, done) {
        const bucket = sourceEntry.getBucket();
        const key = sourceEntry.getObjectVersionedKey();

        // Always call deleteObject with version params undefined so
        // that mongoClient will use deleteObjectNoVer which just deletes
        // the object without further manipulation/actions.
        // S3 takes care of the versioning logic so consuming the queue
        // is sufficient to replay the version logic in the consumer.
        return this._mongoClient.deleteObject(bucket, key, undefined, log,
            err => {
                if (err) {
                    this._normalizePendingMetric(location);
                    log.end().error('error deleting object metadata ' +
                    'from mongo', {
                        bucket,
                        key,
                        error: err.message,
                        location,
                    });
                    return done(err);
                }
                this._produceMetricCompletionEntry(location);
                log.end().info('object metadata deleted from mongo', {
                    entry: sourceEntry.getLogInfo(),
                    location,
                });
                return done();
            });
    }

    /**
     * Process an object entry
     * @param {Logger.newRequestLogger} log - request logger object
     * @param {ObjectQueueEntry} sourceEntry - object metadata entry
     * @param {string} location - zenko storage location name
     * @param {BucketInfo} bucketInfo - bucket info object
     * @param {function} done - callback(error)
     * @return {undefined}
     */
    _processObjectQueueEntry(log, sourceEntry, location, bucketInfo, done) {
        const bucket = sourceEntry.getBucket();
        const key = sourceEntry.getObjectKey();

        this._getZenkoObjectMetadata(log, sourceEntry, bucketInfo,
        (err, zenkoObjMd) => {
            if (err) {
                this._normalizePendingMetric(location);
                log.end().error('error processing object queue entry', {
                    method: 'MongoQueueProcessor._processObjectQueueEntry',
                    entry: sourceEntry.getLogInfo(),
                    location,
                });
                return done(err);
            }

            const content = getContentType(sourceEntry, zenkoObjMd);
            if (content.length === 0) {
                this._normalizePendingMetric(location);
                log.end().debug('skipping duplicate entry', {
                    method: 'MongoQueueProcessor._processObjectQueueEntry',
                    entry: sourceEntry.getLogInfo(),
                    location,
                });
                // identified as duplicate entry, do not store in mongo
                return done();
            }

            // update necessary metadata fields before saving to Zenko MongoDB
            this._updateOwnerMD(sourceEntry, bucketInfo);
            this._updateObjectDataStoreName(sourceEntry, location);
            this._updateLocations(sourceEntry, location);
            this._updateAcl(sourceEntry);
            this._updateReplicationInfo(sourceEntry, bucketInfo, content,
                zenkoObjMd);

            // Some object stores we are ingesting from (e.g. S3C) do
            // not populate the value.key property.
            sourceEntry.setKey(key);

            const objVal = sourceEntry.getValue();
            const params = {};
            if (sourceEntry.getVersionId()) {
                params.versionId = sourceEntry.getVersionId();
                params.repairMaster = true;
            }

            /**
             * For single null versions, their version id is undefined
             * and isNull is undefined. Always call putObject with version
             * params undefined so that mongoClient will use putObjectNoVer
             * which just puts the object without further manipulation/actions.
             * For all other entries, we specify `repairMaster` and `versionId`
             * in params to putObject to use putObjectVerCase4. We rely on
             * internal logic in mongoClient for handling master version ops.
             */
            return this._mongoClient.putObject(bucket, key, objVal, params,
                this.logger, err => {
                    if (err) {
                        this._normalizePendingMetric(location);
                        log.end().error('error putting object metadata ' +
                        'to mongo', {
                            bucket,
                            key,
                            versionId: sourceEntry.getEncodedVersionId(),
                            error: err.message,
                            location,
                        });
                        return done(err);
                    }
                    this._produceMetricCompletionEntry(location);
                    log.end().info('object metadata put to mongo', {
                        entry: sourceEntry.getLogInfo(),
                        location,
                    });
                    return done();
                });
        });
    }

    /**
     * Send accrued metrics by location to kafka
     * @return {undefined}
     */
    _sendMetrics() {
        Object.keys(this._accruedMetrics).forEach(loc => {
            const count = this._accruedMetrics[loc];

            // only report metrics if something has been recorded for location
            if (count > 0) {
                this._accruedMetrics[loc] = 0;
                const metric = { [loc]: { ops: count } };
                this._mProducer.publishMetrics(metric, metricsTypeCompleted,
                    metricsExtension, () => {});
            }
        });
    }

    /**
     * Accrue metrics in-mem every METRIC_REPORT_INTERVAL_MS
     * @param {string} location - zenko storage location name
     * @return {undefined}
     */
    _produceMetricCompletionEntry(location) {
        if (this._accruedMetrics[location]) {
            this._accruedMetrics[location] += 1;
        } else {
            this._accruedMetrics[location] = 1;
        }
    }

    /**
     * For cases where we experience an error or skip an entry, we need to
     * normalize pending metric. This means we will see pending metrics stuck
     * above 0 and will need to bring those metrics down
     * @param {string} location - location constraint name
     * @return {undefined}
     */
    _normalizePendingMetric(location) {
        const metric = { [location]: { ops: 1 } };
        this._mProducer.publishMetrics(metric, metricsTypePendingOnly,
            metricsExtension, () => {});
    }

    /**
     * Get bucket info in memoize state if exists, otherwise fetch from Mongo
     * @param {ObjectQueueEntry} sourceEntry - object metadata entry
     * @param {Logger.newRequestLogger} log - request logger object
     * @param {function} cb - callback(error, BucketInfo)
     * @return {undefined}
     */
    _getBucketInfo(sourceEntry, log, cb) {
        const bucketName = sourceEntry.getBucket();
        const bucketInfo = this._bucketMemState.getBucketInfo(bucketName);
        if (bucketInfo) {
            return cb(null, bucketInfo);
        }
        return this._mongoClient.getBucketAttributes(bucketName, log,
        (err, bucketInfo) => {
            if (err) {
                log.error('error getting bucket owner ' +
                'details', {
                    method: 'MongoQueueProcessor.processKafkaEntry',
                    entry: sourceEntry.getLogInfo(),
                    error: err.message,
                });
                return cb(err);
            }
            // memoize BucketInfo
            this._bucketMemState.memoize(bucketName, bucketInfo);
            return cb(null, bucketInfo);
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
        MongoProcessorMetrics.onProcessKafkaEntry();
        const log = this.logger.newRequestLogger();
        const sourceEntry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        if (sourceEntry.error) {
            log.end().error('error processing source entry',
                              { error: sourceEntry.error });
            this._handleMetrics(null, true);
            return process.nextTick(() => done(errors.InternalError));
        }

        return this._getBucketInfo(sourceEntry, log, (err, bucketInfo) => {
            if (err) {
                this._handleMetrics(sourceEntry, true);
                return done(err);
            }
            const location = bucketInfo.getLocationConstraint();

            if (sourceEntry instanceof DeleteOpQueueEntry) {
                return this._processDeleteOpQueueEntry(log, sourceEntry,
                    location, err => {
                        this._handleMetrics(sourceEntry, !!err);
                        return done(err);
                    });
            }
            if (sourceEntry instanceof ObjectQueueEntry) {
                return this._processObjectQueueEntry(log, sourceEntry, location,
                    bucketInfo, err => {
                        this._handleMetrics(sourceEntry, !!err);
                        return done(err);
                    });
            }
            log.end().warn('skipping unknown source entry', {
                entry: sourceEntry.getLogInfo(),
                entryType: sourceEntry.constructor.name,
                method: 'MongoQueueProcessor.processKafkaEntry',
            });
            this._normalizePendingMetric(location);
            return process.nextTick(done);
        });
    }

    _handleMetrics(sourceEntry, isError) {
        const status = isError ? 'error' : 'success';
        const startProcessing = sourceEntry && sourceEntry.getStartProcessing ? sourceEntry.getStartProcessing() : null;
        const elapsedMs = startProcessing ? Date.now() - startProcessing : null;

        MongoProcessorMetrics.onIngestionProcessed(elapsedMs, status);
    }

    isReady() {
        return this._consumer && this._consumer.isReady();
    }
}

module.exports = MongoQueueProcessor;
