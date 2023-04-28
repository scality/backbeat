const joi = require('joi');
const semver = require('semver');

const { ZenkoMetrics } = require('arsenal').metrics;
const LRUCache = require('arsenal').algorithms
    .cache.LRUCache;
const MongoClient = require('mongodb').MongoClient;
const ChangeStream = require('../../lib/wrappers/ChangeStream');
const constants = require('./constants');
const { constructConnectionString, getMongoVersion } = require('../utils/MongoUtils');

const paramsJoi = joi.object({
    mongoConfig: joi.object().required(),
    logger: joi.object().required(),
}).required();

const MAX_CACHED_ENTRIES = Number(process.env.MAX_CACHED_BUCKET_NOTIFICATION_CONFIGS)
    || 1000;

// should equal true if config manager's cache was hit during a get operation
const CONFIG_MANAGER_CACHE_HIT = 'cache_hit';
// Type of operation performed on the cache
const CONFIG_MANAGER_OPERATION_TYPE = 'op';

const cacheUpdates = ZenkoMetrics.createCounter({
    name: 's3_notification_config_manager_cache_updates_total',
    help: 'Total number of cache updates',
    labelNames: [
        CONFIG_MANAGER_OPERATION_TYPE,
    ],
});

const configGetLag = ZenkoMetrics.createHistogram({
    name: 's3_notification_config_manager_config_get_seconds',
    help: 'Time it takes in seconds to get a bucket notification config from MongoDB',
    labelNames: [
        CONFIG_MANAGER_CACHE_HIT,
    ],
    buckets: [0.001, 0.01, 1, 10, 100, 1000],
});

const cachedBuckets = ZenkoMetrics.createGauge({
    name: 's3_notification_config_manager_cached_buckets_count',
    help: 'Total number of cached buckets in the notification config manager',
});

function onConfigManagerCacheUpdate(op) {
    cacheUpdates.inc({
        [CONFIG_MANAGER_OPERATION_TYPE]: op,
    });
    if (op === 'add') {
        cachedBuckets.inc({});
    } else if (op === 'delete') {
        cachedBuckets.dec({});
    }
}

function onConfigManagerConfigGet(cacheHit, delay) {
    configGetLag.observe({
        [CONFIG_MANAGER_CACHE_HIT]: cacheHit,
    }, delay);
}

/**
 * @class NotificationConfigManager
 *
 * @classdesc Manages bucket notification configurations, the configurations
 * are directly retrieved from the metastore, and are locally cached. Cache
 * is invalidated using MongoDB change streams.
 */
class NotificationConfigManager {
    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.mongoConfig - mongoDB config
     * @param {Logger} params.logger - logger object
     */
    constructor(params) {
        joi.attempt(params, paramsJoi);
        this._logger = params.logger;
        this._mongoConfig = params.mongoConfig;
        this._cachedConfigs = new LRUCache(MAX_CACHED_ENTRIES);
        this._mongoClient = null;
        this._metastore = null;
        this._metastoreChangeStream = null;
    }

    /**
     * Connects to MongoDB using the MongoClientInterface
     * and retreives the metastore collection
     * @param {Function} cb callback
     * @returns {undefined}
     */
    _setupMongoClient(cb) {
        const mongoUrl = constructConnectionString(this._mongoConfig);
        MongoClient.connect(mongoUrl, {
            replicaSet: this._mongoConfig.replicaSet,
            useNewUrlParser: true,
        },
        (err, client) => {
            if (err) {
                this._logger.error('Could not connect to MongoDB', {
                    method: 'NotificationConfigManager._setupMongoClient',
                    error: err.message,
                });
                return cb(err);
            }
            this._logger.debug('Connected to MongoDB', {
                method: 'NotificationConfigManager._setupMongoClient',
            });
            try {
                this._mongoClient = client.db(this._mongoConfig.database, {
                    ignoreUndefined: true,
                });
                this._metastore = this._mongoClient.collection(constants.bucketMetastore);
                // get mongodb version
                getMongoVersion(this._mongoClient, (err, version) => {
                    if (err) {
                        this._logger.error('Could not get MongoDB version', {
                            method: 'NotificationConfigManager._setupMongoClient',
                            error: err.message,
                        });
                        return cb(err);
                    }
                    this._mongoVersion = version;
                    return cb();
                });
                return undefined;
            } catch (error) {
                return cb(error);
            }
        });
    }

    /**
     * Handler for the change stream "change" event.
     * Invalidates cached bucket configs based on the change.
     * @param {ChangeStreamDocument} change Change stream change object
     * @returns {undefined}
     */
    _handleChangeStreamChangeEvent(change) {
        // invalidating cached notification configs
        const cachedConfig = this._cachedConfigs.get(change.documentKey._id);
        const bucketNotificationConfiguration = change.fullDocument ? change.fullDocument.value.
            notificationConfiguration : null;
        switch (change.operationType) {
            case 'delete':
                if (cachedConfig) {
                    this._cachedConfigs.remove(change.documentKey._id);
                    onConfigManagerCacheUpdate('delete');
                }
                break;
            case 'replace':
            case 'update':
                if (cachedConfig) {
                    // add() replaces the value of an entry if it exists in cache
                    this._cachedConfigs.add(change.documentKey._id, bucketNotificationConfiguration);
                    onConfigManagerCacheUpdate('update');
                }
                break;
            default:
                this._logger.debug('Skipping unsupported change stream event', {
                    method: 'NotificationConfigManager._handleChangeStreamChange',
                });
                break;
        }
        this._logger.debug('Change stream event processed', {
            method: 'NotificationConfigManager._handleChangeStreamChange',
        });
    }

    /**
     * Initializes a change stream on the metastore collection
     * Only document delete and update/replace operations are
     * taken into consideration to invalidate cache.
     * Newly created buckets (insert operations) are not cached
     * as queue populator instances read from different kafka
     * partitions and so don't need the configs for all buckets
     * @returns {undefined}
     */
    _setMetastoreChangeStream() {
        /**
         * To avoid processing irrelevant events
         * we filter by the operation types and
         * only project the fields needed
         */
        const changeStreamPipeline = [
            {
                $match: {
                    $or: [
                        { operationType: 'delete' },
                        { operationType: 'replace' },
                        { operationType: 'update' },
                    ]
                }
            },
            {
                $project: {
                    '_id': 1,
                    'operationType': 1,
                    'documentKey._id': 1,
                    'fullDocument._id': 1,
                    'fullDocument.value.notificationConfiguration': 1
                },
            },
        ];
        this._metastoreChangeStream = new ChangeStream({
            logger: this._logger,
            collection: this._metastore,
            pipeline: changeStreamPipeline,
            handler: this._handleChangeStreamChangeEvent.bind(this),
            throwOnError: false,
            useStartAfter: semver.gte(this._mongoVersion, '4.2.0'),
        });
        // start watching metastore
        this._metastoreChangeStream.start();
    }

    /**
     * Sets up the NotificationConfigManager by
     * connecting to mongo and initializing the
     * change stream
     * @param {Function} cb callback
     * @returns {undefined}
     */
    setup(cb) {
        this._setupMongoClient(err => {
            if (err) {
                this._logger.error('An error occured while setting up mongo client', {
                    method: 'NotificationConfigManager.setup',
                });
                return cb(err);
            }
            try {
                this._setMetastoreChangeStream();
            } catch (error) {
                this._logger.error('An error occured while establishing the change stream', {
                    method: 'NotificationConfigManager._setMetastoreChangeStream',
                });
                return cb(error);
            }
            return cb();
        });
    }

    /**
     * Get bucket notification configuration
     *
     * @param {String} bucket - bucket
     * @return {Object|undefined} - configuration if available or undefined
     */
    async getConfig(bucket) {
        const startTime = Date.now();
        // return cached config for bucket if it exists
        const cachedConfig = this._cachedConfigs.get(bucket);
        if (cachedConfig) {
            const delay = (Date.now() - startTime) / 1000;
            onConfigManagerConfigGet(true, delay);
            return cachedConfig;
        }
        try {
            // retreiving bucket metadata from the metastore
            const bucketMetadata = await this._metastore.findOne({ _id: bucket });
            const bucketNotificationConfiguration = (bucketMetadata && bucketMetadata.value &&
                bucketMetadata.value.notificationConfiguration) || undefined;
            // caching the bucket configuration
            this._cachedConfigs.add(bucket, bucketNotificationConfiguration);
            const delay = (Date.now() - startTime) / 1000;
            onConfigManagerConfigGet(false, delay);
            onConfigManagerCacheUpdate('add');
            return bucketNotificationConfiguration;
        } catch (err) {
            this._logger.error('An error occured when getting notification ' +
                'configuration of bucket', {
                method: 'NotificationConfigManager.getConfig',
                bucket,
                error: err.message,
            });
            return undefined;
        }
    }
}

module.exports = NotificationConfigManager;
