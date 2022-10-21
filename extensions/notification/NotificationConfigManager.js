const joi = require('joi');

const { ZenkoMetrics } = require('arsenal').metrics;
const LRUCache = require('arsenal').algorithms
    .cache.LRUCache;
const MongoClient = require('mongodb').MongoClient;
const constants = require('./constants');

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
    name: 'notification_config_manager_cache_updates',
    help: 'Total number of cache updates',
    labelNames: [
        CONFIG_MANAGER_OPERATION_TYPE,
    ],
});

const configGetLag = ZenkoMetrics.createHistogram({
    name: 'notification_config_manager_config_get_sec',
    help: 'Time it takes in seconds to get a bucket notification config from MongoDB',
    labelNames: [
        CONFIG_MANAGER_CACHE_HIT,
    ],
    buckets: [0.0000001, 0.000001, 0.00001, 0.0001, 0.001, 0.01, 1, 10, 100, 1000, 10000],
});

const cachedBuckets = ZenkoMetrics.createGauge({
    name: 'notification_config_manager_cached_buckets_count',
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
     * @param {String} params.mongoConfig - config for connecting to mongo
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
        this._changeStreamResumeToken = null;
    }

    /**
     * Connects to MongoDB using the MongoClientInterface
     * and retreives the metastore collection
     * @param {Function} cb callback
     * @returns {undefined}
     */
    _setupMongoClient(cb) {
        const { authCredentials, replicaSetHosts, replicaSet, database }
            = this._mongoConfig;

        let cred = '';
        if (authCredentials &&
            authCredentials.username &&
            authCredentials.password) {
            const username = encodeURIComponent(authCredentials.username);
            const password = encodeURIComponent(authCredentials.password);
            cred = `${username}:${password}@`;
        }

        this._mongoUrl = `mongodb://${cred}${replicaSetHosts}/`;
        this._replicaSet = replicaSet;
        MongoClient.connect(this._mongoUrl, {
            replicaSet: this._replicaSet,
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
                this._mongoClient = client.db(database, {
                    ignoreUndefined: true,
                });
                this._metastore = this._mongoClient.collection(constants.bucketMetastore);
                return cb();
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
        // caching change stream resume token
        this._changeStreamResumeToken = change._id;
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
     * Handler for change stream the error event
     * it reestablishes the change stream when an error occurs
     * @returns {undefined}
     */
    async _handleChangeStreamErrorEvent() {
        this._logger.error('An error occured when listening to the change stream', {
            method: 'NotificationConfigManager._setMetastoreChangeStream',
        });
        this._metastoreChangeStream.removeListener('change', this._handleChangeStreamChangeEvent.bind(this));
        this._metastoreChangeStream.removeListener('error', this._handleChangeStreamErrorEvent.bind(this));
        // closing and restarting the change stream
        if (!this._metastoreChangeStream.isClosed()) {
            await this._metastoreChangeStream.close();
        }
        this._setMetastoreChangeStream();
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
        const changeStreamParams = { fullDocument: 'updateLookup' };
        if (this._changeStreamResumeToken) {
            /**
             * using "startAfter" instead of "resumeAfter" to resume
             * even after an invalid event
             */
            changeStreamParams.startAfter = this._changeStreamResumeToken;
        }
        this._metastoreChangeStream = this._metastore.watch(changeStreamPipeline, changeStreamParams);
        this._metastoreChangeStream.on('change', this._handleChangeStreamChangeEvent.bind(this));
        this._metastoreChangeStream.on('error', this._handleChangeStreamErrorEvent.bind(this));
        this._logger.debug('Change stream set', {
            method: 'NotificationConfigManager._setMetastoreChangeStream',
        });
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
