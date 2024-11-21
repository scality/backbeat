const MongoConfigManager = require('./configManager/MongoConfigManager');
const ZookeeperConfigManager = require('./configManager/ZookeeperConfigManager');

/**
 * @class NotificationConfigManager
 *
 * @classdesc Manages bucket notification configurations
 */
class NotificationConfigManager {

    constructor(params) {
        joi.attempt(params, paramsJoi);
        this._logger = params.logger;
        this._mongoConfig = params.mongoConfig;
        this._cachedConfigs = new LRUCache(MAX_CACHED_ENTRIES);
        this._mongoClient = null;
        this._metastore = null;
        this._metastoreChangeStream = null;

        const {
            mongoConfig, bucketMetastore, maxCachedConfigs, zkClient, zkConfig, zkPath, zkConcurrency, logger,
        } = params;
        if (mongoConfig) {
            this._configManagerBackend = new MongoConfigManager({
                mongoConfig,
                bucketMetastore,
                maxCachedConfigs,
                logger,
            });
        } else {
            this._usesZookeeperBackend = true;
            this._configManagerBackend = new ZookeeperConfigManager({
                zkClient,
                zkConfig,
                zkPath,
                zkConcurrency,
                logger,
            });
        }
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
        }).then((client) => {
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
        }).catch((err) => {
            this._logger.error('Could not connect to MongoDB', {
                method: 'NotificationConfigManager._setupMongoClient',
                error: err.message,
            });
            return cb(err);
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
    }

    /**
     * Get bucket notification configuration
     *
     * @param {String} bucket - bucket
     * @param {function} [cb] - callback
     * @return {Object|undefined} - configuration if available or undefined
     */
    getConfig(bucket, cb) {
        const val = this._configManagerBackend.getConfig(bucket);
        if (!cb) {
            return val;
        }
        if (val instanceof Promise) {
            return val.then(res => cb(null, res)).catch(err => cb(err));
        }
        return cb(null, val);
    }

    /**
     * Add/update bucket notification configuration.
     *
     * @param {String} bucket - bucket
     * @param {Object} config - bucket notification configuration
     * @return {boolean} - true if set
     */
    setConfig(bucket, config) {
        return this._configManagerBackend.setConfig(bucket, config);
    }

    /**
     * Remove bucket notification configuration
     *
     * @param {String} bucket - bucket
     * @return {undefined}
     */
    removeConfig(bucket) {
        return this._configManagerBackend.removeConfig(bucket);
    }

    /**
     * Setup bucket notification configuration manager
     *
     * @param {function} [cb] - callback
     * @return {undefined}
     */
    setup(cb) {
        return this._configManagerBackend.setup(cb);
    }
}

module.exports = NotificationConfigManager;
