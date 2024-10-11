const MongoConfigManager = require('./configManager/MongoConfigManager');
const ZookeeperConfigManager = require('./configManager/ZookeeperConfigManager');

/**
 * @class NotificationConfigManager
 *
 * @classdesc Manages bucket notification configurations
 */
class NotificationConfigManager {

    constructor(params) {
        const { mongoConfig, bucketMetastore, zkClient, logger } = params;
        if (mongoConfig) {
            this._configManagerBackend = new MongoConfigManager({
                mongoConfig,
                bucketMetastore,
                logger,
            });
        } else {
            this._usesZookeeperBackend = true;
            this._configManagerBackend = new ZookeeperConfigManager({
                zkClient,
                logger,
            });
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
     * Only supported in the Zookeeper backend.
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
     * Only supported in the Zookeeper backend.
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
