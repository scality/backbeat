const async = require('async');
const { EventEmitter } = require('events');
const zookeeper = require('node-zookeeper-client');
const joi = require('joi');

const safeJsonParse = require('../utils/safeJsonParse');
const constants = require('../constants');

const paramsJoi = joi.object({
    zkClient: joi.object().required(),
    logger: joi.object().required(),
}).required();

/**
 * @class ZookeeperConfigManager
 *
 * @classdesc Manages bucket notification configurations in zookeeper
 */
class ZookeeperConfigManager {
    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.zkClient - zookeeper client
     * @param {Logger} params.logger - logger object
     */
    constructor(params) {
        joi.attempt(params, paramsJoi);
        this._zkClient = params.zkClient;
        this.log = params.logger;
        this._configs = new Map();
        this._concurrency = constants.configManager.concurrency;
        this._emitter = new EventEmitter();
        this._setupEventListeners();
    }

    _errorListener(error, listener) {
        this.log.error('ZookeeperConfigManager.emitter.error', {
            listener,
            error,
        });
        return undefined;
    }

    _setConfigListener(bucket, config) {
        this.log.debug('ZookeeperConfigManager.emitter.setConfig', {
            event: 'setConfig',
            bucket,
            config,
        });
        this._setBucketNotifConfig(bucket, JSON.stringify(config), err => {
            if (err) {
                this._emitter.emit('error', err, 'setConfigListener');
            }
            return undefined;
        });
    }

    _getConfigListener(updatedBucket = '') {
        this.log.debug('ZookeeperConfigManager.emitter.getConfig', {
            event: 'getConfig',
        });
        this._listBucketsWithConfig((err, buckets) => {
            if (err) {
                this._emitter.emit('error', err, 'getConfigListener');
                return undefined;
            }
            this.log.debug('bucket config to be updated in map', {
                bucket: updatedBucket,
            });
            const newBuckets = this._getNewBucketNodes(buckets);
            this.log.debug('new bucket configs to be added to map', {
                buckets: newBuckets,
            });
            const bucketsToMap = updatedBucket ? [updatedBucket, ...newBuckets] : newBuckets;
            this.log.debug('bucket configs to be added/updated to map', {
                buckets: bucketsToMap,
            });
            if (bucketsToMap.length > 0) {
                this._updateLocalStore(bucketsToMap);
            }
            return undefined;
        });
    }

    _removeConfigListener(bucket) {
        this.log.debug('ZookeeperConfigManager.emitter.removeConfig', {
            event: 'removeConfig',
            bucket,
        });
        this._removeBucketNotifConfigNode(bucket, err => {
            if (err) {
                this._emitter.emit('error', err, 'removeConfigListener');
            }
            return undefined;
        });
    }

    _setupEventListeners() {
        this._emitter.setMaxListeners(constants.configManager.maxListeners);
        this._emitter
            .on('error', error => this._errorListener(error))
            .on('setConfig',
                (bucket, config) => this._setConfigListener(bucket, config))
            .on('getConfig', bucket => this._getConfigListener(bucket))
            .on('removeConfig', bucket => this._removeConfigListener(bucket));
    }

    _callbackHandler(cb, err, result) {
        if (cb && typeof cb === 'function') {
            return cb(err, result);
        }
        return undefined;
    }

    _getBucketNodeZkPath(bucket) {
        return `/${constants.zkConfigParentNode}/${bucket}`;
    }

    _getConfigDataFromBuffer(data) {
        const { error, result } = safeJsonParse(data);
        if (error) {
            this.log.error('invalid config', { error, config: data });
            return undefined;
        }
        return result;
    }

    _getBucketNotifConfig(bucket, cb) {
        const method
            = 'ZookeeperConfigManager._getBucketNotifConfig';
        const zkPath = this._getBucketNodeZkPath(bucket);
        return this._zkClient.getData(zkPath, event => {
            this.log.debug('zookeeper getData watcher triggered', {
                zkPath,
                method,
                event,
                bucket,
            });
            if (event.type === zookeeper.Event.NODE_DATA_CHANGED) {
                this._emitter.emit('getConfig', bucket);
            }
            if (event.type === zookeeper.Event.NODE_DELETED) {
                this.removeConfig(bucket, false);
            }
        }, (error, data) => {
            if (error && error.name !== 'NO_NODE') {
                const errMsg
                    = 'error fetching bucket notification configuration';
                this.log.error(errMsg, {
                    method,
                    error,
                });
                return this._callbackHandler(cb, error);
            }
            if (data) {
                return this._callbackHandler(cb, null, data);
            }
            // no configuration
            return this._callbackHandler(cb);
        });
    }

    _checkNodeExists(zkPath, cb) {
        const method
            = 'ZookeeperConfigManager._checkNodeExists';
        return this._zkClient.exists(zkPath, (err, stat) => {
            if (err) {
                this.log.error('error checking node existence',
                    { method, zkPath });
                return this._callbackHandler(cb, err);
            }
            if (stat) {
                this.log.debug('node exists', { method, zkPath });
                return this._callbackHandler(cb, null, true);
            }
            this.log.debug('node does not exist', { method, zkPath });
            return this._callbackHandler(cb, null, false);
        });
    }

    _setBucketNotifConfig(bucket, data, cb) {
        const method
            = 'ZookeeperConfigManager._setBucketNotifConfig';
        const zkPath = this._getBucketNodeZkPath(bucket);
        return async.waterfall([
            next => this._checkNodeExists(zkPath, next),
            (exists, next) => {
                if (!exists) {
                    return this._createBucketNotifConfigNode(bucket,
                        err => next(err));
                }
                return next();
            },
            next => this._zkClient.setData(zkPath, Buffer.from(data), -1, next),
        ], err => {
            if (err) {
                this.log.error('error saving config', { method, zkPath, data });
            }
            return this._callbackHandler(cb, err);
        });
    }

    _checkConfigurationParentNode(cb) {
        const method
            = 'ZookeeperConfigManager._checkConfigurationParentNode';
        const zkPath = `/${constants.zkConfigParentNode}`;
        return async.waterfall([
            next => this._checkNodeExists(zkPath, next),
            (exists, next) => {
                if (!exists) {
                    this.log.debug('parent configuration zookeeper node does ' +
                        'not exist', { method, zkPath });
                    return this._zkClient.mkdirp(zkPath, err => next(err));
                }
                this.log.debug('parent configuration zookeeper node exists',
                    { method, zkPath });
                return next();
            },
        ], err => {
            if (err) {
                const errMsg
                    = 'error checking configuration zookeeper parent node';
                this.log.error(errMsg, { method, zkPath, error: err.message });
                return this._callbackHandler(cb, err);
            }
            this.log.debug('parent configuration zookeeper checked/added',
                { method, zkPath });
            return this._callbackHandler(cb);
        });
    }

    _createBucketNotifConfigNode(bucket, cb) {
        const method
            = 'ZookeeperConfigManager._createBucketNotifConfigNode';
        const zkPath = this._getBucketNodeZkPath(bucket);
        return this._zkClient.mkdirp(zkPath, err => {
            if (err) {
                this.log.error('Could not pre-create path in zookeeper', {
                    method,
                    zkPath,
                    error: err,
                });
                return this._callbackHandler(cb, err);
            }
            return this._callbackHandler(cb);
        });
    }

    _removeBucketNotifConfigNode(bucket, cb) {
        const method
            = 'ZookeeperConfigManager._removeBucketNotifConfigNode';
        const zkPath = this._getBucketNodeZkPath(bucket);
        return this._zkClient.remove(zkPath, error => {
            if (error && error.name !== 'NO_NODE') {
                this.log.error('Could not remove zookeeper node', {
                    method,
                    zkPath,
                    error,
                });
                return this._callbackHandler(cb, error);
            }
            if (!error) {
                const msg
                    = 'removed notification configuration zookeeper node';
                this.log.debug(msg, {
                    method,
                    bucket,
                });
            }
            return this._callbackHandler(cb);
        });
    }

    _getNewBucketNodes(bucketsNodeList) {
        if (Array.isArray(bucketsNodeList)) {
            const bucketsFromMap = [...this._configs.keys()];
            return bucketsNodeList.filter(b => !bucketsFromMap.includes(b));
        }
        return [];
    }

    _listBucketsWithConfig(cb) {
        const method
            = 'ZookeeperConfigManager._listBucketsWithConfig';
        const zkPath = `/${constants.zkConfigParentNode}`;
        this._zkClient.getChildren(zkPath, event => {
            this.log.debug('zookeeper getChildren watcher triggered', {
                zkPath,
                method,
                event,
            });
            if (event.type === zookeeper.Event.NODE_CHILDREN_CHANGED) {
                this._emitter.emit('getConfig');
            }
        }, (error, buckets) => {
            if (error) {
                const errMsg
                    = 'error listing buckets with configuration';
                this.log.error(errMsg, {
                    zkPath,
                    method,
                    error,
                });
                this._callbackHandler(cb, error);
            }
            this._callbackHandler(cb, null, buckets);
        });
    }

    _updateLocalStore(buckets, cb) {
        async.eachSeries(buckets, (bucket, next) => {
            this._getBucketNotifConfig(bucket, (err, data) => {
                if (err) {
                    return next(err);
                }
                const configObject = this._getConfigDataFromBuffer(data);
                if (configObject) {
                    this._configs.set(bucket, configObject);
                }
                return next();
            });
        }, err => this._callbackHandler(cb, err));
    }

    /**
     * Get bucket notification configuration
     *
     * @param {String} bucket - bucket
     * @return {Object|undefined} - configuration if available or undefined
     */
    getConfig(bucket) {
        return this._configs.get(bucket);
    }

    /**
     * Add/update bucket notification configuration
     *
     * @param {String} bucket - bucket
     * @param {Object} config - bucket notification configuration
     * @return {boolean} - true if set
     */
    setConfig(bucket, config) {
        try {
            this.log.debug('set config', {
                method: 'ZookeeperConfigManager.setConfig',
                bucket,
                config,
            });
            this._configs.set(bucket, config);
            this._emitter.emit('setConfig', bucket, config);
            return true;
        } catch (err) {
            const errMsg
                = 'error setting bucket notification configuration';
            this.log.error(errMsg, {
                method: 'ZookeeperConfigManager.setConfig',
                error: err.message,
                bucket,
                config,
            });
            return false;
        }
    }

    /**
     * Remove bucket notification configuration
     *
     * @param {String} bucket - bucket
     * @return {boolean} - true if removed
     */
    removeConfig(bucket) {
        try {
            this.log.debug('remove config', {
                method: 'ZookeeperConfigManager.removeConfig',
                bucket,
            });
            this._configs.delete(bucket);
            this._emitter.emit('removeConfig', bucket);
            return true;
        } catch (err) {
            const errMsg
                = 'error removing bucket notification configuration';
            this.log.error(errMsg, {
                method: 'ZookeeperConfigManager.removeConfig',
                error: err,
                bucket,
            });
            return false;
        }
    }

    /**
     * Setup bucket notification configuration manager
     *
     * @param {function} [cb] - callback
     * @return {undefined}
     */
    setup(cb) {
        return this._checkConfigurationParentNode(err => {
            if (err) {
                return cb(err);
            }
            return this._listBucketsWithConfig((err, buckets) => {
                if (err) {
                    return cb(err);
                }
                return this._updateLocalStore(buckets, cb);
            });
        });
    }
}

module.exports = ZookeeperConfigManager;
