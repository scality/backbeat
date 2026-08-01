const async = require('async');
const joi = require('joi');
const { EventEmitter } = require('events');
const zookeeper = require('node-zookeeper-client');

const ZookeeperManager = require('../../../lib/clients/ZookeeperManager');
const BaseConfigManager = require('./BaseConfigManager');

const safeJsonParse = require('../../../lib/util/safeJsonParse');
const constants = require('../constants');

const paramsJoi = joi.object({
    zkClient: joi.object().optional(),
    zkConfig: joi.object().when('zkClient', {
        is: joi.exist(),
        then: joi.optional(),
        otherwise: joi.required(),
    }),
    zkPath: joi.string().when('zkClient', {
        is: joi.exist(),
        then: joi.optional(),
        otherwise: joi.required(),
    }),
    zkConcurrency: joi.number().required(),
    logger: joi.object().required(),
}).required();

/**
 * @class ZookeeperConfigManager
 *
 * @classdesc Manages bucket notification configurations in zookeeper
 */
class ZookeeperConfigManager extends BaseConfigManager  {
    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.zkClient - zookeeper client
     * @param {Logger} params.logger - logger object
     */
    constructor(params) {
        super();
        joi.attempt(params, paramsJoi);
        this._zkClient = params.zkClient;
        this._zkPath = params.zkPath;
        this._zkConfig = params.zkConfig;
        this._zkConcurrency = params.zkConcurrency;
        this.log = params.logger;
        this._configs = new Map();
        this._emitter = new EventEmitter();
        this._setupEventListeners();
    }

    // https://github.com/alexguan/node-zookeeper-client/blob/master/lib/WatcherManager.js#L13-L39
    // watchers should be reapplied only when their event is triggered, otherwise they will be duplicated
    // and the lib will keep adding listeners
    // (even if the functions are the same, they are js object so they don't match)
    // warn early to catch leak more easily instead of waiting event emitter limit at 10 listeners.
    _warnZkWatchersLeak(type, path) {
        const watcherManager = this._zkClient?.client?.connectionManager?.watcherManager;

        const watchers = watcherManager?.[`${type}Watchers`]?.[path];
        if (!watchers) {
            return;
        }

        const listeners = watchers.listenerCount('notification');
        if (listeners > 0) {
            process.emitWarning(`${type}Watchers[${path}] has already ${listeners} listeners`, {
                code: 'ZkWatchersLeak',
            });
        }
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

    _getConfigListener(bucket) {
        this.log.debug('ZookeeperConfigManager.emitter.getConfig', {
            event: 'getConfig',
            bucket,
        });
        this._updateLocalStore([bucket]);
    }

    _listConfigsListener() {
        this.log.debug('ZookeeperConfigManager.emitter.listConfigs', {
            event: 'listConfigs',
        });
        this._listBucketsWithConfig((err, buckets) => {
            if (err) {
                this._emitter.emit('error', err, 'listConfigsListener');
                return undefined;
            }
            const newBuckets = this._getNewBucketNodes(buckets);
            this.log.debug('new bucket configs to be added to map', {
                buckets: newBuckets,
            });
            if (newBuckets.length > 0) {
                this._updateLocalStore(newBuckets);
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
        this._emitter
            .on('error', error => this._errorListener(error))
            .on('setConfig',
                (bucket, config) => this._setConfigListener(bucket, config))
            .on('getConfig', bucket => this._getConfigListener(bucket))
            .on('listConfigs', () => this._listConfigsListener())
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

    _getConfigDataFromBuffer(data, bucket) {
        const { error, result } = safeJsonParse(data);
        if (error) {
            this.log.error('invalid config', { error, config: data, bucket });
            return undefined;
        }
        return result;
    }

    _getBucketNotifConfig(bucket, cb) {
        const method
            = 'ZookeeperConfigManager._getBucketNotifConfig';
        const zkPath = this._getBucketNodeZkPath(bucket);
        this.log.debug('fetching bucket notification configuration', {
            method,
            bucket,
            zkPath,
        });
        this._warnZkWatchersLeak('data', zkPath);
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
        this.log.debug('setting bucket notification configuration', {
            method,
            bucket,
            zkPath,
        });
        return async.waterfall([
            next => this._checkNodeExists(zkPath, next),
            (exists, next) => {
                if (exists) {
                    return this._zkClient.setData(zkPath, Buffer.from(data), -1, next);
                } else {
                    return this._createBucketNotifConfigNode(bucket, data, next);
                }
            }
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

    _createBucketNotifConfigNode(bucket, data, cb) {
        const method
            = 'ZookeeperConfigManager._createBucketNotifConfigNode';
        const zkPath = this._getBucketNodeZkPath(bucket);
        this.log.debug('creating bucket notification configuration node', {
            method,
            bucket,
            zkPath,
        });
        // mkdirp to ensure parent path exists,
        // then atomically create the znode while setting data immediately
        // to avoid other watchers to read the znode because data is set at creation
        return this._zkClient.mkdirpWithChildDataOnly(zkPath, Buffer.from(data), err => {
            if (err) {
                this.log.error('Could not pre-create path in zookeeper', {
                    method,
                    zkPath,
                    error: err,
                });
                return this._callbackHandler(cb, err);
            }
            // if znode is created, run getData to set a watcher on the bucket config
            // in case another node becomes leader on the raft and modifies the config
            // while the current process keeps running
            return this._updateLocalStore([bucket], cb => this._callbackHandler(cb));
        });
    }

    _removeBucketNotifConfigNode(bucket, cb) {
        const method
            = 'ZookeeperConfigManager._removeBucketNotifConfigNode';
        const zkPath = this._getBucketNodeZkPath(bucket);
        this.log.debug('removing bucket notification configuration node', {
            method,
            bucket,
            zkPath,
        });
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
        this._warnZkWatchersLeak('child', zkPath);
        this._zkClient.getChildren(zkPath, event => {
            this.log.debug('zookeeper getChildren watcher triggered', {
                zkPath,
                method,
                event,
            });
            if (event.type === zookeeper.Event.NODE_CHILDREN_CHANGED) {
                this._emitter.emit('listConfigs');
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
            this.log.debug('list of buckets', {
                zkPath,
                method,
                buckets,
            });
            this._callbackHandler(cb, null, buckets);
        });
    }

    _updateLocalStore(buckets, cb) {
        async.eachLimit(buckets, this._zkConcurrency, (bucket, next) => {
            this._getBucketNotifConfig(bucket, (err, data) => {
                if (err) {
                    return next(err);
                }
                const configObject = this._getConfigDataFromBuffer(data, bucket);
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
     * @param {boolean} [emitToZk = true] - whether to emit the event to zookeeper
     * @return {boolean} - true if removed
     */
    removeConfig(bucket, emitToZk = true) {
        try {
            this.log.debug('remove config', {
                method: 'ZookeeperConfigManager.removeConfig',
                bucket,
                emitToZk,
            });
            this._configs.delete(bucket);
            if (emitToZk) {
                this._emitter.emit('removeConfig', bucket);
            }
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

    _setupZookeeper(done) {
        if (this._zkClient) {
            done();
            return;
        }

        const zookeeperUrl =
            `${this._zkConfig.connectionString}${this._zkPath}`;
        this.log.info('opening zookeeper connection for reading ' +
            'bucket notification configuration', {
                zookeeperUrl,
                method: 'ZookeeperConfigManager._setupZookeeper',
            });
        this._zkClient = new ZookeeperManager(zookeeperUrl, {
            autoCreateNamespace: this._zkConfig.autoCreateNamespace,
            retries: this._zkConfig.retries,
        }, this.log);

        this._zkClient.once('error', done);
        this._zkClient.once('ready', () => {
            // just in case there would be more 'error' events emitted
            this._zkClient.removeAllListeners('error');
            done();
        });
    }

    /**
     * Setup bucket notification configuration manager
     *
     * @param {function} [cb] - callback
     * @return {undefined}
     */
    setup(cb) {
        async.waterfall([
            next => this._setupZookeeper(next),
            next => this._checkConfigurationParentNode(next),
            (_, next) => this._listBucketsWithConfig(next),
            (buckets, next) => this._updateLocalStore(buckets, next),
        ], cb);
    }
}

module.exports = ZookeeperConfigManager;
