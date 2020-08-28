const async = require('async');

const { usersBucket, mpuBucketPrefix } = require('arsenal').constants;

const configUtil = require('./utils/config');
const safeJsonParse = require('./utils/safeJsonParse');
const notifConstants = require('./constants');

const QueuePopulatorExtension =
    require('../../lib/queuePopulator/QueuePopulatorExtension');

class NotificationQueuePopulator extends QueuePopulatorExtension {
    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.config - notification configuration object
     * @param {Logger} params.logger - logger object
     */
    constructor(params) {
        super(params);
        this.notificationConfig = params.config;
        this.zkClient = params.zkClient;
    }

    _getBucketNodeZkPath(bucket) {
        const { zkBucketNotificationPath, zkConfigParentNode }
            = notifConstants;
        return `/${zkBucketNotificationPath}/${zkConfigParentNode}/${bucket}`;
    }

    _getBucketNotifConfig(bucket, done) {
        const method
            = 'NotificationQueuePopulator._getBucketNotifConfig';
        const zkPath = this._getBucketNodeZkPath(bucket);
        return this.zkClient.getData(zkPath, (err, data) => {
            if (err && err.name !== 'NO_NODE') {
                const errMsg
                    = 'error fetching bucket notification configuration';
                this.log.error(errMsg, {
                    method,
                    error: err,
                });
                return done(err);
            }
            if (data) {
                const { error, result } = safeJsonParse(data);
                if (error) {
                    this.log.error('invalid config', { method, zkPath, data });
                    return done(null, 1);
                }
                this.log.debug('fetched bucket notification configuration', {
                    method,
                    zkPath,
                    data: result,
                });
                return done(null, result);
            }
            return done(null, 0);
        });
    }

    _setBucketNotifConfig(bucket, data, done) {
        const method
            = 'NotificationQueuePopulator._setBucketNotifConfig';
        const zkPath = this._getBucketNodeZkPath(bucket);
        return this.zkClient.setData(zkPath, Buffer.from(data), -1, err => {
            if (err) {
                this.log.error('error saving config', { method, zkPath, data });
                return done(err);
            }
            this.log.debug('config saved', { method, zkPath, data });
            return done(null, 1);
        });
    }

    _createBucketNotifConfigNode(bucket, done) {
        const method
            = 'NotificationQueuePopulator._createBucketNotifConfigNode';
        const zkPath = this._getBucketNodeZkPath(bucket);
        return this.zkClient.mkdirp(zkPath, err => {
            if (err) {
                this.log.error('Could not pre-create path in zookeeper', {
                    method,
                    zkPath,
                    error: err,
                });
                return done(err);
            }
            return done(null, 1);
        });
    }

    _removeBucketNotifConfigNode(bucket, done) {
        const method
            = 'NotificationQueuePopulator._removeBucketNotifConfigNode';
        const zkPath = this._getBucketNodeZkPath(bucket);
        return this.zkClient.remove(zkPath, err => {
            if (err && err.name !== 'NO_NODE') {
                this.log.error('Could not remove zookeeper node', {
                    method,
                    zkPath,
                    error: err,
                });
                return done(err);
            }
            if (!err) {
                const errMsg
                    = 'removed notification configuration zookeeper node';
                this.log.info(errMsg, {
                    method,
                    bucket,
                });
            }
            return done(null, 1);
        });
    }

    filter(entry) {
        const { bucket, key, value } = entry;
        const { error, result } = safeJsonParse(value);
        // ignore if entry's value is not valid
        if (error) {
            this.log.error('could not parse log entry', { value, error });
            return undefined;
        }
        // ignore bucket op, mpu's or if the entry has no bucket
        if (!bucket || bucket === usersBucket ||
            (key && key.startsWith(mpuBucketPrefix))) {
            return undefined;
        }
        // bucket notification configuration updates
        if (key === undefined) {
            if (bucket && result) {
                const notificationConfiguration
                    = result.notificationConfiguration;
                if (notificationConfiguration &&
                    Object.keys(notificationConfiguration).length > 0) {
                    // bucket notification config is available, update node
                    return async.waterfall([
                        next => this._getBucketNotifConfig(bucket, next),
                        (config, next) => {
                            // config: 0 - node does not exists, create one
                            if (config === 0) {
                                return this._createBucketNotifConfigNode(
                                    bucket, next);
                            }
                            return next();
                        },
                        next => {
                            const bnConfig = {
                                bucket,
                                notificationConfiguration,
                            };
                            return this._setBucketNotifConfig(
                                bucket, JSON.stringify(bnConfig), next);
                        },
                    ], error => {
                        if (error) {
                            const errMsg = 'error setting bucket notification '
                                + 'configuration';
                            this.log.error(errMsg, { error });
                        }
                        return undefined;
                    });
                }
                // bucket notification conf has been removed, so remove zk node
                return async.waterfall([
                    next => this._getBucketNotifConfig(bucket, next),
                    (config, next) => {
                        // config: 0 - node does not exists, continue
                        if (config === 0) {
                            return next();
                        }
                        return this._removeBucketNotifConfigNode(bucket, next);
                    },
                ], error => {
                    if (error) {
                        const errMsg = 'error removing bucket notification '
                            + 'configuration';
                        this.log.err(errMsg, { error });
                    }
                    return undefined;
                });
            }
        }

        // object entry processing - filter and publish
        if (key && result) {
            // TODO: handle versioned object entry
            return async.waterfall([
                next => this._getBucketNotifConfig(bucket, next),
                (config, next) => {
                    if (config && Object.keys(config).length > 0) {
                        const bnConfig = safeJsonParse(config);
                        if (bnConfig.error) {
                            // skip, invalid configuration
                            return next();
                        }
                        const type
                            = value[notifConstants.notificationEventPropName];
                        const lastModified
                            = value[notifConstants.eventTimePropName];
                        // TODO: publish necessary object properties, keeping it
                        // simple for first iteration
                        const ent = { bucket, key, type, lastModified };
                        if (configUtil.validateEntry(bnConfig.result, ent)) {
                            this.publish(this.notificationConfig.topic,
                                bucket,
                                JSON.stringify(ent));
                        }
                        return next();
                    }
                    // skip if there is no bucket notification configuration
                    return next();
                },
            ], error => {
                if (error) {
                    this.log.err('error processing entry', {
                        bucket,
                        key,
                        error,
                    });
                }
                return undefined;
            });
        }
        return undefined;
    }
}

module.exports = NotificationQueuePopulator;
