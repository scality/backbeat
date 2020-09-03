const async = require('async');

const { isMasterKey } = require('arsenal/lib/versioning/Version');
const { usersBucket, mpuBucketPrefix } = require('arsenal').constants;
const VID_SEP = require('arsenal').versioning.VersioningConstants
    .VersionId.Separator;

const configUtil = require('./utils/config');
const safeJsonParse = require('./utils/safeJsonParse');
const messageUtil = require('./utils/message');
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
            return done();
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
            return done();
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
            return done();
        });
    }

    _isBucketEntry(bucket, key) {
        return ((bucket.toLowerCase() === 'metastore' && key)
            || key === undefined);
    }

    _getBucketAttributes(value) {
        if (value && value.attributes) {
            const { error, result } = safeJsonParse(value.attributes);
            if (error) {
                return undefined;
            }
            return result;
        }
        return undefined;
    }

    _getBucketNameFromAttributes(value) {
        const attributes = this._getBucketAttributes(value);
        if (attributes && attributes.name) {
            return attributes.name;
        }
        return undefined;
    }

    _getBucketNotificationConfiguration(value) {
        const attributes = this._getBucketAttributes(value);
        if (attributes && attributes.notificationConfiguration) {
            return attributes.notificationConfiguration;
        }
        return undefined;
    }

    _extractVersionedBaseKey(key) {
        return key.split(VID_SEP)[0];
    }

    /**
     * Asynchronous filter
     * @param {Object} entry - constructor params
     * @param {function} cb - callback
     * @return {undefined}
     */
    filterAsync(entry, cb) {
        const { bucket, key, value, type } = entry;
        const { error, result } = safeJsonParse(value);
        // ignore if entry's value is not valid
        if (error) {
            this.log.error('could not parse log entry', { value, error });
            return cb();
        }
        // ignore bucket op, mpu's or if the entry has no bucket
        if (!bucket || bucket === usersBucket ||
            (key && key.startsWith(mpuBucketPrefix))) {
            return cb();
        }
        // bucket notification configuration updates
        if (bucket && result && this._isBucketEntry(bucket, key)) {
            const bucketName = this._getBucketNameFromAttributes(result);
            const notificationConfiguration
                = this._getBucketNotificationConfiguration(result);
            if (notificationConfiguration &&
                Object.keys(notificationConfiguration).length > 0) {
                // bucket notification config is available, update node
                return async.waterfall([
                    next => this._getBucketNotifConfig(bucketName, next),
                    (config, next) => {
                        // config: 0 - node does not exists, create one
                        if (config === 0) {
                            return this._createBucketNotifConfigNode(
                                bucketName, next);
                        }
                        return next();
                    },
                    next => {
                        const bnConfig = {
                            bucket: bucketName,
                            notificationConfiguration,
                        };
                        return this._setBucketNotifConfig(
                            bucketName, JSON.stringify(bnConfig), next);
                    },
                ], error => {
                    if (error) {
                        const errMsg = 'error setting bucket notification '
                            + 'configuration';
                        this.log.error(errMsg, { error });
                    }
                    return cb();
                });
            }
            // bucket notification conf has been removed, so remove zk node
            return async.waterfall([
                next => this._getBucketNotifConfig(bucketName, next),
                (config, next) => {
                    // config: 0 - node does not exists, continue
                    if (config === 0) {
                        return next();
                    }
                    return this._removeBucketNotifConfigNode(bucketName, next);
                },
            ], error => {
                if (error) {
                    const errMsg = 'error removing bucket notification '
                        + 'configuration';
                    this.log.err(errMsg, { error });
                }
                return cb();
            });
        }
        // object entry processing - filter and publish
        if (key && result) {
            let versionId = null;
            let objectKey = key;
            if (!isMasterKey(entry.key)) {
                objectKey = this._extractVersionedBaseKey(key);
                versionId = result.versionId;
            }
            return async.waterfall([
                next => this._getBucketNotifConfig(bucket, next),
                (config, next) => {
                    if (config && Object.keys(config).length > 0) {
                        const { eventMessageProperty }
                            = notifConstants;
                        let eventType
                            = result[eventMessageProperty.eventType];
                        if (eventType === undefined && type === 'del') {
                            eventType = notifConstants.deleteEvent;
                        }
                        const ent = {
                            bucket,
                            key: objectKey,
                            eventType,
                            versionId,
                        };
                        if (configUtil.validateEntry(config, ent)) {
                            const message
                                = messageUtil.addLogAttributes(result, ent);
                            this.publish(this.notificationConfig.topic,
                                bucket,
                                JSON.stringify(message));
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
                        key: objectKey,
                        error,
                    });
                }
                return cb();
            });
        }
        return cb();
    }
}

module.exports = NotificationQueuePopulator;
