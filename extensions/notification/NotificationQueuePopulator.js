const assert = require('assert');
const util = require('util');

const { isMasterKey } = require('arsenal').versioning;
const { usersBucket, mpuBucketPrefix } = require('arsenal').constants;
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
     * @param {Object} params.bnConfigManager - bucket notification config
     * manager
     */
    constructor(params) {
        super(params);
        this.notificationConfig = params.config;
        this.bnConfigManager = params.bnConfigManager;
        assert(this.bnConfigManager, 'bucket notification configuration manager'
            + ' is not set');
        // callbackify functions
        this._processObjectEntryCb = util.callbackify(this._processObjectEntry).bind(this);
    }

    /**
     * Check if bucket entry based on bucket and key params
     *
     * @param {String} bucket - bucket
     * @param {String} key - object key
     * @return {boolean} - true if bucket entry
     */
    _isBucketEntry(bucket, key) {
        return ((bucket.toLowerCase() === notifConstants.bucketMetastore && !!key)
            || key === undefined);
    }

    /**
     * Returns the correct versionId
     * to display according to the
     * versioning state of the object
     * @param {Object} value log entry object
     * @return {String} versionId
     */
    _getVersionId(value) {
        const isNullVersion = value.isNull;
        const isVersioned = !!value.versionId;
        // Versioning suspended objects have
        // a versionId, however it is internal
        // and should not be used to get the object
        if (isNullVersion || !isVersioned) {
            return null;
        } else {
            return value.versionId;
        }
    }

    /**
     * Process object entry from the log
     *
     * @param {String} bucket - bucket
     * @param {String} key - object key
     * @param {Object} value - log entry object
     * @param {String} type - entry type
     * @return {undefined}
     */
    async _processObjectEntry(bucket, key, value, type) {
        const versionId = this._getVersionId(value);
        if (!isMasterKey(key)) {
            return undefined;
        }
        const config = await this.bnConfigManager.getConfig(bucket);
        if (config && Object.keys(config).length > 0) {
            const { eventMessageProperty }
                = notifConstants;
            let eventType
                = value[eventMessageProperty.eventType];
            if (eventType === undefined && type === 'delete') {
                eventType = notifConstants.deleteEvent;
            }
            const ent = {
                bucket,
                key: value.key,
                eventType,
                versionId,
            };
            this.log.debug('validating entry', {
                method: 'NotificationQueuePopulator._processObjectEntry',
                bucket,
                key,
                eventType,
            });
            // validate and push kafka message foreach destination topic
            this.notificationConfig.destinations.forEach(destination => {
                // get destination specific notification config
                const destBnConf = config.queueConfig.find(
                    c => c.queueArn.split(':').pop()
                        === destination.resource);
                if (!destBnConf) {
                    // skip, if there is no config for the current
                    // destination resource
                    return undefined;
                }
                // pass only destination resource specific config to
                // validate entry
                const bnConfig = {
                    queueConfig: [destBnConf],
                };
                if (configUtil.validateEntry(bnConfig, ent)) {
                    const message
                        = messageUtil.addLogAttributes(value, ent);
                    this.log.info('publishing message', {
                        method: 'NotificationQueuePopulator._processObjectEntry',
                        bucket,
                        key: message.key,
                        versionId,
                        eventType,
                        eventTime: message.dateTime,
                    });
                    const internalTopic = destination.internalTopic ||
                        this.notificationConfig.topic;
                    this.publish(internalTopic,
                        // keeping all messages for same object
                        // in the same partition to keep the order.
                        // here we use the object name and not the
                        // "_id" which also includes the versionId
                        `${bucket}/${message.key}`,
                        JSON.stringify(message));
                }
                return undefined;
            });
        }
        // skip if there is no bucket notification configuration
        return undefined;
    }

    /**
     * filter
     *
     * @param {Object} entry - log entry
     * @param {Function} cb - callback
     * @return {undefined} Promise|undefined
     */
    filterAsync(entry, cb) {
        const { bucket, key, type } = entry;
        const value = entry.value || '{}';
        const { error, result } = safeJsonParse(value);
        // ignore if entry's value is not valid
        if (error) {
            this.log.error('could not parse log entry', { value, error });
            return cb();
        }
        // ignore bucket operations, mpu's or if the entry has no bucket
        const isUserBucketOp = !bucket || bucket === usersBucket;
        const isMpuOp = key && key.startsWith(mpuBucketPrefix);
        const isBucketOp = bucket && result && this._isBucketEntry(bucket, key);
        if ([isUserBucketOp, isMpuOp, isBucketOp].some(cond => cond)) {
            return cb();
        }
        // object entry processing - filter and publish
        if (key && result) {
            return this._processObjectEntryCb(bucket, key, result, type, cb);
        }
        return cb();
    }
}

module.exports = NotificationQueuePopulator;
