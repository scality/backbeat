const assert = require('assert');

const { isMasterKey } = require('arsenal').versioning;
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
     * @param {Object} params.bnConfigManager - bucket notification config
     * manager
     */
    constructor(params) {
        super(params);
        this.notificationConfig = params.config;
        this.bnConfigManager = params.bnConfigManager;
        assert(this.bnConfigManager, 'bucket notification configuration manager'
            + ' is not set');
    }

    /**
     * Check if bucket entry based on bucket and key params
     *
     * @param {String} bucket - bucket
     * @param {String} key - object key
     * @return {boolean} - true if bucket entry
     */
    _isBucketEntry(bucket, key) {
        return ((bucket.toLowerCase() === notifConstants.bucketMetastore && key)
            || key === undefined);
    }

    /**
     * Get bucket attributes from log entry
     *
     * @param {Object} value - log entry object
     * @return {Object|undefined} - bucket attributes if available
     */
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

    /**
     * Get bucket name from bucket attributes
     *
     * @param {Object} value - log entry object
     * @return {String|undefined} - bucket name if available
     */
    _getBucketNameFromAttributes(value) {
        const attributes = this._getBucketAttributes(value);
        if (attributes && attributes.name) {
            return attributes.name;
        }
        return undefined;
    }

    /**
     * Get notification configuration from bucket attributes
     *
     * @param {Object} value - log entry object
     * @return {Object|undefined} - notification configuration if available
     */
    _getBucketNotificationConfiguration(value) {
        const attributes = this._getBucketAttributes(value);
        if (attributes && attributes.notificationConfiguration) {
            return attributes.notificationConfiguration;
        }
        return undefined;
    }

    /**
     * Extract base key from versioned key
     *
     * @param {String} key - object key
     * @return {String} - versioned base key
     */
    _extractVersionedBaseKey(key) {
        return key.split(VID_SEP)[0];
    }

    /**
     * Process bucket entry from the log
     *
     * @param {Object} value - log entry object
     * @return {undefined}
     */
    _processBucketEntry(value) {
        const bucketName = this._getBucketNameFromAttributes(value);
        const notificationConfiguration
            = this._getBucketNotificationConfiguration(value);
        if (notificationConfiguration &&
            Object.keys(notificationConfiguration).length > 0) {
            const bnConfig = {
                bucket: bucketName,
                notificationConfiguration,
            };
            // bucket notification config is available, update node
            this.bnConfigManager.setConfig(bucketName, bnConfig);
            return undefined;
        }
        // bucket notification conf has been removed, so remove zk node
        return this.bnConfigManager.removeConfig(bucketName);
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
    _processObjectEntry(bucket, key, value, type) {
        const versionId = value.versionId || null;
        if (!isMasterKey(key)) {
            return undefined;
        }
        const config = this.bnConfigManager.getConfig(bucket);
        if (config && Object.keys(config).length > 0) {
            const { eventMessageProperty }
                = notifConstants;
            let eventType
                = value[eventMessageProperty.eventType];
            if (eventType === undefined && type === 'del') {
                eventType = notifConstants.deleteEvent;
            }
            const ent = {
                bucket,
                key,
                eventType,
                versionId,
            };
            this.log.debug('validating entry', {
                method: 'NotificationQueuePopulator._processObjectEntry',
                bucket,
                key,
                eventType,
            });
            if (configUtil.validateEntry(config, ent)) {
                const message
                    = messageUtil.addLogAttributes(value, ent);
                this.log.info('publishing message', {
                    method: 'NotificationQueuePopulator._processObjectEntry',
                    bucket,
                    key: message.key,
                    eventType,
                    eventTime: message.dateTime,
                });
                this.publish(this.notificationConfig.topic,
                    `${bucket}/${key}`,
                    JSON.stringify(message));
            }
            return undefined;
        }
        // skip if there is no bucket notification configuration
        return undefined;
    }

    /**
     * filter
     *
     * @param {Object} entry - log entry
     * @return {undefined}
     */
    filter(entry) {
        const { bucket, key, type } = entry;
        const value = entry.value || '{}';
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
        if (bucket && result && this._isBucketEntry(bucket, key)) {
            return this._processBucketEntry(result);
        }
        // object entry processing - filter and publish
        if (key && result) {
            return this._processObjectEntry(bucket, key, result, type);
        }
        return undefined;
    }
}

module.exports = NotificationQueuePopulator;
