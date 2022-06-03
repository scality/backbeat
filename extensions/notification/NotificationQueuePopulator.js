const assert = require('assert');

const { isMasterKey } = require('../../lib/util/versioning');
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
     * Process object entry from the log
     *
     * @param {String} bucket - bucket
     * @param {String} key - object key
     * @param {Object} value - log entry object
     * @param {String} type - entry type
     * @return {undefined}
     */
    async _processObjectEntry(bucket, key, value, type) {
        const versionId = value.versionId || null;
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
     * @return {Promise|undefined} Promise|undefined
     */
    async filter(entry) {
        const { bucket, key, type } = entry;
        const value = entry.value || '{}';
        const { error, result } = safeJsonParse(value);
        // ignore if entry's value is not valid
        if (error) {
            this.log.error('could not parse log entry', { value, error });
            return undefined;
        }
        // ignore bucket operations, mpu's or if the entry has no bucket
        const isUserBucketOp = !bucket || bucket === usersBucket;
        const isMpuOp = key && key.startsWith(mpuBucketPrefix);
        const isBucketOp = bucket && result && this._isBucketEntry(bucket, key);
        if ([isUserBucketOp, isMpuOp, isBucketOp].some(cond => cond)) {
            return undefined;
        }
        // object entry processing - filter and publish
        if (key && result) {
            return this._processObjectEntry(bucket, key, result, type);
        }
        return undefined;
    }
}

module.exports = NotificationQueuePopulator;
