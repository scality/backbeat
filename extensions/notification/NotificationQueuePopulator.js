const assert = require('assert');
const util = require('util');

const { isMasterKey } = require('arsenal').versioning;
const { usersBucket, mpuBucketPrefix, supportedNotificationEvents } = require('arsenal').constants;
const VID_SEP = require('arsenal').versioning.VersioningConstants.VersionId.Separator;
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
        this._metricsStore = params.metricsHandler;
    }

    /**
     * Check if bucket entry based on bucket and key params
     *
     * @param {String} bucket - bucket
     * @param {String} key - object key
     * @return {boolean} - true if bucket entry
     */
    _isBucketEntry(bucket, key) {
        return ((bucket.toLowerCase() === this.notificationConfig.bucketMetastore && !!key)
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
     * Process bucket entry from the log
     *
     * @param {string} bucket - bucket name from log entry
     * @param {Object} value - log entry object
     * @return {undefined}
     */
    _processBucketEntry(bucket, value) {
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
        // bucket was deleter or notification conf has been removed, so remove zk node
        return this.bnConfigManager.removeConfig(bucketName || bucket);
    }

    /**
     * Returns the correct versionId
     * to display according to the
     * versioning state of the object
     * @param {Object} value log entry object
     * @param {Object} overheadFields - extra fields
     * @param {Object} overheadFields.versionId - object versionId
     * @return {String} versionId
     */
    _getVersionId(value, overheadFields) {
        const versionId = value.versionId || (overheadFields && overheadFields.versionId);
        const isNullVersion = value.isNull;
        const isVersioned = !!versionId;
        // Versioning suspended objects have
        // a versionId, however it is internal
        // and should not be used to get the object
        if (isNullVersion || !isVersioned) {
            return null;
        } else {
            return versionId;
        }
    }

    /**
     * Decides if we should process the entry.
     * Since we get both master and version events,
     * we need to avoid pushing two notifications for
     * the same event.
     * - For non versiond buckets, we process the master
     * objects' events.
     * - For versioned buckets, we process version events
     * and ignore all master events.
     * - For versioning suspended buckets, we need to process
     * both master and version events, as the master is considered
     * a separate version.
     * @param {String} key object key
     * @param {Object} value object metadata
     * @return {boolean} - true if entry is valid
     */
    _shouldProcessEntry(key, value) {
        const isMaster = isMasterKey(key);
        const isVersionedMaster = isMaster && !!value.versionId;
        const isNullVersion = isMaster && value.isNull;
        if (!isMaster
            || !isVersionedMaster
            || isNullVersion
            ) {
            return true;
        }
        return false;
    }

    /**
     * Notification rules are normally verified when setting the notification
     * configuration (even for wildcards), however we need an explicit check at
     * this level to filter out non standard events that might be valid for one
     * of the wildcard rules set.
     * @param {String} eventType - notification event type
     * @returns {boolean} - true if notification is supported
     */
    _isNotificationEventSupported(eventType) {
        return supportedNotificationEvents.has(eventType);
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
     * Returns the dateTime of the event
     * based on the event message property if existent
     * or overhead fields
     * @param {ObjectMD} value object metadata
     * @param {Object} overheadFields overhead fields
     * @param {Object} overheadFields.commitTimestamp - Kafka commit timestamp
     * @param {Object} overheadFields.opTimestamp - MongoDB operation timestamp
     * @returns {string} dateTime of the event
     */
    _getEventDateTime(value, overheadFields) {
        if (overheadFields) {
            return overheadFields.opTimestamp || overheadFields.commitTimestamp || null;
        }
        return (value && value[notifConstants.eventMessageProperty.dateTime]) || null;
    }

    /**
     * Process object entry from the log
     *
     * @param {String} bucket - bucket
     * @param {String} key - object key
     * @param {Object} value - log entry object
     * @param {String} type - log entry type
     * @param {Object} overheadFields - extra fields
     * @param {Object} overheadFields.commitTimestamp - Kafka commit timestamp
     * @param {Object} overheadFields.opTimestamp - MongoDB operation timestamp
     * @return {undefined}
     */
    async _processObjectEntry(bucket, key, value, type, overheadFields) {
        this._metricsStore.notifEvent();
        if (!this._shouldProcessEntry(key, value)) {
            return undefined;
        }
        const { eventMessageProperty, deleteEvent } = notifConstants;
        let eventType = value[eventMessageProperty.eventType];
        if (eventType === undefined && type === 'del') {
            eventType = deleteEvent;
        }
        if (!this._isNotificationEventSupported(eventType)) {
            return undefined;
        }
        const baseKey = this._extractVersionedBaseKey(key);
        const versionId = this._getVersionId(value, overheadFields);
        const dateTime = this._getEventDateTime(value, overheadFields);
        const config = await this.bnConfigManager.getConfig(bucket);
        if (config && Object.keys(config).length > 0) {
            const ent = {
                bucket,
                key: baseKey,
                eventType,
                versionId,
                dateTime,
            };
            this.log.debug('validating entry', {
                method: 'NotificationQueuePopulator._processObjectEntry',
                bucket,
                key,
                eventType,
            });
            const pushedToTopic = new Map();
            // validate and push kafka message foreach destination topic
            this.notificationConfig.destinations.forEach(destination => {
                const topic = destination.internalTopic ||
                    this.notificationConfig.topic;
                // avoid pushing a message multiple times to the
                // same internal topic
                if (pushedToTopic[topic]) {
                    return undefined;
                }
                // get destination specific notification config
                const queueConfig = config.notificationConfiguration.queueConfig.filter(
                    c => c.queueArn.split(':').pop() === destination.resource
                );
                if (!queueConfig.length) {
                    // skip, if there is no config for the current
                    // destination resource
                    return undefined;
                }
                // pass only destination resource specific config to
                // validate entry
                const destConfig = {
                    bucket,
                    notificationConfiguration: {
                        queueConfig,
                    },
                };
                const { isValid, matchingConfig } = configUtil.validateEntry(destConfig, ent);
                if (isValid) {
                    const message
                        = messageUtil.addLogAttributes(value, ent);
                    this.log.info('publishing message', {
                        method: 'NotificationQueuePopulator._processObjectEntry',
                        bucket,
                        key: message.key,
                        versionId,
                        eventType,
                        eventTime: message.dateTime,
                        matchingConfig,
                    });
                    this.publish(topic,
                        // keeping all messages for same object
                        // in the same partition to keep the order.
                        // here we use the object name and not the
                        // "_id" which also includes the versionId
                        `${bucket}/${message.key}`,
                        JSON.stringify(message));
                    // keep track of internal topics we have pushed to
                    pushedToTopic[topic] = true;
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
        if (this.notificationConfig.ignoreEmptyEvents && !entry.value) {
            return cb();
        }
        const { bucket, key, type, overheadFields } = entry;
        const value = entry.value || '{}';
        const { error, result } = safeJsonParse(value);
        // ignore if entry's value is not valid
        if (error) {
            this.log.error('could not parse log entry', { value, error });
            return cb();
        }
        // ignore bucket op, mpu's or if the entry has no bucket
        if (!bucket || bucket === usersBucket || (key && key.startsWith(mpuBucketPrefix))) {
            return cb();
        }
        // bucket notification configuration updates
        if (bucket && result && this._isBucketEntry(bucket, key)) {
            this._processBucketEntry(key, result);
            return cb();
        }
        // object entry processing - filter and publish
        if (key && result) {
            return this._processObjectEntryCb(bucket, key, result, type, overheadFields, cb);
        }
        return cb();
    }
}

module.exports = NotificationQueuePopulator;
