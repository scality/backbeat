const async = require('async');
const { constants } = require('arsenal');
const { scaleMsPerDay } = require('arsenal').s3middleware.objectUtils;
const { encode } = require('arsenal').versioning.VersionID;
const { isMasterKey } = require('arsenal').versioning;
const { mpuBucketPrefix } = constants;
const QueuePopulatorExtension =
    require('../../lib/queuePopulator/QueuePopulatorExtension');
const { authTypeAssumeRole } = require('../../lib/constants');
const { v4: uuid } = require('uuid');
const safeJsonParse = require('../../lib/util/safeJsonParse');
const { LifecycleMetrics } = require('./LifecycleMetrics');
const LIFECYCLE_BUCKETS_ZK_PATH = '/data/buckets';
const LIFEYCLE_POPULATOR_CLIENT_ID = 'lifecycle-populator';
const METASTORE = '__metastore';
const VaultClientWrapper = require('../utils/VaultClientWrapper');

const config = require('../../lib/Config');
const {
    coldStorageRestoreAdjustTopicPrefix,
    coldStorageRestoreTopicPrefix,
    coldStorageGCTopicPrefix,
    coldStorageArchiveTopicPrefix,
} = config.extensions.lifecycle;
const BackbeatProducer = require('../../lib/BackbeatProducer');
const locations = require('../../conf/locationConfig.json') || {};

const nSecsPerDay = () => Math.ceil(scaleMsPerDay(config.timeOptions.timeProgressionFactor) / 1000);

class LifecycleQueuePopulator extends QueuePopulatorExtension {

    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.config - extension-specific configuration object
     * @param {Logger} params.logger - logger object
     * @param {Object} params.locationConfigs - location configurations
     */
    constructor(params) {
        super(params);
        this._authConfig = params.authConfig;

        if (this._authConfig?.type === authTypeAssumeRole) {
            this.vaultClientWrapper = new VaultClientWrapper(
                LIFEYCLE_POPULATOR_CLIENT_ID,
                params.vaultAdmin,
                this._authConfig,
                this.log,
            );

            this.vaultClientWrapper.init();
        }

        this.kafkaConfig = params.kafkaConfig;
        this.locationConfigs = params.locationConfigs || locations;
        this._producers = {};
    }

    _setupProducer(topic, done) {
        if (this._producers[topic] !== undefined) {
            return done();
        }
        const producer = new BackbeatProducer({
            kafka: { hosts: this.kafkaConfig.hosts },
            maxRequestSize: this.kafkaConfig.maxRequestSize,
            compressionType: this.kafkaConfig.compressionType,
            requiredAcks: this.kafkaConfig.requiredAcks,
            producerParams: this.kafkaConfig.producerParams,
            topic,
        });
        producer.once('error', done);
        producer.once('ready', () => {
            this.log.debug('producer is ready',
                {
                    kafkaConfig: this.kafkaConfig,
                    topic,
                });
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this.log.error('error from backbeat producer', { topic, error: err });
            });
            this._producers[topic] = producer;
            return done();
        });
        return undefined;
    }

    /**
     * Setup a producer for each cold location
     *
     * @param {function} cb - callback function
     * @return {undefined}
     */
    setupProducers(cb) {
        const locConfigs = this.locationConfigs;
        // eslint-disable-next-line no-unused-vars
        const coldLocations = Object.entries(locConfigs).filter(([key, val]) => val.isCold).map(([key]) => key);
        if (coldLocations.length > 0) {
            return async.eachLimit(coldLocations, 10, (location, done) => {
                async.series([
                    next => this._setupProducer(`${coldStorageRestoreAdjustTopicPrefix}${location}`, next),
                    next => this._setupProducer(`${coldStorageRestoreTopicPrefix}${location}`, next),
                    next => this._setupProducer(`${coldStorageGCTopicPrefix}${location}`, next),
                    next => this._setupProducer(`${coldStorageArchiveTopicPrefix}${location}`, next),
                ], done);
            }, cb);
        } else {
            this.log.info('No cold locations found to setup producers', {
                method: 'LifecycleQueuePopulator.setupProducers',
            });
            return cb();
        }
    }

    /**
     * Pre-create the zookeeper path for bucket lifecycle nodes, if necessary.
     * @param {Function} cb - The callback function.
     * @return {undefined}
     */
    createZkPath(cb) {
        const { zookeeperPath } = this.extConfig;
        const path = `${zookeeperPath}${LIFECYCLE_BUCKETS_ZK_PATH}`;
        return this.zkClient.getData(path, err => {
            if (err) {
                if (err.name !== 'NO_NODE') {
                    this.log.error('could not get zookeeper node path', {
                        method: 'LifecycleQueuePopulator.createZkPath',
                        error: err,
                    });
                    return cb(err);
                }
                return this.zkClient.mkdirp(path, err => {
                    if (err) {
                        this.log.error('could not create path in zookeeper', {
                            method: 'LifecycleQueuePopulator.createZkPath',
                            zookeeperPath,
                            error: err,
                        });
                        return cb(err);
                    }
                    return cb();
                });
            }
            return cb();
        });
    }

    _getBucketNodeZkPath(attributes) {
        const { zookeeperPath } = this.extConfig;
        return `${zookeeperPath}${LIFECYCLE_BUCKETS_ZK_PATH}/` +
            `${attributes.owner}:${attributes.uid}:${attributes.name}`;
    }

    /**
     * Create a new zookeeper node for any bucket that has a lifecycle
     * configuration. Remove the node if the bucket is deleted or the
     * configuration is deleted.
     * @param {Object} attributes - bucket attributes from metadata log
     * @param {String} attributes.owner - canonical ID of the bucket owner
     * @param {String} attributes.name - bucket name
     * @param {String} attributes.uid - bucket unique UID
     * @return {undefined}
     */
    _updateZkBucketNode(attributes) {
        const path = this._getBucketNodeZkPath(attributes);
        // Remove existing node if deleting the bucket or its configuration.
        if (attributes.deleted || !attributes.lifecycleConfiguration) {
            return this.zkClient.remove(path, err => {
                if (err && err.name !== 'NO_NODE') {
                    this.log.error('could not remove zookeeper node', {
                        method: 'LifecycleQueuePopulator._updateZkBucketNode',
                        zkPath: path,
                        error: err,
                    });
                }
                if (!err) {
                    this.log.info(
                        'removed lifecycle zookeeper watch node for bucket',
                        {
                            owner: attributes.owner,
                            bucket: attributes.name
                        });
                }
                return undefined;
            });
        }
        return this.zkClient.create(path, err => {
            if (err && err.name !== 'NODE_EXISTS') {
                this.log.error('could not create new zookeeper node', {
                    method: 'LifecycleQueuePopulator._updateZkBucketNode',
                    zkPath: path,
                    error: err,
                });
            }
            if (!err) {
                this.log.info(
                    'created lifecycle zookeeper watch node for bucket',
                    {
                        owner: attributes.owner,
                        bucket: attributes.name
                    });
            }
            return undefined;
        });
    }

    _isBucketEntryFromBucketd(entry) {
        // raft log entries
        // {
        //   bucket: bucket name
        //   key: no key value
        //   value: bucket value
        // }
        return entry.key === undefined && entry.bucket !== METASTORE;
    }

    _isBucketEntryFromFileMD(entry) {
        // file md log entries
        // {
        //   bucket: METASTORE
        //   key: bucket name
        //   value: bucket value
        // }
        return entry.bucket === METASTORE &&
            !(entry.key && entry.key.startsWith(mpuBucketPrefix));
    }

    _isVersionedObject(mdValue) {
        return mdValue.versionId !== undefined ||
            mdValue.isNull ||
            mdValue.isDeleteMarker;
    }

    /**
     * Parses a date object or string and returns a Date instance. Date in the oplog may be an
     * ISODate from MongoDB, or a fully serialized date: allow both cases to be on the safe side.
     * @param {Object|string} date - The date object or string to parse.
     * @returns {Date} The parsed Date instance.
     */
    _parseDate(date) {
        return new Date(date.$date || date);
    }

    _isColdLocation(locationName) {
        return !!this.locationConfigs[locationName]?.isCold;
    }

    /**
     * Whether a put entry designates an object we may act upon: bucket entries carry no object
     * key, mpu shadow bucket entries are internal, and the master entry of a versioned object
     * duplicates the version entry, which is processed on its own.
     *
     * @param {Object} entry - The record log entry from metadata.
     * @param {Object} value - The object metadata, already decoded by the caller.
     * @return {boolean} true if the entry may be dispatched to an object handler.
     */
    _isDistinctObjectEntry(entry, value) {
        if (!entry.key || entry.key.startsWith(mpuBucketPrefix)) {
            return false;
        }

        if (this._isVersionedObject(value) && isMasterKey(entry.key)) {
            this.log.trace('skip processing of object master entry');
            return false;
        }

        return true;
    }

    /**
     * Check if object transition was initiated by direct-to-cold request instead of lifecycle rule.
     *
     * @param {Object} md - The object metadata.
     * @return {boolean} true if a direct transition is pending for this object.
     */
    _isDirectToCold(md) {
        return md['x-amz-scal-transition-in-progress']
            && this._isColdLocation(md['x-amz-storage-class'])
            && !this._isColdLocation(md.dataStoreName)
            && !md.archive?.archiveInfo;
    }

    /**
     * Handle a "direct-to-cold" transition: cloudserver has written the object data to a hot
     * location, but the user requested a cold storage class in the PUT request. Cloudserver
     * flags the object as "transition in progress", and it is up to the queue populator to
     * trigger the archival, as there is no lifecycle rule (nor lifecycle scan) involved.
     *
     * The message published here is strictly the same as the one the lifecycle bucket processor
     * publishes (c.f. ReplicationAPI.sendDataMoverAction), so that the whole downstream pipeline
     * (Sorbet, cold status processor, garbage collector) is reused unchanged.
     *
     * @param {Object} entry - The record log entry from metadata.
     * @param {Object} [value] - The object metadata, already decoded by the caller.
     * @return {undefined}
     */
    _handleTransitionOp(entry, value) {
        if (!this.vaultClientWrapper) {
            return;
        }

        if (!this._isDistinctObjectEntry(entry, value)) {
            return;
        }

        if (!this._isDirectToCold(value)) {
            return;
        }

        const coldLocation = value['x-amz-storage-class'];
        const attemptHeader = value['x-amz-meta-scal-s3-transition-attempt'];
        const attempt = attemptHeader ? Number.parseInt(attemptHeader, 10) : undefined;
        const ownerId = value['owner-id'];
        this.vaultClientWrapper.getAccountId(ownerId, (err, accountId) => {
            if (err) {
                this.log.error('unable to get account', {
                    method: 'LifecycleQueuePopulator._handleTransitionOp',
                    ownerId,
                    err,
                });
                return;
            }

            this.log.trace(
                'publishing object transition entry',
                { bucket: entry.bucket, key: entry.key, version: value.versionId, coldLocation },
            );

            const topic = `${coldStorageArchiveTopicPrefix}${coldLocation}`;
            const key = `${entry.bucket}/${value.key}`;

            let version;
            if (value.versionId) {
                version = encode(value.versionId);
            }

            const transitionTime = this._parseDate(
                value['x-amz-scal-transition-time'] || value['last-modified']);
            const message = JSON.stringify({
                accountId,
                bucketName: entry.bucket,
                objectKey: value.key,
                objectVersion: version,
                requestId: uuid(),
                size: value['content-length'],
                eTag: `"${value['content-md5']}"`,
                try: attempt,
                transitionTime: transitionTime.toISOString(),
            });

            const producer = this._producers[topic];
            if (producer) {
                LifecycleMetrics.onLifecycleTriggered(this.log, 'queuePopulator', 'archive',
                    coldLocation, Date.now() - transitionTime.getTime());

                const kafkaEntry = { key, message };
                producer.send([kafkaEntry], err => {
                    LifecycleMetrics.onKafkaPublish(this.log, 'ColdStorageArchiveTopic', 'queuePopulator', err, 1);
                    if (err) {
                        this.log.error('error publishing object transition request entry', {
                            error: err,
                            method: 'LifecycleQueuePopulator._handleTransitionOp',
                        });
                    }
                });
            } else {
                this.log.error(`producer not available for location ${coldLocation}`, {
                    method: 'LifecycleQueuePopulator._handleTransitionOp',
                });
            }
        });
    }

    _handleRestoreOp(entry, value) {
        if (!this.vaultClientWrapper) {
            return;
        }

        if (!this._isDistinctObjectEntry(entry, value)) {
            return;
        }

        const locationName = value.dataStoreName;
        const locationConfig = this.locationConfigs[locationName];
        if (!locationConfig) {
            this.log.error('could not get location configuration', {
                method: 'LifecycleQueuePopulator._handleRestoreOp',
                location: locationName,
                bucket: entry.bucket,
                key: entry.key,
            });
            return;
        }

        // If object already restored, adjust restore max age with the new value
        const isObjectAlreadyRestored = !!value.archive && !!value.archive.restoreCompletedAt;
        if (isObjectAlreadyRestored) {
            this._adjustRestoreMaxAge(value);
            return;
        }

        if (!value.archive || !value.archive.restoreRequestedAt ||
            !value.archive.restoreRequestedDays) {
            return;
        }

        // We would need to provide the object's bucket's account id as part of the kafka entry.
        // This account id would be used by Sorbet to assume the bucket's account role.
        // The assumed credentials will be sent and used by TLP server to put object version
        // to the specific S3 bucket.
        const ownerId = value['owner-id'];
        this.vaultClientWrapper.getAccountId(ownerId, (err, accountId) => {
            // TODO: BB-344 fixes me
            // LifecycleMetrics.onVaultRequest(this.log, 'getAccountIds', err);

            if (err) {
                this.log.error('unable to get account', {
                    method: 'LifecycleQueuePopulator._handleRestoreOp',
                    ownerId,
                    err,
                });
                return;
            }

            this.log.trace(
                'publishing object restore entry',
                { bucket: entry.bucket, key: entry.key, version: value.versionId },
            );

            const topic = `${coldStorageRestoreTopicPrefix}${locationName}`;
            const key = `${entry.bucket}/${entry.key}`;

            let version;
            if (value.versionId) {
                version = encode(value.versionId);
            }

            const requestedDurationSecs = value.archive.restoreRequestedDays * nSecsPerDay();
            const transitionTime = this._parseDate(value.archive.restoreRequestedAt);
            const message = JSON.stringify({
                bucketName: entry.bucket,
                objectKey: value.key,
                objectVersion: version,
                archiveInfo: value.archive.archiveInfo,
                requestId: uuid(),
                eTag: value['content-md5'],
                transitionTime: transitionTime.toISOString(),
                accountId,
                requestedDurationSecs,
            });

            const producer = this._producers[topic];
            if (producer) {
                LifecycleMetrics.onLifecycleTriggered(this.log, 'queuePopulator', 'restore',
                    locationName, Date.now() - transitionTime.getTime());

                const kafkaEntry = { key: encodeURIComponent(key), message };
                producer.send([kafkaEntry], err => {
                    LifecycleMetrics.onKafkaPublish(this.log, 'ColdStorageRestoreTopic', 'queuePopulator', err, 1);
                    if (err) {
                        this.log.error('error publishing object restore request entry', {
                            error: err,
                            method: 'LifecycleQueuePopulator._handleRestoreOp',
                        });
                    }
                });
            } else {
                this.log.error(`producer not available for location ${locationName}`, {
                    method: 'LifecycleQueuePopulator._handleRestoreOp',
                });
            }
        });
    }

    _adjustRestoreMaxAge(md) {
        // We might be receiving this message with some delay, so ignore if already expired.
        const objectExpired = new Date(md.archive.restoreWillExpireAt) < new Date();
        if (objectExpired) {
            return;
        }

        const expiryDate = this._parseDate(md.archive.restoreWillExpireAt);
        const message = JSON.stringify({
            archiveInfo: md.archive.archiveInfo,
            adjust: {
                restoreWillExpireAt: expiryDate.toISOString(),
            },
            updatedAt: md['last-modified'],
            requestId: uuid(),
        });

        const coldLocation = md['x-amz-storage-class'];
        const topic = `${coldStorageRestoreAdjustTopicPrefix}${coldLocation}`;
        const producer = this._producers[topic];
        if (producer) {
            const kafkaEntry = { message };
            producer.send([kafkaEntry], err => {
                LifecycleMetrics.onKafkaPublish(this.log, 'ColdStorageRestoreAdjustTopic', 'queuePopulator', err, 1);
                if (err) {
                    this.log.error('error publishing object restore request entry', {
                        error: err,
                        method: 'LifecycleQueuePopulator._adjustRestoreMaxAge',
                    });
                }
            });
        } else {
            this.log.error(`producer not available for location ${coldLocation}`, {
                method: 'LifecycleQueuePopulator._adjustRestoreMaxAge',
            });
        }
    }

    _handleDeleteOp(entry) {
        if (!this.vaultClientWrapper) {
            return;
        }

        const value = JSON.parse(entry.value);

        // if object is not archived there is nothing to do here
        if (!value.archive) {
            return;
        }

        const locationName = value['x-amz-storage-class'];
        const locationConfig = this.locationConfigs[locationName];
        if (!locationConfig) {
            this.log.error('could not get location configuration', {
                method: 'LifecycleQueuePopulator._handleDeleteOp',
                location: locationName,
                bucket: entry.bucket,
                key: entry.key,
            });
            return;
        }

        const isMaster = isMasterKey(entry.key);
        const isVersionedMaster = isMaster && !!value.versionId && !value.isNull;
        const isNullVersion = !isMaster && value.isNull;
        const isDeleteMarker = value.isDeleteMarker;
        if (isVersionedMaster || isNullVersion || isDeleteMarker) {
            this.log.trace('skip processing of object master entry');
            return;
        }

        const ownerId = value['owner-id'];
        this.vaultClientWrapper.getAccountId(ownerId, (err, accountId) => {
            // In case of failure, we still send the delete request to Sorbet forwarder
            // without the accountId. The accountId is not used for delete operations. It is
            // only included for consistency with other operations.
            if (err) {
                this.log.error('unable to get account', {
                    method: 'LifecycleQueuePopulator._handleDeleteOp',
                    ownerId,
                    err,
                });
            }

            this.log.trace(
                'publishing object delete entry',
                { bucket: entry.bucket, key: entry.key, version: value.versionId },
            );

            const topic = `${coldStorageGCTopicPrefix}${locationName}`;
            const key = `${entry.bucket}/${entry.key}`;

            let version;
            if (value.versionId) {
                version = encode(value.versionId);
            }

            const message = JSON.stringify({
                bucketName: entry.bucket,
                objectKey: value.key,
                objectVersion: version,
                archiveInfo: value.archive.archiveInfo,
                requestId: uuid(),
                transitionTime: new Date(entry.overheadFields.commitTimestamp).toISOString(),
                accountId: accountId || '',
            });

            const producer = this._producers[topic];
            if (producer) {
                LifecycleMetrics.onLifecycleTriggered(this.log, 'queuePopulator', 'archive:gc',
                    locationName, Date.now() - entry.overheadFields.commitTimestamp);

                const kafkaEntry = { key: encodeURIComponent(key), message };
                producer.send([kafkaEntry], err => {
                    LifecycleMetrics.onKafkaPublish(this.log, 'ColdStorageGCTopic', 'queuePopulator', err, 1);
                    if (err) {
                        this.log.error('error publishing object delete request entry', {
                            error: err,
                            method: 'LifecycleQueuePopulator._handleDeleteOp',
                        });
                    }
                });
            } else {
                this.log.error(`producer not available for location ${locationName}`, {
                    method: 'LifecycleQueuePopulator._handleDeleteOp',
                });
            }
        });
    }

    /**
     * Filter record log entries for those that are potentially relevant to
     * lifecycle.
     * @param {Object} entry - The record log entry from metadata.
     * @return {undefined}
     */
    filter(entry) {
        if (entry.type === 'delete') {
            this._handleDeleteOp(entry);
            return undefined;
        }

        if (entry.type !== 'put') {
            return undefined;
        }

        // The entry is decoded once here, then dispatched to the single handler which may be
        // interested in it: most entries are of no interest to any of them.
        const { error, result: value } = safeJsonParse(entry.value);
        if (error) {
            this.log.error('could not parse log entry', {
                method: 'LifecycleQueuePopulator.filter',
                bucket: entry.bucket,
                key: entry.key,
                error,
            });
            return undefined;
        }

        switch (value.originOp) {
        // supporting both 's3:ObjectRestore' and 's3:ObjectRestore:Post' to keep compatibility with
        // older cloudserver versions, the switch to 's3:ObjectRestore:Post' was made to have the
        // correct event type for bucket notifications
        case 's3:ObjectRestore':
        case 's3:ObjectRestore:Post':
        case 's3:ObjectRestore:Retry':
            this._handleRestoreOp(entry, value);
            break;

        // Object creation is the only operation which may declare a cold storage class, and a retry
        // asks for a new attempt: any other originOp comes from backbeat itself, and must not
        // re-trigger a transition.
        case 's3:ObjectCreated:Put':
        case 's3:ObjectCreated:CompleteMultipartUpload':
        case 's3:ObjectCreated:Copy':
        case 's3:LifecycleTransition:Retry':
            this._handleTransitionOp(entry, value);
            break;

        default:
            break;
        }

        if (this.extConfig.conductor.bucketSource !== 'zookeeper') {
            this.log.debug('bucket source is not zookeeper, skipping entry', {
                bucketSource: this.extConfig.conductor.bucketSource,
            });
            return undefined;
        }

        let bucketValue = {};
        if (this._isBucketEntryFromBucketd(entry)) {
            const parsedAttr = safeJsonParse(value.attributes);
            if (parsedAttr.error) {
                this.log.error('could not parse raft log entry attribute', {
                    value: entry.value,
                    error: parsedAttr.error,
                });
                return undefined;
            }
            bucketValue = parsedAttr.result;
        } else if (this._isBucketEntryFromFileMD(entry)) {
            bucketValue = value;
        } else {
            // not a bucket entry
            return undefined;
        }

        return this._updateZkBucketNode(bucketValue);

    }
}
module.exports = LifecycleQueuePopulator;
