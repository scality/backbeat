const async = require('async');
const { constants } = require('arsenal');
const { encode } = require('arsenal').versioning.VersionID;
const { isMasterKey } = require('arsenal').versioning;
const { mpuBucketPrefix } = constants;
const QueuePopulatorExtension =
    require('../../lib/queuePopulator/QueuePopulatorExtension');
const { authTypeAssumeRole } = require('../../lib/constants');
const uuid = require('uuid/v4');
const safeJsonParse = require('./util/safeJsonParse');
const { LifecycleMetrics } = require('./LifecycleMetrics');
const LIFECYCLE_BUCKETS_ZK_PATH = '/data/buckets';
const LIFEYCLE_POPULATOR_CLIENT_ID = 'lifecycle-populator';
const METASTORE = '__metastore';
const VaultClientWrapper = require('../utils/VaultClientWrapper');

const config = require('../../lib/Config');
const { coldStorageRestoreTopicPrefix } = config.extensions.lifecycle;
const BackbeatProducer = require('../../lib/BackbeatProducer');
const locations = require('../../conf/locationConfig.json') || {};

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

        this.vaultClientWrapper = new VaultClientWrapper(
            LIFEYCLE_POPULATOR_CLIENT_ID,
            params.vaultAdmin,
            this._authConfig,
            this.log,
        );

        if (this._authConfig.type === authTypeAssumeRole) {
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
            return async.each(coldLocations, (location, done) => {
                const topic = `${coldStorageRestoreTopicPrefix}${location}`;
                return this._setupProducer(topic, done);
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
        if (attributes.deleted ||
            attributes.lifecycleConfiguration === null) {
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

    _handleRestoreOp(entry) {
        this.log.info('_handleRestoreOp called', {
            method: 'LifecycleQueuePopulator._handleRestoreOp',
            entry,
        });
        if (entry.type !== 'put' ||
            entry.key.startsWith(mpuBucketPrefix)) {
            this.log.info('starts with mpu bucket prefix, returning', {
                method: 'LifecycleQueuePopulator._handleRestoreOp',
                entry,
            });
            return;
        }

        const value = JSON.parse(entry.value);

        const operation = value.originOp;
        if (operation !== 's3:ObjectRestore') {
            this.log.info('no s3:ObjectRestore', {
                method: 'LifecycleQueuePopulator._handleRestoreOp',
                entry,
            });
            return;
        }

        const locationName = value.dataStoreName;
        const locationConfig = this.locationConfigs[locationName];
        if (!locationConfig) {
            this.log.info('could not get location configuration', {
                method: 'LifecycleQueuePopulator._handleRestoreOp',
                entry,
            });
            this.log.error('could not get location configuration', {
                method: 'LifecycleQueuePopulator._handleRestoreOp',
                location: locationName,
                bucket: entry.bucket,
                key: entry.key,
            });
            return;
        }

        // if object already restore nothing to do, S3 has already updated object MD
        const isObjectAlreadyRestored = value.archive
            && value.archive.restoreCompletedAt
            && new Date(value.archive.restoreWillExpireAt) >= new Date();

        if (!value.archive || !value.archive.restoreRequestedAt ||
            !value.archive.restoreRequestedDays || isObjectAlreadyRestored) {
            this.log.info('already restored returning..', {
                method: 'LifecycleQueuePopulator._handleRestoreOp',
                entry,
            });
            return;
        }

        // if entry is a versioned object and is the master entry, skip task as
        // the non-master entry will be processed
        if (this._isVersionedObject(value) && isMasterKey(entry.key)) {
            this.log.trace('skip processing of object master entry');
            this.log.info('master entry, returning..', {
                method: 'LifecycleQueuePopulator._handleRestoreOp',
                entry,
            });
            return;
        }

        // We would need to provide the object's bucket's account id as part of the kafka entry.
        // This account id would be used by Sorbet to assume the bucket's account role.
        // The assumed credentials will be sent and used by TLP server to put object version
        // to the specific S3 bucket.
        const ownerId = value['owner-id'];
        this.vaultClientWrapper.getAccountId(ownerId, (err, accountId) => {
            LifecycleMetrics.onVaultRequest(this.log, 'getAccountIds', err);

            if (err) {
                this.log.error('unable to get account', {
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

            const message = JSON.stringify({
                bucketName: entry.bucket,
                objectKey: value.key,
                objectVersion: version,
                archiveInfo: value.archive.archiveInfo,
                requestId: uuid(),
                accountId
            });

            this.log.info('sending message', {
                method: 'LifecycleQueuePopulator._handleRestoreOp',
                entry,
            });
            const producer = this._producers[topic];
            if (producer) {
                const kafkaEntry = { key: encodeURIComponent(key), message };
                producer.send([kafkaEntry], err => {
                    this.log.info('sent message', {
                        method: 'LifecycleQueuePopulator._handleRestoreOp',
                        kafkaEntry,
                    });
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

    /**
     * Filter record log entries for those that are potentially relevant to
     * lifecycle.
     * @param {Object} entry - The record log entry from metadata.
     * @return {undefined}
     */
    filter(entry) {
        this.log.info('filter called', {
            method: 'LifecycleQueuePopulator.filter',
            entry,
        });
        if (entry.type !== 'put') {
            return undefined;
        }

        this._handleRestoreOp(entry);

        if (this.extConfig.conductor.bucketSource !== 'zookeeper') {
            this.log.debug('bucket source is not zookeeper, skipping entry', {
                bucketSource: this.extConfig.conductor.bucketSource,
            });
            return undefined;
        }

        let bucketValue = {};
        if (this._isBucketEntryFromBucketd(entry)) {
            const parsedEntry = safeJsonParse(entry.value);
            if (parsedEntry.error) {
                this.log.error('could not parse raft log entry', {
                    value: entry.value,
                    error: parsedEntry.error,
                });
                return undefined;
            }
            const parsedAttr = safeJsonParse(parsedEntry.result.attributes);
            if (parsedAttr.error) {
                this.log.error('could not parse raft log entry attribute', {
                    value: entry.value,
                    error: parsedAttr.error,
                });
                return undefined;
            }
            bucketValue = parsedAttr.result;
        } else if (this._isBucketEntryFromFileMD(entry)) {
            const { error, result } = safeJsonParse(entry.value);
            if (error) {
                this.log.error('could not parse file md log entry',
                    { value: entry.value, error });
                return undefined;
            }
            bucketValue = result;
        }

        const { lifecycleConfiguration } = bucketValue;
        if (lifecycleConfiguration !== undefined) {
            return this._updateZkBucketNode(bucketValue);
        }

        return undefined;
    }
}
module.exports = LifecycleQueuePopulator;
