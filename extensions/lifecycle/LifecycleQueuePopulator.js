const { constants } = require('arsenal');
const { encode } = require('arsenal').versioning.VersionID;
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

class LifecycleQueuePopulator extends QueuePopulatorExtension {

    /**
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.config - extension-specific configuration object
     * @param {Logger} params.logger - logger object
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
        this._producers = {};
    }

    _setupProducer(topic, done) {
        if (this._producers[topic] !== undefined) {
            return process.nextTick(() => done(this._producers[topic]));
        }
        console.log('new producer for topic!!!', topic);
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
                this.log.error('error from backbeat producer',
                    { topic, error: err });
            });
            this._producers[topic] = producer;
            done(producer);
        });
        return undefined;
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

    _handleRestoreOp(entry) {
        if (entry.type !== 'put' ||
            entry.key.startsWith(mpuBucketPrefix)) {
            return;
        }

        const value = JSON.parse(entry.value);

        const operation = value.originOp;
        if (operation !== 's3:ObjectRestore') {
            return;
        }

        const locationName = value.dataStoreName;

        // if object already restore nothing to do, S3 has already updated object MD
        const isObjectAlreadyRestored = value.archive
            && value.archive.restoreCompletedAt
            && new Date(value.archive.restoreWillExpireAt) >= new Date();

        if (!value.archive || !value.archive.restoreRequestedAt ||
            !value.archive.restoreRequestedDays || isObjectAlreadyRestored) {
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

            const topic = coldStorageRestoreTopicPrefix + locationName;
            const key = `${entry.bucket}/${entry.key}`;
            const message = JSON.stringify({
                bucketName: entry.bucket,
                objectKey: value.key,
                objectVersion: encode(value.versionId),
                archiveInfo: value.archive.archiveInfo,
                requestId: uuid(),
                accountId
            });

            this._setupProducer(topic, producer => {
                const kafkaEntry = { key: encodeURIComponent(key), message };
                producer.send([kafkaEntry], err => {
                    if (err) {
                        this.log.error('error publishing object restore request entry', {
                            error: err,
                            method: 'LifecycleQueuePopulator._handleRestoreOp',
                        });
                    }
                });
            });
        });
    }

    /**
     * Filter record log entries for those that are potentially relevant to
     * lifecycle.
     * @param {Object} entry - The record log entry from metadata.
     * @return {undefined}
     */
    filter(entry) {
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
