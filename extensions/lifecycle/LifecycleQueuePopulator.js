const { mpuBucketPrefix } = require('arsenal').constants;
const QueuePopulatorExtension =
    require('../../lib/queuePopulator/QueuePopulatorExtension');
const safeJsonParse = require('./util/safeJsonParse');
const LIFECYCLE_BUCKETS_ZK_PATH = '/data/buckets';
const METASTORE = '__metastore';

class LifecycleQueuePopulator extends QueuePopulatorExtension {

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
                        { owner: attributes.owner,
                          bucket: attributes.name });
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
                    { owner: attributes.owner,
                      bucket: attributes.name });
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

    // TODO: ZENKO-4175
    _handleRestoreOp(entry) {
        if (entry.type !== 'put' ||
            entry.key.startsWith(mpuBucketPrefix)) {
            return;
        }

        // TODO: we might need some extra check.

        const value = JSON.parse(entry.value);

        // TODO: We will need to add `objectMD.originOp = 's3:ObjectRestore';`
        // in CloudServer object restore API
        const operation = value.originOp;
        if (operation !== 's3:ObjectRestore') {
            return;
        }

        const locationName = value.dataStoreName;

        // if object already restore nothing to do, S3 has already updated object MD
        const isObjectAlreadyRestored = value.archive
        && value.archive.restoreCompletedAt
        && new Date(value.archive.restoreWillExpireAt) >= new Date(Date.now());

        if (!value.archive || !value.archive.restoreRequestedAt ||
        !value.archive.restoreRequestedDays || isObjectAlreadyRestored) {
            return;
        }

        // TODO: (To be double checked by the entry consumer)
        // We would need to provide the object's bucket's account id as part of the kafka entry.
        // This account id would be used by Sorbet to assume the bucket's account role.
        // The assumed credentials will be sent and used by TLP server to put object version
        // to the specific S3 bucket.


        // NOTE: the message value of the entry should be review by the topic consumer's developer.
        // {
        //     "bucketName": "<BUCKET_NAME>",
        //     "objectKey": "<KEY_NAME>",
        //     "objectVersion": "<VERSION_ID>",
        //     "accountId": "<ACCOUNT_ID>"
        //     "archiveInfo": {
        //         "archiveId": "<ARCHIVE_ID>",
        //         "archiveVersion": "<ARCHIVE_VERSION>"
        //     }
        //     "requestId": "<req_id>"
        // }

        this.log.trace('publishing bucket replication entry',
                       { bucket: entry.bucket });

        const topic = `cold-restore-req-${locationName}`;
        this.publish(topic,
                    `${entry.bucket}/${entry.key}`, JSON.stringify({}));
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
