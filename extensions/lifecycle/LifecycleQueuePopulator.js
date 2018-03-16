const { usersBucket, mpuBucketPrefix } = require('arsenal').constants;
const QueuePopulatorExtension =
    require('../../lib/queuePopulator/QueuePopulatorExtension');
const safeJsonParse = require('./util/safeJsonParse');
const LIFECYCLE_BUCKETS_ZK_PATH = '/data/buckets';

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

    /**
     * Create a new zookeeper node for any bucket that has a lifecycle
     * configuration. Remove the node if the bucket is deleted or the
     * configuration is deleted.
     * @param {Object} attributes - bucket attributes from metadata log
     * @param {String} attributes.owner - canonical ID of the bucket owner
     * @param {String} attributes.name - bucket name
     * @return {undefined}
     */
    _updateZkBucketNode(attributes) {
        const { zookeeperPath } = this.extConfig;
        const path = `${zookeeperPath}${LIFECYCLE_BUCKETS_ZK_PATH}/` +
            `${attributes.owner}:${attributes.name}`;

        // FIXME disabled removal of existing node for now
        //
        // Rationale:
        //
        // From the raft log, we observe that bucket
        // operations come from multiple raft sessions (observed
        // bucket metadata for one bucket from sessions 1, 2 and 3),
        // where the original expectation was that operations on one
        // bucket came from only one raft session at a time.
        //
        // This means that we have no guarantee in which order we
        // process operations on a single bucket, since raft sessions
        // are processed independently by multiple queue
        // populators. Because of time constraints on delivery we
        // disable the removal of bucket nodes for now, but we should
        // consider re-enabling removal with a correct method later.
        if (attributes.deleted ||
            attributes.lifecycleConfiguration === null) {
            this.log.debug(
                'read bucket log entry with no lifecycle configuration, ' +
                    'skipping',
                { owner: attributes.owner,
                  bucket: attributes.name });
            return undefined;
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

    /**
     * Filter record log entries for those that are potentially relevant to
     * lifecycle.
     * @param {Object} entry - The record log entry from metadata.
     * @return {undefined}
     */
    filter(entry) {
        if (entry.type !== 'put' || entry.bucket === usersBucket ||
            (entry.key && entry.key.startsWith(mpuBucketPrefix))) {
            return undefined;
        }
        const { error, result } = safeJsonParse(entry.value);
        if (error) {
            this.log.error('could not parse raft log entry',
                           { value: entry.value, error });
            return undefined;
        }
        const { attributes } = result;
        if (attributes !== undefined) {
            const { error, result } = safeJsonParse(attributes);
            if (error) {
                this.log.error('could not parse attributes in raft log entry',
                               { attributes, error });
                return undefined;
            }
            const { lifecycleConfiguration } = result;
            if (lifecycleConfiguration !== undefined) {
                return this._updateZkBucketNode(result);
            }
        }
        return undefined;
    }
}

module.exports = LifecycleQueuePopulator;
