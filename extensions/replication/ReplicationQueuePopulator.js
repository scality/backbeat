const { isMasterKey } = require('arsenal').versioning;
const { usersBucket, mpuBucketPrefix } = require('arsenal').constants;

const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const ObjectQueueEntry = require('./utils/ObjectQueueEntry');

class ReplicationQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.repConfig = params.config;
        this.metricsHandler = params.metricsHandler;
    }

    filter(entry) {
        if (entry.key === undefined || entry.value === undefined) {
            return undefined;
        }
        if (entry.bucket === usersBucket) {
            return this._filterBucketOp(entry);
        }
        return this._filterVersionedKey(entry);
    }

    _filterBucketOp(entry) {
        if (entry.type !== 'put' ||
            entry.key.startsWith(mpuBucketPrefix)) {
            return;
        }
        // remove logReader to prevent circular stringify
        const publishedEntry = Object.assign({}, entry);
        delete publishedEntry.logReader;

        this.log.trace('publishing bucket replication entry',
                       { bucket: entry.bucket });
        this.publish(this.repConfig.topic,
                     entry.bucket, JSON.stringify(publishedEntry));
    }

    _filterVersionedKey(entry) {
        if (entry.type !== 'put') {
            return;
        }
        const value = JSON.parse(entry.value);
        const queueEntry = new ObjectQueueEntry(entry.bucket,
                                                entry.key, value);
        const sanityCheckRes = queueEntry.checkSanity();
        if (sanityCheckRes) {
            return;
        }
        // Allow a non-versioned object if being replicated from an NFS bucket.
        // Or if the master key is of a non versioned object
        if (!this._entryCanBeReplicated(queueEntry)) {
            return;
        }
        if (queueEntry.getReplicationStatus() !== 'PENDING') {
            return;
        }

        // TODO: getSite is always null
        this._incrementMetrics(queueEntry.getSite(),
            queueEntry.getContentLength());

        // TODO: replication specific metrics go here
        this.metricsHandler.bytes(
            entry.logReader.getMetricLabels(),
            queueEntry.getContentLength()
        );
        this.metricsHandler.objects(
            entry.logReader.getMetricLabels()
        );

        // remove logReader to prevent circular stringify
        const publishedEntry = Object.assign({}, entry);
        delete publishedEntry.logReader;

        this.log.trace('publishing object replication entry',
                       { entry: queueEntry.getLogInfo() });
        this.publish(this.repConfig.topic,
                     `${queueEntry.getBucket()}/${queueEntry.getObjectKey()}`,
                     JSON.stringify(publishedEntry));
    }

    /**
     * Filter if the entry is considered a valid master key entry.
     * There is a case where a single null entry looks like a master key and
     * will not have a duplicate versioned key. They are created when you have a
     * non-versioned bucket with objects, and then convert bucket to versioned.
     * If no new versioned objects are added for given object(s), they look like
     * standalone master keys. The `isNull` case is undefined for these entries.
     * Non-versioned objects if being replicated from an NFS bucket are also allowed
     * @param {ObjectQueueEntry} entry - raw queue entry
     * @return {Boolean} true if we should filter entry
     */
    _entryCanBeReplicated(entry) {
        const isMaster = isMasterKey(entry.getObjectVersionedKey());
        // single null entries will have a version id as undefined.
        // do not filter single null entries
        const isNonVersionedMaster = entry.getVersionId() === undefined;
        if (isMaster && !isNonVersionedMaster) {
            this.log.trace('skipping master key entry');
            return false;
        }
        return true;
    }
}

module.exports = ReplicationQueuePopulator;
