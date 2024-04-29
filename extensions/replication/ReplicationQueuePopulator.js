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
        return this._filterKeyOp(entry);
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

    _filterKeyOp(entry) {
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
     * Accept the entry if considered a valid master key entry.
     * There is a case where a single null entry looks like a master key and
     * will not have a duplicate versioned key. They are created when you have a
     * non-versioned bucket with objects, and then convert bucket to versioned.
     * If no new versioned objects are added for given object(s), they look like
     * standalone master keys. The `isNull` case is undefined for these entries.
     * Null versions which are objects created after suspending versioning are allowed,
     * these only have a master object that has an internal versionId and a 'isNull' flag.
     * @param {ObjectQueueEntry} entry - queue entry
     * @return {Boolean} true if we should accept entry
     */
    _entryCanBeReplicated(entry) {
        const isMaster = isMasterKey(entry.getObjectVersionedKey());
        // single null entries will have a version id as undefined or null.
        // do not filter single null entries
        const isNonVersionedMaster = entry.getVersionId() === undefined;
        const isNullVersionedMaster = entry.getIsNull();
        if (!isMaster || isNonVersionedMaster || isNullVersionedMaster) {
            return true;
        }
        this.log.trace('skipping master key entry');
        return false;
    }
}

module.exports = ReplicationQueuePopulator;
