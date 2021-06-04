const { isMasterKey } = require('arsenal/lib/versioning/Version');
const { usersBucket, mpuBucketPrefix } = require('arsenal').constants;

const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const ObjectQueueEntry = require('../../lib/models/ObjectQueueEntry');

class ReplicationQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.repConfig = params.config;
        this.metricsHandler = params.metricsHandler;
    }

    filter(entry) {
        if (entry.key === undefined) {
            // bucket updates have no key in raft log
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
        // ALlow a non-versioned object if being replicated from an NFS bucket.
        if (isMasterKey(entry.key) && !queueEntry.getReplicationIsNFS()) {
            return;
        }
        if (queueEntry.getReplicationStatus() !== 'PENDING') {
            return;
        }

        // remove logReader to prevent circular stringify
        const repSites = queueEntry.getReplicationInfo().backends;
        const content = queueEntry.getReplicationContent();
        const bytes = content.includes('DATA') ?
            queueEntry.getContentLength() : 0;

        // record replication metrics by site
        repSites.filter(entry => entry.status === 'PENDING')
            .forEach(backend => {
                this._incrementMetrics(backend.site, bytes);
            });

        // TODO: replication specific metrics go here
        this.metricsHandler.bytes(
            entry.logReader.getMetricLabels(),
            bytes
        );
        this.metricsHandler.objects(
            entry.logReader.getMetricLabels()
        );

        const publishedEntry = Object.assign({}, entry);
        delete publishedEntry.logReader;

        this.log.trace('publishing object replication entry',
                       { entry: queueEntry.getLogInfo() });
        this.publish(this.repConfig.topic,
                     `${queueEntry.getBucket()}/${queueEntry.getObjectKey()}`,
                     JSON.stringify(publishedEntry));
    }
}

module.exports = ReplicationQueuePopulator;
