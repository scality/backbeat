const { isMasterKey } = require('arsenal/lib/versioning/Version');
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
        if (entry.bucket === usersBucket) {
            return this._filterBucketOp(entry);
        }
        if (!isMasterKey(entry.key)) {
            return this._filterVersionedKey(entry);
        }
        return undefined;
    }

    _filterBucketOp(entry) {
        if (entry.type !== 'put' ||
            entry.key.startsWith(mpuBucketPrefix)) {
            return;
        }
        this.log.trace('publishing bucket replication entry',
                       { bucket: entry.bucket });
        this.publish(this.repConfig.topic,
                     entry.bucket, JSON.stringify(entry));
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
}

module.exports = ReplicationQueuePopulator;
