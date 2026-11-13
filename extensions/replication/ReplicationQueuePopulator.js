const { usersBucket, mpuBucketPrefix } = require('@scality/arsenal').constants;

const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const ObjectQueueEntry = require('../../lib/models/ObjectQueueEntry');
const ReplicationAPI = require('./ReplicationAPI');
const ReplicationMetrics = require('./ReplicationMetrics');
const { LifecycleMetrics, PULL_REPLICATION } = require('../lifecycle/LifecycleMetrics');
const config = require('../../lib/Config');
const locationsConfig = require('../../conf/locationConfig.json') || {};
const safeJsonParse = require('../../lib/util/safeJsonParse');
const { getTransitionAttempt } = require('../../lib/util/transitionAttempt');
const { isShadowedMaster } = require('../../lib/util/versioning');
const { traceHeadersFromEntry } = require('@scality/arsenal/build/lib/tracing').kafka;
const { replicationDirections } = require('./constants');

class ReplicationQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.repConfig = params.config;
        this.metricsHandler = params.metricsHandler;
        this.transitionTasksTopic = config.extensions?.lifecycle?.transitionTasksTopic;
    }

    filter(entry) {
        if (entry.key === undefined || entry.value === undefined) {
            // bucket updates have no key in raft log
            return undefined;
        }
        // users..bucket has a special role for "echo mode"
        if (entry.bucket === usersBucket) {
            return this._filterBucketOp(entry);
        }
        // internal buckets (other than users..bucket) are ignored
        if (entry.bucket.includes('..')) {
            return undefined;
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
        const { error, result: value } = safeJsonParse(entry.value);
        if (error) {
            // The raft entry value is malformed (corruption upstream of the
            // populator). Log + skip + advance the batch so the populator
            // does not crash and restart-loop on the same record.
            this.log.error('failed to parse raft entry value, skipping', {
                method: 'ReplicationQueuePopulator._filterKeyOp',
                bucket: entry.bucket,
                key: entry.key,
                type: entry.type,
                raftId: entry.logReader
                    && typeof entry.logReader.getMetricLabels === 'function'
                    && entry.logReader.getMetricLabels().logId,
                error: error.message,
            });
            return;
        }
        const queueEntry = new ObjectQueueEntry(entry.bucket,
                                                entry.key, value);
        const sanityCheckRes = queueEntry.checkSanity();
        if (sanityCheckRes) {
            return;
        }
        const locationConfig = locationsConfig[queueEntry.getDataStoreName()] || {};
        // Allow a non-versioned object if being replicated from an NFS bucket.
        // Or if the master key is of a non versioned object
        if (!this._entryCanBeReplicated(queueEntry)) {
            return;
        }
        // Data still on the source location has to be fetched first. This is
        // unrelated to replicationInfo, which tracks replication of a *local*
        // object to remote sites, hence the check before any of its conditions.
        if (locationConfig.isCRR && this.transitionTasksTopic) {
            this._publishPullReplicationAction(entry, queueEntry, value);
            return;
        }
        if (queueEntry.getReplicationStatus() !== 'PENDING') {
            return;
        }
        // We do not replicate cold objects.
        if (locationConfig.isCold) {
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

        const metricLabels = {
            ...entry.logReader.getMetricLabels(),
            direction: replicationDirections.push,
        };
        this.metricsHandler.bytes(metricLabels, bytes);
        this.metricsHandler.objects(metricLabels);

        const publishedEntry = Object.assign({}, entry);
        delete publishedEntry.logReader;

        const traceHeaders = traceHeadersFromEntry(value);

        this.log.trace('publishing object replication entry',
                       { entry: queueEntry.getLogInfo() });
        this.publish(this.repConfig.topic,
                     `${queueEntry.getBucket()}/${queueEntry.getObjectKey()}`,
                     JSON.stringify(publishedEntry),
                     undefined,
                     traceHeaders);
    }

    /**
     * Queue a copyLocation action for an object whose data still lives on the
     * source location: the data mover copies it over, and the transition
     * processor merges the new location into the object metadata.
     *
     * Duplicates are expected (and harmless): the same object may show up
     * several times in the oplog, and the copy is idempotent.
     *
     * @param {Object} entry - raw metadata log entry
     * @param {ObjectQueueEntry} queueEntry - parsed entry
     * @param {Object} value - parsed entry metadata
     * @return {undefined}
     */
    _publishPullReplicationAction(entry, queueEntry, value) {
        if (queueEntry.getIsDeleteMarker()) {
            return;
        }

        // Fail if the object is not empty, but has no location to pull data from
        const contentLength = queueEntry.getContentLength();
        const locations = queueEntry.getLocation();
        if (!locations?.length && contentLength > 0) {
            this.log.error('non-empty object without location, skipping pull replication', {
                method: 'ReplicationQueuePopulator._publishPullReplicationAction',
                ...queueEntry.getLogInfo(),
                dataStoreName: queueEntry.getDataStoreName(),
                contentLength,
            });
            return;
        }

        const bucket = queueEntry.getBucket();
        const objectKey = queueEntry.getObjectKey();
        const sourceLocation = queueEntry.getDataStoreName();
        // for metrics only: the copy task resolves where the data actually goes
        const { targetLocation } = locations?.[0] ?? {};
        const transitionTime = new Date(entry.overheadFields?.commitTimestamp ?? Date.now());
        const action = ReplicationAPI.createCopyLocationAction({
            bucketName: bucket,
            objectKey,
            owner: queueEntry.getOwnerId(),
            versionId: queueEntry.getEncodedVersionId() || 'null',
            eTag: `"${queueEntry.getContentMd5()}"`,
            lastModified: queueEntry.getLastModified(),
            // Data processors pick the copies whose toLocation is their own
            // site, and only the processor of the location pulled from can
            // read the data: CopyLocationTask resolves the actual target, and
            // sets it when publishing the result.
            toLocation: sourceLocation,
            originLabel: PULL_REPLICATION,
            fromLocation: sourceLocation,
            contentLength,
            resultsTopic: this.transitionTasksTopic,
            transitionTime: transitionTime.toISOString(),
            attempt: getTransitionAttempt(queueEntry),
        });
        // 'transition' is what the lifecycle transition processor dispatches
        // on to pick up the copyLocation result.
        action.addContext({
            origin: PULL_REPLICATION,
            ruleType: 'transition',
            bucketName: bucket,
            objectKey,
            versionId: value.versionId,
        });
        action.setAttribute('source', {
            bucket,
            objectKey,
            storageClass: sourceLocation,
        });

        LifecycleMetrics.onLifecycleTriggered(this.log, 'queuePopulator',
            PULL_REPLICATION, sourceLocation, Date.now() - transitionTime.getTime());

        this.log.trace('publishing pull replication entry', { entry: queueEntry.getLogInfo() });
        this.publish(ReplicationAPI.getDataMoverTopic(),
                     `${bucket}/${objectKey}`,
                     action.toKafkaMessage(),
                     undefined,
                     traceHeadersFromEntry(value));

        // Counterpart of the bytes the data mover reports once the copy
        // completed, to measure the pull replication backlog.
        ReplicationMetrics.onReplicationQueued(PULL_REPLICATION,
            sourceLocation, targetLocation, contentLength);

        const metricLabels = {
            ...entry.logReader.getMetricLabels(),
            direction: replicationDirections.pull,
        };
        this.metricsHandler.bytes(metricLabels, contentLength);
        this.metricsHandler.objects(metricLabels);
    }

    /**
     * Whether the entry stands for an object to replicate: the master key of
     * a versioned object duplicates its version key entry, except when
     * replicating from an NFS bucket, where the master is the entry to use.
     *
     * @param {ObjectQueueEntry} entry - raw queue entry
     * @return {Boolean} true if the entry is to be processed
     */
    _entryCanBeReplicated(entry) {
        if (entry.getReplicationIsNFS()) {
            this.log.trace('replicating NFS entry regardless of its key', entry.getLogInfo());
            return true;
        }
        if (isShadowedMaster(entry.getObjectVersionedKey(), entry.getValue())) {
            this.log.trace('skipping master key entry', entry.getLogInfo());
            return false;
        }
        return true;
    }
}

module.exports = ReplicationQueuePopulator;
