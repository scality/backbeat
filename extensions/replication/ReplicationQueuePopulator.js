const { isMasterKey } = require('arsenal').versioning;
const { encode } = require('arsenal').versioning.VersionID;
const { usersBucket, mpuBucketPrefix } = require('arsenal').constants;

const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const ObjectQueueEntry = require('../../lib/models/ObjectQueueEntry');
const ReplicationAPI = require('./ReplicationAPI');
const { LifecycleMetrics, PULL_REPLICATION_TYPE } = require('../lifecycle/LifecycleMetrics');
const config = require('../../lib/Config');
const locationsConfig = require('../../conf/locationConfig.json') || {};
const safeJsonParse = require('../../lib/util/safeJsonParse');
const { getTransitionAttempt } = require('../../lib/util/transitionAttempt');
const { traceHeadersFromEntry } = require('arsenal/build/lib/tracing').kafka;

class ReplicationQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.repConfig = params.config;
        this.metricsHandler = params.metricsHandler;
        this.transitionTasksTopic = config.extensions?.lifecycle?.transitionTasksTopic;

        // Where the data is fetched to when the object metadata does not name a usable
        // target: the first location that can actually hold a local copy.
        this.defaultLocalLocation = Object.keys(locationsConfig).find(
            name => !locationsConfig[name].isCold && !locationsConfig[name].isCRR);
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
        // Data still on the source location has to be fetched first. This is
        // unrelated to replicationInfo, which tracks replication of a *local*
        // object to remote sites, hence the check before any of its conditions.
        if (locationConfig.isCRR && this.transitionTasksTopic) {
            this._publishPullReplicationAction(entry, queueEntry, value);
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
        // Those buckets are versioned, and the metadata layer will repair the
        // master key once the version has been copied over.
        if (isMasterKey(queueEntry.getObjectVersionedKey())) {
            return;
        }
        if (queueEntry.getIsDeleteMarker()) {
            return;
        }
        const locations = queueEntry.getLocation();
        if (!locations || locations.length === 0) {
            // Empty objects hold no data, there is nothing to fetch. Any other
            // object without location information is inconsistent.
            if (queueEntry.getContentLength() > 0) {
                this.log.error('non-empty object without location, skipping pull replication', {
                    method: 'ReplicationQueuePopulator._publishPullReplicationAction',
                    ...queueEntry.getLogInfo(),
                    dataStoreName: queueEntry.getDataStoreName(),
                    contentLength: queueEntry.getContentLength(),
                });
            }
            return;
        }

        const bucket = queueEntry.getBucket();
        const objectKey = queueEntry.getObjectKey();
        const contentLength = queueEntry.getContentLength();
        const targetLocation = this._getPullReplicationTarget(queueEntry, locations);
        if (!targetLocation) {
            return;
        }
        const transitionTime = new Date(entry.overheadFields?.commitTimestamp ?? Date.now());
        const action = ReplicationAPI.createCopyLocationAction({
            bucketName: bucket,
            objectKey,
            owner: queueEntry.getOwnerId(),
            versionId: value.versionId ? encode(value.versionId) : undefined,
            eTag: `"${queueEntry.getContentMd5()}"`,
            lastModified: queueEntry.getLastModified(),
            toLocation: targetLocation,
            originLabel: PULL_REPLICATION_TYPE,
            fromLocation: queueEntry.getDataStoreName(),
            contentLength,
            resultsTopic: this.transitionTasksTopic,
            transitionTime: transitionTime.toISOString(),
            attempt: getTransitionAttempt(queueEntry),
        });
        // 'transition' is what the lifecycle transition processor dispatches
        // on to pick up the copyLocation result.
        action.addContext({
            origin: PULL_REPLICATION_TYPE,
            ruleType: 'transition',
            bucketName: bucket,
            objectKey,
            versionId: value.versionId,
        });
        action.setAttribute('source', {
            bucket,
            objectKey,
            storageClass: queueEntry.getDataStoreName(),
        });

        LifecycleMetrics.onLifecycleTriggered(this.log, 'queuePopulator',
            PULL_REPLICATION_TYPE, targetLocation, Date.now() - transitionTime.getTime());

        this.log.trace('publishing pull replication entry', { entry: queueEntry.getLogInfo() });
        this.publish(ReplicationAPI.getDataMoverTopic(),
                     `${bucket}/${objectKey}`,
                     action.toKafkaMessage(),
                     undefined,
                     traceHeadersFromEntry(value));
    }

    /**
     * Local location the object data must be copied to.
     * @param {ObjectQueueEntry} queueEntry - parsed entry
     * @param {Object[]} locations - object data locations
     * @return {String|undefined} target location, undefined if there is none
     */
    _getPullReplicationTarget(queueEntry, locations) {
        const { targetLocation } = locations[0];
        if (locationsConfig[targetLocation]) {
            return targetLocation;
        }
        // Either the object predates the rewrite pipeline naming a target, or
        // the location was deleted since. Neither is recoverable here, and
        // copying the data elsewhere beats leaving it on the source forever.
        if (!this.defaultLocalLocation) {
            this.log.error('invalid target location and no local location ' +
                'to fall back to, skipping pull replication', {
                method: 'ReplicationQueuePopulator._getPullReplicationTarget',
                ...queueEntry.getLogInfo(),
                targetLocation,
            });
            return undefined;
        }
        this.log.error('invalid target location in object metadata', {
            method: 'ReplicationQueuePopulator._getPullReplicationTarget',
            ...queueEntry.getLogInfo(),
            targetLocation,
            fallbackLocation: this.defaultLocalLocation,
        });
        return this.defaultLocalLocation;
    }

    /**
     * Filter if the entry is considered a valid master key entry.
     * There is a case where a single null entry looks like a master key and
     * will not have a duplicate versioned key. They are created when you have a
     * non-versioned bucket with objects, and then convert bucket to versioned.
     * If no new versioned objects are added for given object(s), they look like
     * standalone master keys. The `isNull` case is undefined for these entries.
     * Non-versioned objects if being replicated from an NFS bucket are also allowed
     * Null versions which are objects created after suspending versioning are allowed,
     * these only have a master object that has an internal versionId and a 'isNull' flag.
     * @param {ObjectQueueEntry} entry - raw queue entry
     * @return {Boolean} true if we should filter entry
     */
    _entryCanBeReplicated(entry) {
        const isMaster = isMasterKey(entry.getObjectVersionedKey());
        const isNFS = entry.getReplicationIsNFS();
        // single null entries will have a version id as undefined or null.
        // do not filter single null entries
        const isNonVersionedMaster = entry.getVersionId() === undefined;
        const isNullVersionedMaster = entry.getIsNull();
        if (isMaster && !isNFS && !isNonVersionedMaster && !isNullVersionedMaster) {
            this.log.trace('skipping master key entry');
            return false;
        }
        return true;
    }
}

module.exports = ReplicationQueuePopulator;
