const { isMasterKey } = require('arsenal').versioning;
const { encode } = require('arsenal').versioning.VersionID;
const { usersBucket, mpuBucketPrefix } = require('arsenal').constants;

const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const ObjectQueueEntry = require('../../lib/models/ObjectQueueEntry');
const ReplicationAPI = require('./ReplicationAPI');
const locationsConfig = require('../../conf/locationConfig.json') || {};
const safeJsonParse = require('../../lib/util/safeJsonParse');
const { traceHeadersFromEntry } = require('arsenal/build/lib/tracing').kafka;

const TRANSITION_ATTEMPT_MD = 'x-amz-meta-scal-s3-transition-attempt';

class ReplicationQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.repConfig = params.config;
        this.metricsHandler = params.metricsHandler;
        // Clean room: when set, objects whose data still lives on the source
        // (isCRR) location are queued for localization instead of replication.
        this.localizationConfig = params.config.localization;
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
        const dataStoreName = queueEntry.getDataStoreName();
        const locationConfig = (dataStoreName && locationsConfig[dataStoreName])
            || {};
        // Clean room: the object data still lives on the source (isCRR)
        // location and first needs to be localized. This is unrelated to
        // replicationInfo, which tracks replication of a *local* object to
        // remote sites, hence the check before any replication condition.
        if (locationConfig.isCRR) {
            if (this.localizationConfig) {
                this._publishLocalizationAction(entry, queueEntry, value);
            }
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
     * source (isCRR) location, so the data mover copies it to the local
     * location and the transition processor merges the new location back into
     * the object metadata.
     *
     * Duplicates are expected (and harmless): the same object may show up
     * several times in the oplog, and the copy is idempotent.
     *
     * @param {Object} entry - raw metadata log entry
     * @param {ObjectQueueEntry} queueEntry - parsed entry
     * @param {Object} value - parsed entry metadata
     * @return {undefined}
     */
    _publishLocalizationAction(entry, queueEntry, value) {
        // Clean room buckets are versioned: the master key is repaired by the
        // metadata layer once the version has been localized.
        if (isMasterKey(queueEntry.getObjectVersionedKey())) {
            return;
        }
        if (queueEntry.getIsDeleteMarker()) {
            return;
        }
        const locations = queueEntry.getLocation();
        if (!locations || locations.length === 0) {
            // Empty objects hold no data, there is nothing to localize. Any
            // other object without location information is inconsistent.
            if (queueEntry.getContentLength() > 0) {
                this.log.error(
                    'non-empty object without location, skipping localization',
                    {
                        method: 'ReplicationQueuePopulator.' +
                            '_publishLocalizationAction',
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
        const action = ReplicationAPI.createCopyLocationAction({
            bucketName: bucket,
            objectKey,
            owner: queueEntry.getOwnerId(),
            versionId: value.versionId ? encode(value.versionId) : undefined,
            eTag: `"${queueEntry.getContentMd5()}"`,
            lastModified: queueEntry.getLastModified(),
            toLocation: this.localizationConfig.toLocation,
            originLabel: 'localization',
            fromLocation: queueEntry.getDataStoreName(),
            contentLength,
            resultsTopic: this.localizationConfig.resultsTopic,
            transitionTime: new Date(
                entry.overheadFields?.commitTimestamp ?? Date.now()
            ).toISOString(),
            attempt: this._getTransitionAttempt(queueEntry),
        });
        // 'transition' is what the lifecycle transition processor dispatches
        // on to pick up the copyLocation result.
        action.addContext({
            origin: 'localization',
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

        this.metricsHandler.localizationBytes(
            entry.logReader.getMetricLabels(),
            contentLength
        );
        this.metricsHandler.localizationObjects(
            entry.logReader.getMetricLabels()
        );

        this.log.trace('publishing object localization entry',
                       { entry: queueEntry.getLogInfo() });
        this.publish(ReplicationAPI.getDataMoverTopic(),
                     `${bucket}/${objectKey}`,
                     action.toKafkaMessage(),
                     undefined,
                     traceHeadersFromEntry(value));
    }

    /**
     * Number of times the data mover already tried to copy this object. The
     * transition processor bumps the counter on failure, which produces a new
     * oplog entry and re-triggers the copy.
     * @param {ObjectQueueEntry} queueEntry - parsed entry
     * @return {Number|undefined} attempt count, if any
     */
    _getTransitionAttempt(queueEntry) {
        const umd = queueEntry.getUserMetadata();
        if (!umd) {
            return undefined;
        }
        const { error, result } = safeJsonParse(umd);
        if (error) {
            return undefined;
        }
        const attempt = Number.parseInt(result[TRANSITION_ATTEMPT_MD], 10);
        return Number.isInteger(attempt) ? attempt : undefined;
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
