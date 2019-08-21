const errors = require('arsenal').errors;
const assert = require('assert');

const config = require('../../../lib/Config');

const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const BackbeatMetadataProxy = require('../../../lib/BackbeatMetadataProxy');

const {
    metricsExtension,
    metricsTypeCompleted,
    metricsTypeFailed,
} = require('../constants');

class UpdateReplicationStatus extends BackbeatTask {
    /**
     * Update a source object replication status from a kafka entry
     *
     * @constructor
     * @param {ReplicationStatusProcessor} rsp - replication status
     *   processor instance
     */
    constructor(rsp) {
        const rspState = rsp.getStateVars();
        super();
        Object.assign(this, rspState);
        this.retryParams = this.repConfig.replicationStatusProcessor.retry;
        this.sourceRole = null;
        this.s3sourceCredentials = null;
        const { transport, s3, auth } = this.sourceConfig;
        this.backbeatSourceClient = new BackbeatMetadataProxy(
            `${transport}://${s3.host}:${s3.port}`, auth, this.sourceHTTPAgent);
    }

    processQueueEntry(sourceEntry, done) {
        const log = this.logger.newRequestLogger();

        log.debug('updating replication status for entry',
                  { entry: sourceEntry.getLogInfo() });
        const { error } =
            this.backbeatSourceClient.setupSourceRole(sourceEntry, log);
        if (error) {
            return setImmediate(() => done(error));
        }
        this.backbeatSourceClient.setSourceClient(log);
        return this._updateReplicationStatus(sourceEntry, log, done);
    }

    _refreshSourceEntry(sourceEntry, log, cb) {
        const params = {
            bucket: sourceEntry.getBucket(),
            objectKey: sourceEntry.getObjectKey(),
            versionId: sourceEntry.getEncodedVersionId(),
        };
        return this.backbeatSourceClient
        .getMetadata(params, log, (err, blob) => {
            if (err) {
                log.error('error getting metadata blob from S3', {
                    method: 'ReplicateObject._refreshSourceEntry',
                    error: err,
                });
                return cb(err);
            }
            const parsedEntry = ObjectQueueEntry.createFromBlob(blob.Body);
            if (parsedEntry.error) {
                log.error('error parsing metadata blob', {
                    error: parsedEntry.error,
                    method: 'ReplicateObject._refreshSourceEntry',
                });
                return cb(errors.InternalError.
                    customizeDescription('error parsing metadata blob'));
            }
            const refreshedEntry = new ObjectQueueEntry(sourceEntry.getBucket(),
                sourceEntry.getObjectVersionedKey(), parsedEntry.result);
            return cb(null, refreshedEntry);
        });
    }

    /**
     * Report CRR metrics
     * @param {ObjectQueueEntry} sourceEntry - The original entry
     * @param {ObjectQueueEntry} updatedSourceEntry - updated object entry
     * @return {undefined}
     */
    _reportMetrics(sourceEntry, updatedSourceEntry) {
        const content = updatedSourceEntry.getReplicationContent();
        const contentLength = updatedSourceEntry.getContentLength();
        const bytes = content.includes('DATA') ? contentLength : 0;
        const data = {};
        const site = sourceEntry.getSite();
        data[site] = { ops: 1, bytes };
        const status = sourceEntry.getReplicationSiteStatus(site);
        // Report to MetricsProducer with completed/failed metrics.
        if (status === 'COMPLETED' || status === 'FAILED') {
            const entryType = status === 'COMPLETED' ?
                metricsTypeCompleted : metricsTypeFailed;

            this.mProducer.publishMetrics(data, entryType, metricsExtension,
            err => {
                if (err) {
                    this.logger.trace('error occurred in publishing metrics', {
                        error: err,
                        method: 'UpdateReplicationStatus._reportMetrics',
                    });
                }
            });
            // TODO: update ZenkoMetrics
        }
        return undefined;
    }

    /**
     * Get the appropriate source metadata for a non-versioned bucket. If the
     * object metadata has changed since we performed CRR, then we want to
     * keep the PENDING status while updating other relevant metadata values.
     * Otherwise we put a COMPLETED status, as usual.
     * @param {ObjectQueueEntry} sourceEntry - The source entry
     * @param {ObjectQueueEntry} refreshedEntry - The entry from source metadata
     * @return {ObjectQueueEntry} The entry to put on the source
     */
    _getNFSUpdatedSourceEntry(sourceEntry, refreshedEntry) {
        const hasMD5Mismatch =
            sourceEntry.getContentMd5() !== refreshedEntry.getContentMd5();
        if (hasMD5Mismatch) {
            return refreshedEntry.toPendingEntry(sourceEntry.getSite());
        }
        try {
            const soureEntryTags = sourceEntry.getTags();
            const refreshedEntryTags = refreshedEntry.getTags();
            assert.deepStrictEqual(soureEntryTags, refreshedEntryTags);
        } catch (e) {
            return refreshedEntry.toPendingEntry(sourceEntry.getSite());
        }
        return refreshedEntry.toCompletedEntry(sourceEntry.getSite());
    }

    _checkStatus(sourceEntry) {
        const site = sourceEntry.getSite();
        const status = sourceEntry.getReplicationSiteStatus(site);
        const statuses = ['COMPLETED', 'FAILED', 'PENDING'];
        if (!statuses.includes(status)) {
            const msg = `unknown status in replication info: ${status}`;
            return errors.InternalError.customizeDescription(msg);
        }
        return undefined;
    }

    _getUpdatedSourceEntry(params, log) {
        const { sourceEntry, refreshedEntry } = params;
        const site = sourceEntry.getSite();
        const oldStatus = refreshedEntry.getReplicationSiteStatus(site);
        if (oldStatus === 'COMPLETED') {
            // COMPLETED is to be considered a final state, we should
            // not be able to override it.
            //
            // This in particular might happen with transient sources
            // because we cannot read the data anymore once it's
            // replicated everywhere and garbage collected, so any new
            // attempt to replicate after the GC happens is bound to
            // fail, but we don't want to set a FAILED status over a
            // COMPLETED status since that means the replication
            // already happened successfully.

            log.info('entry replication is already COMPLETED for this ' +
                     'location, skipping metadata update', {
                         entry: sourceEntry.getLogInfo(),
                         location: site,
                     });
            return null;
        }
        const newStatus = sourceEntry.getReplicationSiteStatus(site);
        let entry;
        if (newStatus === 'COMPLETED' && sourceEntry.getReplicationIsNFS()) {
            entry = this._getNFSUpdatedSourceEntry(sourceEntry, refreshedEntry);
        } else if (newStatus === 'COMPLETED') {
            entry = refreshedEntry.toCompletedEntry(site);
        } else if (newStatus === 'FAILED') {
            entry = refreshedEntry.toFailedEntry(site);
        } else if (newStatus === 'PENDING') {
            entry = refreshedEntry.toPendingEntry(site);
        }
        const versionId =
            sourceEntry.getReplicationSiteDataStoreVersionId(site);
        entry.setReplicationSiteDataStoreVersionId(site, versionId);
        entry.setSite(site);
        return entry;
    }

    _handleGarbageCollection(entry, log, cb) {
        const dataStoreName = entry.getDataStoreName();
        const isTransient = config.getIsTransientLocation(dataStoreName);
        const status = entry.getReplicationStatus();
        // Should we garbage collect the source data?
        if (isTransient && status === 'COMPLETED') {
            const locations = entry.getReducedLocations();
            // Schedule garbage collection of transient data locations array.
            const gcEntry = ActionQueueEntry.create('deleteData')
                  .addContext({
                      origin: 'transientSource',
                      reqId: log.getSerializedUids(),
                  })
                  .addContext(entry.getLogInfo())
                  .setAttribute('target.locations', locations);
            this.gcProducer.publishActionEntry(gcEntry);
        }
        return cb();
    }

    _putMetadata(updatedSourceEntry, log, cb) {
        const client = this.backbeatSourceClient;
        return client.putMetadata({
            bucket: updatedSourceEntry.getBucket(),
            objectKey: updatedSourceEntry.getObjectKey(),
            versionId: updatedSourceEntry.getEncodedVersionId(),
            mdBlob: updatedSourceEntry.getSerialized(),
        }, log, err => {
            if (err) {
                log.error('an error occurred when updating metadata', {
                    entry: updatedSourceEntry.getLogInfo(),
                    origin: 'source',
                    peer: this.sourceConfig.s3,
                    replicationStatus:
                        updatedSourceEntry.getReplicationStatus(),
                    error: err.message,
                });
                return cb(err);
            }
            log.end().info('metadata updated', {
                entry: updatedSourceEntry.getLogInfo(),
                replicationStatus: updatedSourceEntry.getReplicationStatus(),
            });
            return cb();
        });
    }

    _updateReplicationStatus(sourceEntry, log, done) {
        const error = this._checkStatus(sourceEntry);
        if (error) {
            return done(error);
        }
        return this._refreshSourceEntry(sourceEntry, log,
        (err, refreshedEntry) => {
            if (err) {
                return done(err);
            }
            const params = { sourceEntry, refreshedEntry };
            const updatedSourceEntry = this._getUpdatedSourceEntry(params, log);
            if (!updatedSourceEntry) {
                return done();
            }
            return this._putMetadata(updatedSourceEntry, log, err => {
                if (err) {
                    return done(err);
                }
                this._reportMetrics(sourceEntry, updatedSourceEntry);
                return this._handleGarbageCollection(
                    updatedSourceEntry, log, done);
            });
        });
    }
}

module.exports = UpdateReplicationStatus;
