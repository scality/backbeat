const errors = require('arsenal').errors;

const config = require('../../../conf/Config');

const ObjectQueueEntry = require('../../replication/utils/ObjectQueueEntry');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const BackbeatMetadataProxy = require('../utils/BackbeatMetadataProxy');

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
        super({
            retryTimeoutS:
            rspState.repConfig.replicationStatusProcessor.retryTimeoutS,
        });
        Object.assign(this, rspState);

        this.sourceRole = null;
        this.s3sourceCredentials = null;
        this.backbeatSourceClient =
            new BackbeatMetadataProxy(this.sourceConfig, this.sourceHTTPAgent);
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
            logInfo: sourceEntry.getLogInfo(),
            bucket: sourceEntry.getBucket(),
            objectKey: sourceEntry.getObjectKey(),
            encodedVersionId: sourceEntry.getEncodedVersionId(),
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

    _updateReplicationStatus(sourceEntry, log, done) {
        return this._refreshSourceEntry(sourceEntry, log,
        (err, refreshedEntry) => {
            if (err) {
                return done(err);
            }
            const site = sourceEntry.getSite();
            const status = sourceEntry.getReplicationSiteStatus(site);
            let updatedSourceEntry;
            if (status === 'COMPLETED') {
                updatedSourceEntry = refreshedEntry.toCompletedEntry(site);
            } else if (status === 'FAILED') {
                updatedSourceEntry = refreshedEntry.toFailedEntry(site);
            } else if (status === 'PENDING') {
                updatedSourceEntry = refreshedEntry.toPendingEntry(site);
            } else {
                const msg = `unknown status in replication info: ${status}`;
                return done(errors.InternalError.customizeDescription(msg));
            }
            updatedSourceEntry.setSite(site);
            updatedSourceEntry.setReplicationSiteDataStoreVersionId(site,
                sourceEntry.getReplicationSiteDataStoreVersionId(site));
            return this.backbeatSourceClient
            .putMetadata(updatedSourceEntry, log, err => {
                if (err) {
                    log.error('an error occurred when writing replication ' +
                              'status',
                        { entry: updatedSourceEntry.getLogInfo(),
                          origin: 'source',
                          peer: this.sourceConfig.s3,
                          replicationStatus:
                            updatedSourceEntry.getReplicationStatus(),
                          error: err.message });
                    return done(err);
                }
                log.end().info('replication status updated', {
                    entry: updatedSourceEntry.getLogInfo(),
                    replicationStatus:
                        updatedSourceEntry.getReplicationStatus(),
                });
                if (config.getIsTransientLocation(
                    updatedSourceEntry.getDataStoreName()) &&
                    updatedSourceEntry.getReplicationStatus()
                    === 'COMPLETED') {
                    // schedule garbage-collection of transient data
                    // locations array
                    return this.gcProducer.publishDeleteDataEntry(
                        updatedSourceEntry.getReducedLocations(), done);
                }
                return done();
            });
        });
    }
}

module.exports = UpdateReplicationStatus;
