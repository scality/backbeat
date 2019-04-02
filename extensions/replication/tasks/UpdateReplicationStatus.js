const errors = require('arsenal').errors;

const config = require('../../../conf/Config');

const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const BackbeatMetadataProxy = require('../utils/BackbeatMetadataProxy');
const monitoringClient = require('../../../lib/clients/monitoringHandler');

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

    /**
     * Report CRR metrics
     * @param {ObjectQueueEntry} entry - updated object entry
     * @param {String} site - site recently updated
     * @return {undefined}
     */
    _reportMetrics(entry, site) {
        const status = entry.getReplicationSiteStatus(site);
        const data = {};
        const content = entry.getReplicationContent();
        const bytes = content.includes('DATA') ? entry.getContentLength() : 0;
        data[site] = { ops: 1, bytes };

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
            // TODO: promclient metrics can now report failed metrics. add here
            monitoringClient.crrOpDone.inc();
            monitoringClient.crrBytesDone.inc(bytes);
        }
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

                // Report to MetricsProducer with completed/failed metrics
                this._reportMetrics(updatedSourceEntry, site);

                if (config.getIsTransientLocation(
                    updatedSourceEntry.getDataStoreName()) &&
                    updatedSourceEntry.getReplicationStatus()
                    === 'COMPLETED') {
                    // schedule garbage-collection of transient data
                    // locations array
                    this.gcProducer.publishDeleteDataEntry(
                        updatedSourceEntry.getReducedLocations());
                }
                return done();
            });
        });
    }
}

module.exports = UpdateReplicationStatus;
