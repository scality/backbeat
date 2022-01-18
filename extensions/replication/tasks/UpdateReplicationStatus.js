const errors = require('arsenal').errors;

const ObjectQueueEntry = require('../../replication/utils/ObjectQueueEntry');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const BackbeatMetadataProxy = require('../utils/BackbeatMetadataProxy');
const {
    getSortedSetMember,
    getSortedSetKey,
} = require('../../../lib/util/sortedSetHelper');

class UpdateReplicationStatus extends BackbeatTask {
    /**
     * Update a source object replication status from a kafka entry
     *
     * @constructor
     * @param {ReplicationStatusProcessor} rsp - replication status
     *   processor instance
     * @param {MetricsHandler} metricsHandler - instance of metric handler
     */
    constructor(rsp, metricsHandler) {
        const rspState = rsp.getStateVars();
        super({
            retryTimeoutS:
            rspState.repConfig.replicationStatusProcessor.retryTimeoutS,
        });
        Object.assign(this, rspState);

        this.metricsHandler = metricsHandler;
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
     * Push any failed entry to the "failed" topic.
     * @param {QueueEntry} queueEntry - The queue entry with the failed status.
     * @return {undefined}
     */
    _pushFailedEntry(queueEntry) {
        const site = queueEntry.getSite();
        const bucket = queueEntry.getBucket();
        const objectKey = queueEntry.getObjectKey();
        const versionId = queueEntry.getEncodedVersionId();
        const role = queueEntry.getReplicationRoles().split(',')[0];
        const score = Date.now();
        const latestHour = this.statsClient.getSortedSetCurrentHour(score);
        const message = {
            key: getSortedSetKey(site, latestHour),
            member: getSortedSetMember(bucket, objectKey, versionId, role),
            score,
        };
        this.failedCRRProducer.publishFailedCRREntry(JSON.stringify(message));
    }

    _pushReplayEntry(queueEntry) {
        this.replayProducer.publishReplayEntry(queueEntry.toKafkaEntry());
    }

    _updateReplicationStatus(sourceEntry, log, done) {
        return this._refreshSourceEntry(sourceEntry, log,
        (err, refreshedEntry) => {
            if (err) {
                return done(err);
            }
            console.log('sourceEntry!!!', sourceEntry);
            console.log('backends!!!', sourceEntry.getValue().replicationInfo.backends);
            const site = sourceEntry.getSite();
            const status = sourceEntry.getReplicationSiteStatus(site);
            let updatedSourceEntry;
            if (status === 'COMPLETED') {
                updatedSourceEntry = refreshedEntry.toCompletedEntry(site);
            } else if (status === 'FAILED') {
                const count = sourceEntry.getReplayCount();
                console.log('count!!!', count);
                if (count === 0) {
                    if (this.repConfig.monitorReplicationFailures) {
                        this._pushFailedEntry(sourceEntry);
                    }
                    updatedSourceEntry = refreshedEntry.toFailedEntry(site);
                } else if (count > 0) {
                    sourceEntry.decReplayCount();
                    this.replayProducer.publishReplayEntry(sourceEntry.toRetryEntry(site).toKafkaEntry());
                    // Source object metadata replication status should stay "PENDING".
                    return done();
                } else if (!count) {
                    // If no replay count has been defined yet:
                    sourceEntry.setReplayCount(5);
                    this.replayProducer.publishReplayEntry(sourceEntry.toRetryEntry(site).toKafkaEntry());
                    // Source object metadata replication status should stay "PENDING".
                    return done();
                }
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
                this.metricsHandler.status({
                    replicationStatus: status,
                });
                log.end().info('replication status updated', {
                    entry: updatedSourceEntry.getLogInfo(),
                    replicationStatus:
                        updatedSourceEntry.getReplicationStatus(),
                });
                return done();
            });
        });
    }
}

module.exports = UpdateReplicationStatus;
