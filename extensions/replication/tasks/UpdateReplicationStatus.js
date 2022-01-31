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

    /**
     * Push any failed entry to the "failed" topic.
     * @param {QueueEntry} queueEntry - The queue entry with the failed status.
     * @param {string} site - site name.
     * @param {Logger} log - Logger
     * @return {undefined}
     */
    _pushReplayEntry(queueEntry, site, log) {
        queueEntry.decReplayCount();
        const count = queueEntry.getReplayCount();
        const retryEntry = queueEntry.toRetryEntry(site).toKafkaEntry();
        const topicName = this.replayTopics[count];
        if (topicName && this.replayProducers[topicName]) {
            this.replayProducers[topicName].publishReplayEntry(retryEntry);
            log.info('replay entry pushed', {
                entry: queueEntry.getLogInfo(),
                topicName,
                count,
                site,
            });
        } else {
            log.info('replay entry not pushed',
                {
                    entry: queueEntry.getLogInfo(),
                    topicName,
                    count,
                    site,
                });
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
                if (!this.replayTopics) {
                    // if replay topics are not defined in the configuration,
                    // we push the failing entry directly to the "failed" topic.
                    updatedSourceEntry = refreshedEntry.toFailedEntry(site);
                    if (this.repConfig.monitorReplicationFailures) {
                        this._pushFailedEntry(sourceEntry);
                    }
                } else {
                    const count = sourceEntry.getReplayCount();
                    const totalAttemps = this.replayTopics.length;
                    if (count === 0) {
                        if (this.repConfig.monitorReplicationFailures) {
                            this._pushFailedEntry(sourceEntry);
                        }
                        updatedSourceEntry = refreshedEntry.toFailedEntry(site);
                    } else if (count > 0) {
                        if (count > totalAttemps) { // might happen if replay config has changed
                            sourceEntry.setReplayCount(totalAttemps);
                        }
                        this._pushReplayEntry(sourceEntry, site, log);
                        // return here because no need to update source object md,
                        // since site replication status should stay "PENDING".
                        return process.nextTick(done);
                    } else if (!count) {
                        // If no replay count has been defined yet:
                        sourceEntry.setReplayCount(totalAttemps);
                        this._pushReplayEntry(sourceEntry, site, log);
                        // return here because no need to update source object md,
                        // since site replication status should stay "PENDING".
                        return process.nextTick(done);
                    }
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
