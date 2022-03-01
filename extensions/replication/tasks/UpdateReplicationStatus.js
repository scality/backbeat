const errors = require('arsenal').errors;

const ObjectQueueEntry = require('../../replication/utils/ObjectQueueEntry');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const BackbeatMetadataProxy = require('../../../lib/BackbeatMetadataProxy');
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
     * @param {ReplicationStatusMetricsHandler} metricsHandler - instance of metric handler
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
     * Decrement count value by 1 and push failed entry to the "replay" topic.
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
            log.info('push failed entry to the replay topic', {
                entry: queueEntry.getLogInfo(),
                topicName,
                count,
                site,
            });
            this.metricsHandler.replayAttempts();
        } else {
            log.error('error pushing failed entry to the replay topic',
                {
                    entry: queueEntry.getLogInfo(),
                    topicName,
                    count,
                    site,
                });
        }
    }

    /**
     * Manage entry that failed replication.
     * if replay topics are not defined in the configuration, replay logic is disabled.
     * else:
     *  - if count value has not been set,
     *      - set it to max total attempt,
     *      - decrement count value by 1,
     *      - push entry to "replay topic".
     *  - if count value > 0
     *      - decrement count value by 1,
     *      - push entry to "replay topic".
     *  - if count value is 0
     *      - push entry "failed topic"
     *      - update source object md with site replication status set to FAILED
     * @param {QueueEntry} refreshedEntry - Updated queue entry with latest "source object" metadata.
     * @param {QueueEntry} queueEntry - The queue entry with the failed status.
     * @param {string} site - site name.
     * @param {Logger} log - Logger
     * @return {null | ObjectQueueEntry} updated "source object" metadata - either null
     * if no "source object" metadata update needed, or ObjectQueueEntry with the "source object" metadata
     */
    _handleFailedReplicationEntry(refreshedEntry, queueEntry, site, log) {
        if (!this.replayTopics) {
            // if replay topics are not defined in the configuration,
            // replay logic is disabled.
            if (this.repConfig.monitorReplicationFailures) {
                this._pushFailedEntry(queueEntry);
            }
            return refreshedEntry.toFailedEntry(site);
        }
        const count = queueEntry.getReplayCount();
        const totalAttempts = this.replayTopics.length;
        if (count === 0) {
            if (this.repConfig.monitorReplicationFailures) {
                this._pushFailedEntry(queueEntry);
            }
            return refreshedEntry.toFailedEntry(site);
        }
        if (count > 0) {
            if (count > totalAttempts) { // might happen if replay config has changed
                queueEntry.setReplayCount(totalAttempts);
            }
            this._pushReplayEntry(queueEntry, site, log);
            return null;
        }
        if (!count) {
            // If no replay count has been defined yet:
            queueEntry.setReplayCount(totalAttempts);
            this._pushReplayEntry(queueEntry, site, log);
            return null;
        }
        log.error('count value is invalid',
            {
                entry: queueEntry.getLogInfo(),
                count,
                site,
            });
        return null;
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
                if (sourceEntry.getReplayCount() >= 0) {
                    this.metricsHandler.replaySuccess();
                }
            } else if (status === 'FAILED') {
                updatedSourceEntry = this._handleFailedReplicationEntry(refreshedEntry, sourceEntry, site, log);
                if (!updatedSourceEntry) {
                    // return here because no need to update source object md,
                    // since site replication status should stay "PENDING".
                    return process.nextTick(done);
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
