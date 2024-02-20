const async = require('async');

const { errors } = require('arsenal');
const ObjectMD = require('arsenal').models.ObjectMD;
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { LifecycleMetrics } = require('../LifecycleMetrics');
const locations = require('../../../conf/locationConfig.json') || {};

class LifecycleUpdateExpirationTask extends BackbeatTask {
    /**
     * Process a lifecycle object entry
     *
     * @constructor
     * @param {LifecycleObjectProcessor} proc - object processor instance
     */
    constructor(proc) {
        const procState = proc.getStateVars();
        super();
        Object.assign(this, procState);
    }

    _getMetadata(entry, log, done) {
        const {
            accountId,
            bucket,
            key,
            version,
        } = entry.getAttribute('target');

        const backbeatClient = this.getBackbeatMetadataProxy(accountId);
        if (!backbeatClient) {
            log.error('failed to get backbeat client', { accountId });
            done(errors.InternalError.customizeDescription(
                'Unable to obtain client',
            ));
            return;
        }

        backbeatClient.getMetadata({
            bucket,
            objectKey: key,
            versionId: version,
        }, log, (err, blob) => {
            LifecycleMetrics.onS3Request(log, 'getMetadata', 'restore:delete', err);
            if (err) {
                log.error('error getting metadata blob from S3', Object.assign({
                    method: 'LifecycleUpdateExpirationTask._getMetadata',
                    error: err.message,
                }, entry.getLogInfo()));
                done(err);
                return;
            }
            const res = ObjectMD.createFromBlob(blob.Body);
            if (res.error) {
                log.error('error parsing metadata blob', Object.assign({
                    error: res.error,
                    method: 'LifecycleUpdateExpirationTask._getMetadata',
                }, entry.getLogInfo()));
                done(errors.InternalError.customizeDescription(
                    'error parsing metadata blob'
                ));
            } else {
                done(null, res.result);
            }
        });
        return;
    }

    _putMetadata(entry, objMD, log, done) {
        const {
            accountId,
            bucket,
            key,
            version,
        } = entry.getAttribute('target');

        const backbeatClient = this.getBackbeatMetadataProxy(accountId);
        if (!backbeatClient) {
            log.error('failed to get backbeat client', { accountId });
            done(errors.InternalError.customizeDescription(
                'Unable to obtain client',
            ));
            return;
        }

        backbeatClient.putMetadata({
            bucket,
            objectKey: key,
            versionId: version,
            mdBlob: objMD.getSerialized(),
        }, log, err => {
            LifecycleMetrics.onS3Request(log, 'putMetadata', 'restore:delete', err);
            if (err) {
                log.error(
                    'an error occurred when updating metadata for transition',
                    Object.assign(
                        {
                            method: 'LifecycleUpdateExpirationTask._putMetadata',
                            error: err.message,
                        },
                        entry.getLogInfo(),
                    )
                );
                done(err);
                return;
            } else {
                log.end().info('metadata updated for transition', entry.getLogInfo());
                done();
                return;
            }
        });
    }

    _garbageCollectLocation(entry, locations, log, done) {
        const { bucket, key, version, eTag, accountId } = entry.getAttribute('target');
        const gcEntry = ActionQueueEntry.create('deleteData')
              .addContext({
                  origin: 'lifecycle',
                  ruleType: 'restore',
                  reqId: log.getSerializedUids(),
                  bucketName: bucket,
                  objectKey: key,
                  versionId: version,
                  eTag,
              })
              .setAttribute('source', entry.getAttribute('source'))
              .setAttribute('serviceName', 'lifecycle-transition')
              .setAttribute('target.locations', locations)
              .setAttribute('target.accountId', accountId);
        this.gcProducer.publishActionEntry(gcEntry);
        return process.nextTick(done);
    }

    /**
     * Execute the action specified in action entry to update expirations on an object
     *
     * @param {ActionQueueEntry} entry - action entry to execute
     * @param {Function} done - callback funtion
     * @return {undefined}
     */
    processActionEntry(entry, done) {
        const startTime = Date.now();
        const log = this.logger.newRequestLogger();
        entry.addLoggedAttributes({
            bucketName: 'target.bucket',
            objectKey: 'target.key',
            versionId: 'target.version',
        });

        async.waterfall([
            next => this._getMetadata(entry, log, next),
            (objMD, next) => {
                const coldLocation = entry.getAttribute('target.location') ||
                    // If location not specified, use the first (and only) location
                    // This is a temporary fix, until Sorbet is fixed to provide the information
                    Object.keys(locations).find(name => locations[name].isCold);

                const archive = objMD.getArchive();

                // Confirm the object has indeed expired: it can happen that the
                // expiration date is updated while the expiry was "in-flight" (e.g.
                // queued for expiry but not yet expired)
                const restoreExpirationDate = new Date(archive.restoreWillExpireAt);
                if (restoreExpirationDate > new Date()) {
                    return process.nextTick(done);
                }

                LifecycleMetrics.onLifecycleStarted(log, 'restore:delete',
                    coldLocation, startTime - restoreExpirationDate);

                // Reset archive flags to no longer show it as restored
                objMD.setArchive({
                    archiveInfo: archive.archiveInfo,
                });
                objMD.setAmzRestore();
                objMD.setDataStoreName(coldLocation);
                objMD.setAmzStorageClass(coldLocation);
                objMD.setTransitionInProgress(false);
                objMD.setOriginOp('s3:ObjectRestore:Delete');
                return this._putMetadata(entry, objMD, log, err => {
                    LifecycleMetrics.onLifecycleCompleted(log, 'restore:delete',
                        coldLocation, Date.now() - restoreExpirationDate);
                    next(err, objMD);
                });
            },
            (objMD, next) => this._garbageCollectLocation(
                entry, objMD.getLocation(), log, next,
            ),
        ], done);
    }
}

module.exports = LifecycleUpdateExpirationTask;
