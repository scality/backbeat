const async = require('async');
const { errors } = require('arsenal');
const ObjectMD = require('arsenal').models.ObjectMD;

const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { attachReqUids } = require('../../../lib/clients/utils');
const { LifecycleMetrics } = require('../LifecycleMetrics');

class ObjectLockedError extends Error {}
class PendingReplicationError extends Error {}

class LifecycleDeleteObjectTask extends BackbeatTask {
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
        this.objectMD = null;
    }

    _getMetadata(entry, log, done) {
        // only retreiving object metadata once
        if (this.objectMD) {
            return done(null, this.objectMD);
        }
        const { owner: canonicalId, accountId } = entry.getAttribute('target');
        const backbeatClient = this.getBackbeatMetadataProxy(accountId);
        if (!backbeatClient) {
            log.error('failed to get backbeat client', {
                canonicalId,
                accountId,
            });
            return done(errors.InternalError
                .customizeDescription('Unable to obtain client'));
        }

        const { bucket, key, version } = entry.getAttribute('target');
        return backbeatClient.getMetadata({
            bucket,
            objectKey: key,
            versionId: version,
        }, log, (err, blob) => {
            LifecycleMetrics.onS3Request(log, 'getMetadata', 'expiration', err);
            if (err) {
                // <!> Only in S3C <!> Backbeat API returns 'InvalidBucketState' error if the bucket is not versioned.
                // In this case, instead of logging an error, it should be logged as a debug message,
                // to avoid causing unnecessary concern to the customer.
                // TODO: BB-612
                const logLevel = err.code === 'InvalidBucketState' ? 'debug' : 'error';
                log[logLevel]('error getting metadata blob from S3', Object.assign({
                    method: 'LifecycleDeleteObjectTask._getMetadata',
                    error: err.message,
                }, entry.getLogInfo()));
                return done(err);
            }
            const res = ObjectMD.createFromBlob(blob.Body);
            if (res.error) {
                log.error('error parsing metadata blob', Object.assign({
                    error: res.error,
                    method: 'LifecycleDeleteObjectTask._getMetadata',
                }, entry.getLogInfo()));
                return done(
                    errors.InternalError.
                        customizeDescription('error parsing metadata blob'));
            }
            this.objectMD = res.result;
            return done(null, this.objectMD);
        });
    }

    _checkDate(entry, log, done) {
        const { accountId } = entry.getAttribute('target');
        const s3Client = this.getS3Client(accountId);
        if (!s3Client) {
            log.error('failed to get S3 client', { accountId });
            return done(errors.InternalError
                .customizeDescription('Unable to obtain client'));
        }

        const bucket = entry.getAttribute('target.bucket');
        const key = entry.getAttribute('target.key');
        const lastModified = entry.getAttribute('details.lastModified');

        if (lastModified) {
            const reqParams = {
                Bucket: bucket,
                Key: key,
                IfUnmodifiedSince: lastModified,
            };
            const req = s3Client.headObject(reqParams);
            attachReqUids(req, log);
            return req.send((err, res) => {
                LifecycleMetrics.onS3Request(log, 'headObject', 'expiration', err);
                return done(err, res);
            });
        }

        return done();
    }

    _getS3Action(actionType, accountId) {
        let reqMethod;
        if (actionType === 'deleteObject') {
            const backbeatClient = this.getBackbeatClient(accountId);
            if (!backbeatClient) {
                return null;
            }
            // Zenko supports the "deleteObjectFromExpiration" API, which
            // sets the proper originOp in the metadata to trigger a
            // nortification when an object gets expired.
            if (typeof backbeatClient.deleteObjectFromExpiration === 'function') {
                return backbeatClient.deleteObjectFromExpiration.bind(backbeatClient);
            }
            reqMethod = 'deleteObject';
        } else {
            reqMethod = 'abortMultipartUpload';
        }
        const client = this.getS3Client(accountId);
        return client[reqMethod].bind(client);
    }

    _executeDelete(entry, startTime, log, done) {
        const { accountId } = entry.getAttribute('target');

        const reqParams = {
            Bucket: entry.getAttribute('target.bucket'),
            Key: entry.getAttribute('target.key'),
        };
        const version = entry.getAttribute('target.version');
        if (version !== undefined) {
            reqParams.VersionId = version;
        }
        let reqMethod;

        const actionType = entry.getActionType();
        const transitionTime = entry.getAttribute('transitionTime');
        const location = this.objectMD?.dataStoreName || entry.getAttribute('details.dataStoreName');
        let req = null;

        const s3Action = this._getS3Action(actionType, accountId);
        if (!s3Action) {
            log.error('failed to get s3 action', {
                accountId,
                actionType,
                method: 'LifecycleDeleteObjectTask._executeDelete',
            });
            return done(errors.InternalError
                .customizeDescription('Unable to obtain s3 action'));
        }
        LifecycleMetrics.onLifecycleStarted(log,
            actionType === 'deleteMPU' ? 'expiration:mpu' : 'expiration',
            location, startTime - transitionTime);
        if (actionType === 'deleteMPU') {
            reqParams.UploadId = entry.getAttribute('details.UploadId');
        }
        req = s3Action(reqParams);
        attachReqUids(req, log);
        return req.send(err => {
            LifecycleMetrics.onS3Request(log, reqMethod, 'expiration', err);
            LifecycleMetrics.onLifecycleCompleted(log,
                actionType === 'deleteMPU' ? 'expiration:mpu' : 'expiration',
                location, Date.now() - entry.getAttribute('transitionTime'));

            if (err) {
                log.error(
                    `an error occurred on ${reqMethod} to S3`, Object.assign({
                        method: 'LifecycleDeleteObjectTask._executeDelete',
                        error: err.message,
                        httpStatus: err.statusCode,
                    }, entry.getLogInfo()));
                return done(err);
            }
            return done();
        });
    }

    _checkObjectLockState(entry, log, done) {
        const version = entry.getAttribute('target.version');

        if (!version) {
            // if expiration of non-versioned object, ignore object-lock check
            return process.nextTick(done);
        }

        return this._getMetadata(entry, log, (err, objMD) => {
            if (err) {
                return done(err);
            }

            if (objMD.getLegalHold()) {
                return done(new ObjectLockedError('object locked'));
            }

            const retentionMode = objMD.getRetentionMode();
            const retentionDate = objMD.getRetentionDate();


            if (!retentionMode || !retentionDate) {
                return done();
            }

            const objectDate = new Date(retentionDate);
            const now = new Date();

            if (now < objectDate) {
                return done(new ObjectLockedError('object locked'));
            }

            return done();
        });
    }

    /**
     * Throws error if object has a 'PENDING' replication status
     * @param {ActionQueueEntry} entry entry object
     * @param {Logger} log logger instance
     * @param {Function} done callback
     * @returns {undefined}
     */
    _checkReplicationStatus(entry, log, done) {
        const actionType = entry.getActionType();
        // skip check if entry is an incomplete MPU
        // as we only replicate complete objects
        if (actionType === 'deleteMPU') {
            return done();
        }
        return this._getMetadata(entry, log, (err, objMD) => {
            if (err) {
                // <!> Only in S3C <!> Backbeat API returns 'InvalidBucketState' error if the bucket is not versioned,
                // so we can skip checking object replication for non-versioned buckets.
                // TODO: BB-612
                if (err.code === 'InvalidBucketState') {
                    return done();
                }
                return done(err);
            }
            const replicationStatus = objMD.getReplicationStatus();
            if (replicationStatus && replicationStatus !== 'COMPLETED') {
                const error = new PendingReplicationError('object has a pending replication status');
                return done(error);
            }
            return done();
        });
    }

    /**
     * Execute the action specified in action entry to delete an object
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

        return async.series([
            next => this._checkDate(entry, log, next),
            next => this._checkObjectLockState(entry, log, next),
            next => this._checkReplicationStatus(entry, log, next),
            next => this._executeDelete(entry, startTime, log, next),
        ], err => {
            if (err && err instanceof ObjectLockedError) {
                log.debug('Object is locked, skipping',
                    entry.getLogInfo());
                return done();
            }
            if (err && err instanceof PendingReplicationError) {
                log.debug('Object has pending replication status, skipping',
                    entry.getLogInfo());
                return done();
            }
            if (err && err.statusCode === 404) {
                log.debug('Unable to find object to delete, skipping',
                    entry.getLogInfo());
                return done();
            }
            if (err && err.statusCode === 412) {
                log.info('Object was modified after delete entry ' +
                         'created so object was not deleted',
                         entry.getLogInfo());
            }
            return done(err);
        });
    }
}

module.exports = LifecycleDeleteObjectTask;
