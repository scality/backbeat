const async = require('async');
const { errors } = require('arsenal');
const ObjectMD = require('arsenal').models.ObjectMD;

const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { attachReqUids } = require('../../../lib/clients/utils');
const { LifecycleMetrics } = require('../LifecycleMetrics');

class ObjectLockedError extends Error {}

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
    }

    _getMetadata(entry, log, done) {
        const { owner: canonicalId, accountId } = entry.getAttribute('target');
        const backbeatClient = this.getBackbeatMetadataProxy(canonicalId, accountId);
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
            if (err) {
                log.error('error getting metadata blob from S3', Object.assign({
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
            return done(null, res.result);
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

    _executeDelete(entry, log, done) {
        const { accountId } = entry.getAttribute('target');
        const s3Client = this.getS3Client(accountId);
        if (!s3Client) {
            log.error('failed to get S3 client', { accountId });
            return done(errors.InternalError
                .customizeDescription('Unable to obtain client'));
        }

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
        if (actionType === 'deleteObject') {
            reqMethod = 'deleteObject';
        } else if (actionType === 'deleteMPU') {
            reqParams.UploadId = entry.getAttribute('details.UploadId');
            reqMethod = 'abortMultipartUpload';
        }
        const req = s3Client[reqMethod](reqParams);
        attachReqUids(req, log);
        return req.send(err => {
            LifecycleMetrics.onS3Request(log, reqMethod, 'expiration', err);

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
     * Execute the action specified in action entry to delete an object
     *
     * @param {ActionQueueEntry} entry - action entry to execute
     * @param {Function} done - callback funtion
     * @return {undefined}
     */

    processActionEntry(entry, done) {
        const log = this.logger.newRequestLogger();
        entry.addLoggedAttributes({
            bucketName: 'target.bucket',
            objectKey: 'target.key',
            versionId: 'target.version',
        });

        return async.series([
            next => this._checkDate(entry, log, next),
            next => this._checkObjectLockState(entry, log, next),
            next => this._executeDelete(entry, log, next),
        ], err => {
            if (err && err instanceof ObjectLockedError) {
                log.debug('Object is locked, skipping',
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
