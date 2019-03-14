const async = require('async');

const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { attachReqUids } = require('../../../lib/clients/utils');


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

    _checkDate(entry, log, done) {
        const bucket = entry.getAttribute('target.bucket');
        const key = entry.getAttribute('target.key');
        const lastModified = entry.getAttribute('details.lastModified');

        if (lastModified) {
            const reqParams = {
                Bucket: bucket,
                Key: key,
                IfUnmodifiedSince: lastModified,
            };
            const req = this.s3Client.headObject(reqParams);
            attachReqUids(req, log);
            return req.send(done);
        }
        return done();
    }

    _executeDelete(entry, log, done) {
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
        const req = this.s3Client[reqMethod](reqParams);
        attachReqUids(req, log);
        return req.send(err => {
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
            next => this._executeDelete(entry, log, next),
        ], err => {
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
