const async = require('async');
const AWS = require('aws-sdk');

const errors = require('arsenal').errors;
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { getAccountCredentials } =
      require('../../../lib/credentials/AccountCredentials');
const getVaultCredentials =
    require('../../../lib/credentials/getVaultCredentials');
const { attachReqUids } = require('../../../lib/clients/utils');


class LifecycleObjectTask extends BackbeatTask {
    /**
     * Process a lifecycle object entry
     *
     * @constructor
     * @param {QueueProcessor} qp - queue processor instance
     */
    constructor(qp) {
        const qpState = qp.getStateVars();
        super();
        Object.assign(this, qpState);

        this.s3Client = null;
        this.accountCredsCache = {};
    }

    _getCredentials(canonicalId, log, cb) {
        const cachedCreds = this.accountCredsCache[canonicalId];
        if (cachedCreds) {
            return process.nextTick(() => cb(null, cachedCreds));
        }
        const credentials = getAccountCredentials(this.lcConfig.auth, log);
        if (credentials) {
            this.accountCredsCache[canonicalId] = credentials;
            return process.nextTick(() => cb(null, credentials));
        }
        const { type } = this.lcConfig.auth;
        if (type === 'vault') {
            return getVaultCredentials(
                this.authConfig, canonicalId, 'lifecycle',
                (err, accountCreds) => cb(err, accountCreds));
        }
        return process.nextTick(
            () => cb(errors.InternalError.customizeDescription(
                `invalid auth type ${type}`)));
    }

    _setupClients(canonicalId, log, done) {
        this._getCredentials(canonicalId, log, (err, accountCreds) => {
            if (err) {
                log.error('error generating new access key', {
                    error: err.message,
                    method: 'LifecycleObjectTask._getCredentials',
                });
                return done(err);
            }
            const s3 = this.s3Config;
            const transport = this.transport;
            log.debug('creating s3 client', { transport, s3 });
            this.accountCredsCache[canonicalId] = accountCreds;
            this.s3Client = new AWS.S3({
                endpoint: `${transport}://${s3.host}:${s3.port}`,
                credentials: accountCreds,
                sslEnabled: transport === 'https',
                s3ForcePathStyle: true,
                signatureVersion: 'v4',
                httpOptions: { agent: this.httpAgent, timeout: 0 },
                maxRetries: 0,
            });
            return done();
        });
    }

    _checkDate(entry, log, done) {
        const { bucket, key } = entry.target;
        const { lastModified } = entry.details || {};

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
        const action = entry.action;
        const { bucket, key, version } = entry.target;
        const details = entry.details;
        const reqParams = { Bucket: bucket, Key: key };
        if (version !== undefined) {
            reqParams.VersionId = version;
        }
        let reqMethod;

        if (action === 'deleteObject') {
            reqMethod = 'deleteObject';
        } else if (action === 'deleteMPU') {
            reqParams.UploadId = details.UploadId;
            reqMethod = 'abortMultipartUpload';
        }
        const req = this.s3Client[reqMethod](reqParams);
        attachReqUids(req, log);
        return req.send(err => {
            if (err) {
                log.error(`an error occurred on ${reqMethod} to S3`,
                    { method: 'LifecycleObjectTask._executeDelete',
                        error: err.message,
                        httpStatus: err.statusCode });
                return done(err);
            }
            return done();
        });
    }

    /**
     * Execute the action specified in kafka queue entry
     *
     * @param {Object} entry - kafka queue entry object
     * @param {String} entry.action - entry action name
     * @param {Object} entry.target - entry action target object
     * @param {Function} done - callback funtion
     * @return {undefined}
     */

    processQueueEntry(entry, done) {
        const log = this.logger.newRequestLogger();

        // entries are small, we can log them directly
        log.debug('processing lifecycle object entry', { entry });

        if (!entry.target) {
            log.error('missing "target" in object queue entry',
                      { entry });
            return process.nextTick(done);
        }
        const action = entry.action;
        if (action === 'deleteObject' ||
            action === 'deleteMPU') {
            return async.series([
                next => this._setupClients(entry.target.owner, log, next),
                next => this._checkDate(entry, log, next),
                next => this._executeDelete(entry, log, next),
            ], err => {
                if (err && err.statusCode === 412) {
                    log.info('Object was modified after delete entry ' +
                             'created so object was not deleted',
                             { entry });
                }
                return done(err);
            });
        }
        log.info(`skipped unsupported action ${action}`, { entry });
        return process.nextTick(done);
    }
}

module.exports = LifecycleObjectTask;
