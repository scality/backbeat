const async = require('async');
const uuid = require('uuid/v4');

const { errors, jsutil, models } = require('arsenal');
const { ObjectMD } = models;

const BackbeatMetadataProxy = require('../../../lib/BackbeatMetadataProxy');
const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const ReplicationMetric = require('../ReplicationMetric');
const { attachReqUids } = require('../../../lib/clients/utils');
const { getAccountCredentials } =
          require('../../../lib/credentials/AccountCredentials');
const RoleCredentials =
          require('../../../lib/credentials/RoleCredentials');
const { metricsExtension, metricsTypeQueued, metricsTypeCompleted } =
    require('../constants');

const MPU_CONC_LIMIT = 10;
const MPU_GCP_MAX_PARTS = 1024;

class CopyLocationTask extends BackbeatTask {

    _getReplicationEndpointType() {
        const replicationEndpoint = this.destConfig.bootstrapList
            .find(endpoint => endpoint.site === this.site);
        return replicationEndpoint.type;
    }

    constructor(qp) {
        const qpState = qp.getStateVars();
        super();
        Object.assign(this, qpState);
        this.destType = null;
        if (this.destConfig && this.destConfig.bootstrapList) {
            this.destType = this._getReplicationEndpointType();
            const retryParams =
                  this.repConfig.queueProcessor.retry[this.destType];
            if (retryParams) {
                this.retryParams = retryParams;
            }
        }
        this._replicationMetric = new ReplicationMetric()
            .withProducer(this.mProducer.getProducer())
            .withSite(this.site)
            .withExtension(metricsExtension);
    }

    _validateActionCredentials(actionEntry) {
        const authConfig = this.sourceConfig.auth;
        if (authConfig.type === 'role') {
            return actionEntry.getAttribute(
                'auth.roleArn', { required: true }) !== undefined;
        }
        return true;
    }

    _createCredentials(actionEntry, log) {
        const authConfig = this.sourceConfig.auth;
        const accountCredentials = getAccountCredentials(authConfig, log);
        if (accountCredentials) {
            return accountCredentials;
        }
        const vaultclient = this.vaultclientCache.getClient('source:s3');
        const actionAuth = actionEntry.getAttribute('auth');
        return new RoleCredentials(
            vaultclient, 'replication', actionAuth.roleArn, log);
    }

    _setupClients(actionEntry, log) {
        const s3Credentials = this._createCredentials(actionEntry, log);
        // Disable retries, use our own retry policy (mandatory for
        // putObject route in order to fetch data again from source).
        const { transport, s3, auth } = this.sourceConfig;
        this.backbeatClient = new BackbeatClient({
            endpoint: `${transport}://${s3.host}:${s3.port}`,
            credentials: s3Credentials,
            sslEnabled: transport === 'https',
            httpOptions: { agent: this.sourceHTTPAgent, timeout: 0 },
            maxRetries: 0,
        });
        this.backbeatMetadataProxy = new BackbeatMetadataProxy(
            `${transport}://${s3.host}:${s3.port}`, auth, this.sourceHTTPAgent);
        this.backbeatMetadataProxy
            .setSourceRole(actionEntry.getAttribute('auth.roleArn'))
            .setSourceClient(log);
    }

    processQueueEntry(actionEntry, kafkaEntry, done) {
        const log = this.logger.newRequestLogger();
        actionEntry.addLoggedAttributes({
            bucketName: 'target.bucket',
            objectKey: 'target.key',
            versionId: 'target.version',
            copyToLocation: 'toLocation',
        });
        log.debug('action execution starts', actionEntry.getLogInfo());
        return async.waterfall([
            next => {
                if (!this._validateActionCredentials(actionEntry)) {
                    return next(errors.AccessDenied);
                }
                this._setupClients(actionEntry, log);
                return next();
            },
            next => this._getSourceMD(actionEntry, log, (err, objMD) => {
                if (err && err.code === 'ObjNotFound') {
                    // The object was deleted before entry is processed, we
                    // can safely skip this entry.
                    return next(errors.ObjNotFound);
                }
                if (err) {
                    return next(err);
                }
                return next(null, objMD);
            }),
            (objMD, next) => {
                const err = this._checkObjectState(actionEntry, objMD);
                if (err) {
                    return next(err);
                }
                // Do a multipart upload when either the size is above
                // a threshold or the source object is itself a MPU.
                //
                // FIXME: object ETag for MPUs is an aggregate from
                // each part's ETag, which does not allow the current
                // implementation to check the data integrity when
                // doing ranged PUTs. Also in the current
                // implementation we are forced to send an ETag for a
                // multiple backend putObject(), which only matches if
                // the object is not a MPU, so we cannot use this
                // route for MPUs as-is without recomputing a new
                // checksum, which is not the case today (hence the
                // MPU check below).
                if (objMD.getContentLength() / 1000000 >=
                    this.repConfig.queueProcessor.minMPUSizeMB ||
                    objMD.isMultipartUpload()) {
                    return this._getAndPutMultipartUpload(actionEntry, objMD,
                        log, next);
                }
                return this._getAndPutObject(actionEntry, objMD, log, next);
            },
        ], err => this._publishCopyLocationStatus(
            err, actionEntry, kafkaEntry, log, done));
    }

    _getSourceMD(actionEntry, log, cb) {
        const { bucket, key, version } = actionEntry.getAttribute('target');
        const params = {
            bucket,
            objectKey: key,
            versionId: version,
        };
        return this.backbeatMetadataProxy.getMetadata(
        params, log, (err, blob) => {
            if (err) {
                log.error('error getting metadata blob from S3', Object.assign({
                    method: 'CopyLocationTask._getSourceMD',
                    error: err,
                }, actionEntry.getLogInfo()));
                return cb(err);
            }
            const res = ObjectMD.createFromBlob(blob.Body);
            if (res.error) {
                log.error('error parsing metadata blob', Object.assign({
                    error: res.error,
                    method: 'CopyLocationTask._getSourceMD',
                }, actionEntry.getLogInfo()));
                return cb(errors.InternalError.
                    customizeDescription('error parsing metadata blob'));
            }
            return cb(null, res.result);
        });
    }

    _getAndPutObject(actionEntry, objMD, log, cb) {
        const objectLogger = this.logger.newRequestLogger(log.getUids());
        this._replicationMetric
            .withEntry(actionEntry)
            .withMetricType(metricsTypeQueued)
            .withObjectSize(objMD.getContentLength())
            .publish();
        this.retry({
            actionDesc: 'stream object data',
            logFields: { entry: actionEntry.getLogInfo() },
            actionFunc: done => this._getAndPutObjectOnce(
                actionEntry, objMD, objectLogger, done),
            shouldRetryFunc: err => err.retryable,
            log: objectLogger,
        }, cb);
    }

    _getAndPutObjectOnce(actionEntry, objMD, log, done) {
        log.debug('getting object data', actionEntry.getLogInfo());
        const doneOnce = jsutil.once(done);
        const size = objMD.getContentLength();
        let incomingMsg = null;
        if (size !== 0) {
            const { bucket, key, version } = actionEntry.getAttribute('target');
            const sourceReq = this.backbeatClient.getObject({
                Bucket: bucket,
                Key: key,
                VersionId: version,
                LocationConstraint: objMD.getDataStoreName(),
            });
            attachReqUids(sourceReq, log);
            sourceReq.on('error', err => {
                // eslint-disable-next-line no-param-reassign
                if (err.statusCode === 404) {
                    log.error('the source object was not found', Object.assign({
                        method: 'CopyLocationTask._getAndPutObjectOnce',
                        peer: this.sourceConfig.s3,
                        error: err.message,
                        httpStatus: err.statusCode,
                    }, actionEntry.getLogInfo()));
                    return doneOnce(err);
                }
                log.error('an error occurred on getObject from S3',
                    Object.assign({
                        method: 'CopyLocationTask._getAndPutObjectOnce',
                        peer: this.sourceConfig.s3,
                        error: err.message,
                        httpStatus: err.statusCode,
                    }, actionEntry.getLogInfo()));
                return doneOnce(err);
            });
            incomingMsg = sourceReq.createReadStream();
            incomingMsg.on('error', err => {
                if (err.statusCode === 404) {
                    log.error('the source object was not found', Object.assign({
                        method: 'CopyLocationTask._getAndPutObjectOnce',
                        peer: this.sourceConfig.s3,
                        error: err.message,
                        httpStatus: err.statusCode,
                    }, actionEntry.getLogInfo()));
                    return doneOnce(errors.ObjNotFound);
                }
                log.error('an error occurred when streaming data from S3',
                    Object.assign({
                        method: 'CopyLocationTask._getAndPutObjectOnce',
                        peer: this.sourceConfig.s3,
                        error: err.message,
                    }, actionEntry.getLogInfo()));
                return doneOnce(err);
            });
            log.debug('putting data', actionEntry.getLogInfo());
        }
        return this._sendMultipleBackendPutObject(
            actionEntry, objMD, size, incomingMsg, log, doneOnce);
    }

    /**
     * Send the put object request to Cloudserver.
     * @param {ActionQueueEntry} actionEntry - the action entry
     * @param {ObjectMD} objMD - metadata object
     * @param {Number} size - The size of object to stream
     * @param {Readable} incomingMsg - The stream of data to put
     * @param {Werelogs} log - The logger instance
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _sendMultipleBackendPutObject(actionEntry, objMD, size,
        incomingMsg, log, cb) {
        const { bucket, key, version } = actionEntry.getAttribute('target');
        const destReq = this.backbeatClient.multipleBackendPutObject({
            Bucket: bucket,
            Key: key,
            CanonicalID: objMD.getOwnerId(),
            ContentLength: size,
            ContentMD5: objMD.getContentMd5(),
            StorageType: this.destType,
            StorageClass: this.site,
            VersionId: version,
            UserMetaData: objMD.getUserMetadata(),
            ContentType: objMD.getContentType() || undefined,
            CacheControl: objMD.getCacheControl() || undefined,
            ContentDisposition:
                objMD.getContentDisposition() || undefined,
            ContentEncoding: objMD.getContentEncoding() || undefined,
            Tags: JSON.stringify(objMD.getTags()),
            Body: incomingMsg,
        });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                log.error('an error occurred on putObject to S3',
                Object.assign({
                    method: 'CopyLocationTask._sendMultipleBackendPutObject',
                    error: err.message,
                }, actionEntry.getLogInfo()));
                return cb(err);
            }
            actionEntry.setSuccess({
                location: data.location,
            });
            this._replicationMetric
                .withEntry(actionEntry)
                .withMetricType(metricsTypeCompleted)
                .withObjectSize(size)
                .publish();
            return cb(null, data);
        });
    }

    /**
     * This is a retry wrapper for calling _getRangeAndPutMPUPartOnce.
     * @param {ActionQueueEntry} actionEntry - the action entry
     * @param {ObjectMD} objMD - metadata object
     * @param {String} range - The range to get an object with
     * @param {Number} partNumber - The part number for the current part
     * @param {String} uploadId - The upload ID of the initiated MPU
     * @param {Werelogs} log - The logger instance
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _getRangeAndPutMPUPart(actionEntry, objMD, range, partNumber, uploadId,
        log, cb) {
        this.retry({
            actionDesc: 'stream part data',
            logFields: { entry: actionEntry.getLogInfo() },
            actionFunc: done => this._getRangeAndPutMPUPartOnce(
                actionEntry, objMD, range, partNumber, uploadId, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    /**
     * Get the ranged object, calculate the data's size, then put the part.
     * @param {ActionQueueEntry} actionEntry - the action entry
     * @param {ObjectMD} objMD - metadata object
     * @param {Object} [range] - The range to get an object with
     * @param {Number} range.start - The start byte range
     * @param {Number} range.end - The end byte range
     * @param {Number} partNumber - The part number for the current part
     * @param {String} uploadId - The upload ID of the initiated MPU
     * @param {Werelogs} log - The logger instance
     * @param {Function} done - The callback to call
     * @return {undefined}
     */
    _getRangeAndPutMPUPartOnce(actionEntry, objMD, range, partNumber,
        uploadId, log, done) {
        log.debug('getting object range', Object.assign({
            range,
        }, actionEntry.getLogInfo()));
        // A 0-byte object has no range, otherwise range is inclusive.
        const size = range ? range.end - range.start + 1 : 0;
        let sourceReq = null;
        const { bucket, key, version } = actionEntry.getAttribute('target');
        if (size !== 0) {
            sourceReq = this.backbeatClient.getObject({
                Bucket: bucket,
                Key: key,
                VersionId: version,
                // A 0-byte part has no range.
                Range: range && `bytes=${range.start}-${range.end}`,
                LocationConstraint: objMD.getDataStoreName(),
            });
        }
        return this._putMPUPart(actionEntry, objMD, sourceReq, size,
            uploadId, partNumber, log, done);
    }

    /**
     * Wrapper for aborting an MPU which uses exponential backoff retry.
     * @param {ActionQueueEntry} actionEntry - the action entry
     * @param {ObjectMD} objMD - metadata object
     * @param {String} uploadId - The MPU upload ID to abort
     * @param {Werelogs} log - The logger instance
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _multipleBackendAbortMPU(actionEntry, objMD, uploadId, log, cb) {
        this.retry({
            actionDesc: 'abort multipart upload',
            logFields: { entry: actionEntry.getLogInfo() },
            actionFunc: done => this._multipleBackendAbortMPUOnce(
                actionEntry, objMD, uploadId, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    /**
     * Attempt to abort the given MPU on the source. Used when an
     * operation performed in the process of replicating a multipart
     * upload fails.
     *
     * @param {ActionQueueEntry} actionEntry - the action entry
     * @param {ObjectMD} objMD - metadata object
     * @param {String} uploadId - The MPU upload ID to abort
     * @param {Werelogs} log - The logger instance
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _multipleBackendAbortMPUOnce(actionEntry, objMD, uploadId, log, cb) {
        log.debug('aborting multipart upload', Object.assign({
            uploadId,
        }, actionEntry.getLogInfo()));
        const { bucket, key } = actionEntry.getAttribute('target');
        const destReq = this.backbeatClient.multipleBackendAbortMPU({
            Bucket: bucket,
            Key: key,
            StorageType: this.destType,
            StorageClass: this.site,
            UploadId: uploadId,
        });
        attachReqUids(destReq, log);
        return destReq.send(err => {
            if (err) {
                log.error('an error occurred aborting multipart upload', {
                    method: 'CopyLocationTask._multipleBackendAbortMPUOnce',
                    error: err.message,
                }, actionEntry.getLogInfo());
                return cb(err);
            }
            return cb();
        });
    }

   /**
    * Perform a multipart upload.
     * @param {ActionQueueEntry} actionEntry - the action entry
     * @param {ObjectMD} objMD - metadata object
    * @param {String} uploadId - The upload ID of the initiated MPU
    * @param {Stream} data - The incoming message of the get request
    * @param {Werelogs} log - The logger instance
    * @param {Function} doneOnce - The callback to call
    * @return {undefined}
    */
    _completeMPU(actionEntry, objMD, uploadId, data, log, doneOnce) {
        const { bucket, key, version } = actionEntry.getAttribute('target');
        const destReq = this.backbeatClient.multipleBackendCompleteMPU({
            Bucket: bucket,
            Key: key,
            StorageType: this.destType,
            StorageClass: this.site,
            VersionId: version,
            UserMetaData: objMD.getUserMetadata(),
            ContentType: objMD.getContentType(),
            CacheControl: objMD.getCacheControl() || undefined,
            ContentDisposition: objMD.getContentDisposition() ||
                undefined,
            ContentEncoding: objMD.getContentEncoding() ||
                undefined,
            UploadId: uploadId,
            Tags: JSON.stringify(objMD.getTags()),
            Body: JSON.stringify(data),
        });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                log.error('an error occurred on completing MPU to S3',
                Object.assign({
                    method: 'CopyLocationTask._completeMPU',
                    error: err.message,
                }, actionEntry.getLogInfo()));
                // Attempt to abort the MPU, but pass the error from
                // multipleBackendCompleteMPU because that operation's result
                // should determine the replication's success or failure.
                return this._multipleBackendAbortMPU(
                    actionEntry, objMD, uploadId, log, () => doneOnce(err));
            }
            actionEntry.setSuccess({
                location: data.location,
            });
            return doneOnce();
        });
    }

    /**
     * Get the ranged object, calculate the data's size, then put the part.
     * @param {ActionQueueEntry} actionEntry - the action entry
     * @param {ObjectMD} objMD - metadata object
     * @param {AWS.Request} sourceReq - The source request for getting data
     * @param {Number} size - The size of the content
     * @param {String} uploadId - The upload ID of the initiated MPU
     * @param {Number} partNumber - The part number of the part
     * @param {Werelogs} log - The logger instance
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _putMPUPart(actionEntry, objMD, sourceReq, size, uploadId, partNumber,
                log, cb) {
        const doneOnce = jsutil.once(cb);
        let incomingMsg = null;
        if (sourceReq) {
            attachReqUids(sourceReq, log);
            sourceReq.on('error', err => {
                if (err.statusCode === 404) {
                    return doneOnce(err);
                }
                log.error('an error occurred on getObject from S3',
                Object.assign({
                    method: 'CopyLocationTask._putMPUPart',
                    error: err.message,
                    httpStatus: err.statusCode,
                }, actionEntry.getLogInfo()));
                return doneOnce(err);
            });
            incomingMsg = sourceReq.createReadStream();
            incomingMsg.on('error', err => {
                if (err.statusCode === 404) {
                    return doneOnce(errors.ObjNotFound);
                }
                log.error('an error occurred when streaming data from S3',
                Object.assign({
                    method: 'CopyLocationTask._putMPUPart',
                    error: err.message,
                }, actionEntry.getLogInfo()));
                return doneOnce(err);
            });
            log.debug('putting data', actionEntry.getLogInfo());
        }

        const { bucket, key } = actionEntry.getAttribute('target');
        const destReq = this.backbeatClient.multipleBackendPutMPUPart({
            Bucket: bucket,
            Key: key,
            ContentLength: size,
            StorageType: this.destType,
            StorageClass: this.site,
            PartNumber: partNumber,
            UploadId: uploadId,
            Body: incomingMsg,
        });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                log.error('an error occurred on putting MPU part to S3',
                Object.assign({
                    method: 'CopyLocationTask._putMPUPart',
                    error: err.message,
                }, actionEntry.getLogInfo()));
                return doneOnce(err);
            }
            this._replicationMetric
                .withEntry(actionEntry)
                .withMetricType(metricsTypeCompleted)
                .withObjectSize(size)
                .publish();
            return doneOnce(null, data);
        });
    }

    _getAndPutMultipartUpload(actionEntry, objMD, log, cb) {
        this.retry({
            actionDesc: 'stream MPU data',
            logFields: { entry: actionEntry.getLogInfo() },
            actionFunc: done => this._getAndPutMultipartUploadOnce(
                actionEntry, objMD, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _initiateMPU(actionEntry, objMD, log, cb) {
        // If using Azure backend, create a unique ID to use as the block ID.
        if (this._getReplicationEndpointType() === 'azure') {
            const uploadId = uuid().replace(/-/g, '');
            return setImmediate(() => cb(null, uploadId));
        }
        const { bucket, key, version } = actionEntry.getAttribute('target');
        const destReq = this.backbeatClient.multipleBackendInitiateMPU({
            Bucket: bucket,
            Key: key,
            StorageType: this.destType,
            StorageClass: this.site,
            VersionId: version,
            UserMetaData: objMD.getUserMetadata(),
            ContentType: objMD.getContentType() || undefined,
            CacheControl: objMD.getCacheControl() || undefined,
            ContentDisposition:
                objMD.getContentDisposition() || undefined,
            ContentEncoding: objMD.getContentEncoding() || undefined,
            Tags: JSON.stringify(objMD.getTags()),
        });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                log.error('an error occurred on initating MPU to S3',
                Object.assign({
                    method: 'CopyLocationTask._initiateMPU',
                    error: err.message,
                }, actionEntry.getLogInfo()));
                return cb(err);
            }
            return cb(null, data.uploadId);
        });
    }

    /**
     * Get a byte range size for an object of the given content length, such
     * that the range count does not exceed 1024 elements (i.e.
     * MPU_GCP_MAX_PARTS).
     * @param {Number} contentLen - The content length of the whole object
     * @return {Number} The range size to use
     */
    _getGCPRangeSize(contentLen) {
        let rangeSize = this._getRangeSize(contentLen);
        if (contentLen / rangeSize > MPU_GCP_MAX_PARTS) {
            const pow =
                Math.pow(2, Math.ceil(Math.log(contentLen) / Math.log(2)));
            rangeSize = Math.ceil(pow / MPU_GCP_MAX_PARTS);
        }
        return rangeSize;
    }

    /**
     * Get a byte range size for an object of the given content length, such
     * that the range size sums to the content length when multiplied by a value
     * between 1 and 10000. This has the effect of creating MPU parts of the
     * given range size. This method also optimizes for the subsequent range
     * requests by returning a range size that is an interval of MB or GB.
     * @param {Number} contentLen - The content length of the whole object
     * @return {Number} The range size to use
     */
    _getRangeSize(contentLen) {
        let rangeSize = (1024 * 1024) * 16; // Default 16MB part size.
        if (contentLen <= rangeSize) {
            return contentLen;
        }
        // Target creation of an MPU that is between a 2 and 1000 parts.
        while (contentLen / rangeSize > 1000) {
            // When given a very large object we want to allow use of up to 10K
            // parts, so we limit the part size to 512MB here.
            if (rangeSize >= (1024 * 1024) * 512) {
                break;
            }
            rangeSize *= 2;
        }
        // If the object is large enough to exceed 10K parts of 512MB, then at
        // this point we need to increase the part size such that the largest
        // object size of 5TB can be accounted for.
        while (contentLen / rangeSize > 10000) {
            rangeSize *= 2;
        }
        return rangeSize;
    }

    /**
     * Get byte ranges for an object of the given content length, such that the
     * range count does not exceed 1024 parts if replicating to GCP or does not
     * exceed 10000 parts otherwise.
     * @param {Number} contentLen - The content length of the whole object
     * @param {Boolean} isGCP - Whether the object is being replicated to GCP
     * @return {Array} The array of byte ranges.
     */
    _getRanges(contentLen, isGCP) {
        if (contentLen === 0) {
            // 0-byte object has no range. However we still want to put a single
            // part so the range in the subsequent GET request is undefined.
            return [null];
        }
        const size = isGCP ?
            this._getGCPRangeSize(contentLen) : this._getRangeSize(contentLen);
        const ranges = [];
        let start = 0;
        let end = 0;
        while (end < contentLen - 1) {
            end = start + size - 1;
            if (end < contentLen - 1) {
                ranges.push({ start, end });
                start = end + 1;
            }
        }
        ranges.push({ start, end: contentLen - 1 });
        return ranges;
    }

    /**
     * Perform a multipart upload using ranged get object requests.
     * @param {ActionQueueEntry} actionEntry - the action entry
     * @param {ObjectMD} objMD - metadata object
     * @param {String} uploadId - The upload ID of the initiated MPU
     * @param {Werelogs} log - The logger instance
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _completeRangedMPU(actionEntry, objMD, uploadId, log, cb) {
        const isGCP = this._getReplicationEndpointType() === 'gcp';
        const isAzure = this._getReplicationEndpointType() === 'azure';
        const ranges = this._getRanges(objMD.getContentLength(), isGCP);
        return async.timesLimit(ranges.length, MPU_CONC_LIMIT, (n, next) =>
            this._getRangeAndPutMPUPart(actionEntry, objMD, ranges[n],
                n + 1, uploadId, log, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const res = {
                        PartNumber: [data.partNumber],
                        ETag: [data.ETag],
                        Size: [ranges[n].end - ranges[n].start + 1],
                    };
                    if (isAzure) {
                        res.NumberSubParts = [data.numberSubParts];
                    }
                    return next(null, res);
                }),
            (err, data) => {
                if (err) {
                    log.error('an error occurred on putting MPU part to S3',
                    Object.assign({
                        method: 'CopyLocationTask._completeRangedMPU',
                        error: err.message,
                    }, actionEntry.getLogInfo()));
                    // Attempt to abort the MPU, but pass an error from
                    // multipleBackendPutMPUPart because that operation's result
                    // should determine the replication's success or failure.
                    return this._multipleBackendAbortMPU(
                        actionEntry, objMD, uploadId, log, () => cb(err));
                }
                return this._completeMPU(actionEntry, objMD, uploadId, data,
                    log, cb);
            });
    }

    /**
     * Send the initiate MPU request and then complete the MPU.
     * @param {ActionQueueEntry} actionEntry - the action entry
     * @param {ObjectMD} objMD - metadata object
     * @param {Werelogs} log - The logger instance
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _getAndPutMultipartUploadOnce(actionEntry, objMD, log, cb) {
        return this._initiateMPU(actionEntry, objMD, log, (err, uploadId) => {
            if (err) {
                return cb(err);
            }
            this._replicationMetric
                .withEntry(actionEntry)
                .withMetricType(metricsTypeQueued)
                .withObjectSize(objMD.getContentLength())
                .publish();
            return this._completeRangedMPU(actionEntry, objMD,
                uploadId, log, cb);
        });
    }

    /**
     * Get the source object metadata and ensure the latest object MD5 hash is
     * the same as in the action entry.
     * @param {ActionQueueEntry} actionEntry - the action entry
     * @param {ObjectMD} objMD - metadata object

     * @return {null|Error} - null if the check passes, or an error
     * object of type InvalidObjectState describing the check failure.
     */
    _checkObjectState(actionEntry, objMD) {
        const eTag = actionEntry.getAttribute('target.eTag');
        if (eTag) {
            const strippedETag = eTag.slice(1, -1);
            if (objMD.getContentMd5() !== strippedETag) {
                // The object was overwritten with new contents since
                // the action was initiated
                return errors.InvalidObjectState.customizeDescription(
                    'object contents have changed');
            }
        }
        return null;
    }

    _publishCopyLocationStatus(err, actionEntry, kafkaEntry, log, done) {
        if (err && !actionEntry.getError()) {
            actionEntry.setError(err);
        }
        log.info('action execution ended', actionEntry.getLogInfo());
        if (!actionEntry.getResultsTopic()) {
            // no result requested, we may commit immediately
            return process.nextTick(() => done(null, { committable: true }));
        }
        this.replicationStatusProducer.sendToTopic(
            actionEntry.getResultsTopic(),
            [{ message: actionEntry.toKafkaMessage() }], deliveryErr => {
                if (deliveryErr) {
                    log.error('error in entry delivery to results topic',
                    Object.assign({
                        method: 'CopyLocationTask._publishCopyLocationStatus',
                        topic: actionEntry.getResultsTopic(),
                        error: deliveryErr.message,
                    }, actionEntry.getLogInfo()));
                }
                // Commit whether there was an error or not delivering
                // the message to allow progress of the consumer, as
                // best effort measure when there are errors.
                if (this.dataMoverConsumer) {
                    this.dataMoverConsumer.onEntryCommittable(kafkaEntry);
                }
            });
        return process.nextTick(() => done(null, { committable: false }));
    }
}

module.exports = CopyLocationTask;
