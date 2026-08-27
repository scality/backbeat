const async = require('async');
const { v4: uuid } = require('uuid');

const { errors, jsutil, models } = require('arsenal');
const { ObjectMD } = models;
const { S3Client: AwsS3Client, GetObjectCommand: AwsGetObjectCommand } =
    require('@aws-sdk/client-s3');

const BackbeatMetadataProxy = require('../../../lib/BackbeatMetadataProxy');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const {
    BackbeatRoutesClient,
    GetObjectCommand: BackbeatRoutesGetObjectCommand,
    MultipleBackendPutObjectCommand,
    MultipleBackendInitiateMPUCommand,
    MultipleBackendPutMPUPartCommand,
    MultipleBackendCompleteMPUCommand,
    MultipleBackendAbortMPUCommand,
    addContentLengthMiddleware,
} = require('@scality/cloudserverclient');
const { LifecycleMetrics } = require('../../lifecycle/LifecycleMetrics');
const ReplicationMetric = require('../ReplicationMetric');
const ReplicationMetrics = require('../ReplicationMetrics');
const { isRetryableMiddleware, TIMEOUT_MS } = require('../../../lib/clients/utils');
const { getAccountCredentials } =
          require('../../../lib/credentials/AccountCredentials');
const RoleCredentials =
          require('../../../lib/credentials/RoleCredentials');
const config = require('../../../lib/Config');
const { authTypeAssumeRole } = require('../../../lib/constants');
const { metricsExtension, metricsTypeQueued, metricsTypeCompleted } =
    require('../constants');

const MPU_GCP_MAX_PARTS = 1024;

class CopyLocationTask extends BackbeatTask {

    _getReplicationEndpointType() {
        return this.destConfig?.replicationEndpoint?.type;
    }

    constructor(qp) {
        const qpState = qp.getStateVars();
        super();
        Object.assign(this, qpState);
        this.destType = this._getReplicationEndpointType();
        if (this.destConfig && this.destType) {
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
        const requestHandler = {
            [transport === 'https' ? 'httpsAgent' : 'httpAgent']: this.sourceHTTPAgent,
            requestTimeout: TIMEOUT_MS,
        };
        this.backbeatClient = new BackbeatRoutesClient({
            endpoint: `${transport}://${s3.host}:${s3.port}`,
            credentials: s3Credentials.getCredentialsProvider(),
            region: 'us-east-1',
            maxAttempts: 1,
            requestHandler,
        });
        this.backbeatClient.middlewareStack.add(isRetryableMiddleware(), {
            step: 'deserialize',
            priority: 'high',
        });
        this.backbeatMetadataProxy = new BackbeatMetadataProxy(
            `${transport}://${s3.host}:${s3.port}`, auth, this.sourceHTTPAgent);
        this.backbeatMetadataProxy
            .setSourceRole(actionEntry.getAttribute('auth.roleArn'))
            .setSourceClient(log);
    }

    /**
     * Get a cached S3 client, authenticated with the assumed-role
     * credentials for the role carried on a location part.
     * @param {Object} locationConfig - the location's config
     * @param {String} roleArn - the role ARN carried by the location part
     * @param {Werelogs} log - the logger instance
     * @return {AwsS3Client} the client
     * @throws {ArsenalError} AccessDenied (retryable) if credentials
     * could not be obtained for the role
     */
    _getAssumedRoleS3Client(locationConfig, roleArn, log) {
        const { details } = locationConfig;
        const s3Endpoint = `${details.transport}://${details.servers[0]}`;
        const cacheKey = `${s3Endpoint}::${roleArn}`;
        if (this.assumedRoleS3Clients[cacheKey]) {
            return this.assumedRoleS3Clients[cacheKey];
        }
        const accountId = roleArn.split(':')[4];
        const roleName = roleArn.split(':role/')[1];
        const credentials = this.assumedRoleCredentialsManager.getCredentials({
            id: roleArn,
            accountId,
            authConfig: {
                type: authTypeAssumeRole,
                roleName,
            },
            stsConfig: {
                endpoint: `${details.transport}://${details.sts.host}:${details.sts.port}`,
                credentials: {
                    accessKeyId: details.sts.accessKey,
                    secretAccessKey: details.sts.secretKey,
                },
            },
        });
        if (!credentials) {
            log.error('unable to obtain assumed-role credentials for source location', {
                method: 'CopyLocationTask._getAssumedRoleS3Client',
                roleArn,
                endpoint: s3Endpoint,
            });
            const err = errors.AccessDenied.customizeDescription(
                `unable to assume role ${roleArn} for isCRR source location`);
            err.retryable = true;
            throw err;
        }
        const isHttps = details.transport === 'https';
        const client = new AwsS3Client({
            endpoint: s3Endpoint,
            credentials: credentials.getCredentialsProvider(),
            region: 'us-east-1',
            forcePathStyle: true,
            requestHandler: {
                [isHttps ? 'httpsAgent' : 'httpAgent']:
                    isHttps ? this.assumedRoleHTTPSAgent : this.assumedRoleHTTPAgent,
                requestTimeout: TIMEOUT_MS,
            },
            maxAttempts: 1,
        });
        client.middlewareStack.add(isRetryableMiddleware(), {
            step: 'deserialize',
            priority: 'high',
        });
        this.assumedRoleS3Clients[cacheKey] = client;
        return this.assumedRoleS3Clients[cacheKey];
    }

    /**
     * Send a GetObject request for the object's data,
     * reading either through Cloudserver's multiple-backend routes,
     * or directly from a CRR source location's own S3
     * endpoint via an assumed role.
     * @param {ActionQueueEntry} actionEntry - the action entry
     * @param {ObjectMD} objMD - metadata object
     * @param {Object} [range] - byte range to request, or undefined for the whole object
     * @param {Werelogs} log - the logger instance
     * @param {AbortController} abortController - abort controller for the GET request
     * @return {Promise} resolves to the GetObject response
     */
    async _sendGetObject(actionEntry, objMD, range, log, abortController) {
        const locationConfig = config.getLocationConstraint(objMD.getDataStoreName());
        if (locationConfig?.isCRR === true) {
            const locations = objMD.getLocation();
            const part = locations && locations[0];
            if (!part || !part.role) {
                const err = errors.AccessDenied.customizeDescription(
                    'missing role on location part for isCRR source location');
                err.retryable = true;
                throw err;
            }
            const s3Client = this._getAssumedRoleS3Client(locationConfig, part.role, log);
            const command = new AwsGetObjectCommand({
                Bucket: part.bucket,
                Key: objMD.getKey(),
                VersionId: part.dataStoreVersionId,
                Range: range && `bytes=${range.start}-${range.end}`,
            });
            return await s3Client.send(command, { abortSignal: abortController.signal });
        }

        const { bucket, key, version } = actionEntry.getAttribute('target');
        const command = new BackbeatRoutesGetObjectCommand({
            Bucket: bucket,
            Key: key,
            VersionId: version,
            Range: range && `bytes=${range.start}-${range.end}`,
            LocationConstraint: objMD.getDataStoreName(),
            RequestUids: log.getSerializedUids(),
        });
        return await this.backbeatClient.send(command, { abortSignal: abortController.signal });
    }

    processQueueEntry(actionEntry, kafkaEntry, done) {
        const startTime = Date.now();
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
                if (err && (err.name === 'ObjNotFound')) {
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

                const transitionTime = actionEntry.getAttribute('metrics.transitionTime') ||
                    objMD.getTransitionTime();
                LifecycleMetrics.onLifecycleStarted(log, 'transition',
                    actionEntry.getAttribute('toLocation'),
                    startTime - Date.parse(transitionTime));

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
        ], err => {
            const retArgs = this._publishCopyLocationStatus(
                err, actionEntry, kafkaEntry, log);

            const { origin, fromLocation, contentLength } =
                  actionEntry.getAttribute('metrics');
            ReplicationMetrics.onReplicationProcessed(
                origin, fromLocation, this.site, contentLength,
                actionEntry.getStatus(),
                actionEntry.getElapsedMs());

            return done(null, retArgs);
        });
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
        if (size === 0) {
            log.debug('putting data', actionEntry.getLogInfo());
            return this._sendMultipleBackendPutObject(
                actionEntry, objMD, size, undefined, log, doneOnce);
        }

        let sourceStreamAborted = false;
        let abortedByPut = false;
        const abortController = new AbortController();

        return this._sendGetObject(actionEntry, objMD, undefined, log, abortController)
            .then(response => {
                const incomingMsg = response.Body;
                incomingMsg.on('error', err => {
                    if (!sourceStreamAborted) {
                        sourceStreamAborted = true;
                        abortController.abort();
                        if (err.$metadata?.httpStatusCode === 404) {
                            log.error('the source object was not found', Object.assign({
                                method: 'CopyLocationTask._getAndPutObjectOnce',
                                peer: this.sourceConfig.s3,
                                error: err.message,
                                httpStatus: err.$metadata?.httpStatusCode,
                            }, actionEntry.getLogInfo()));
                            return doneOnce(errors.ObjNotFound);
                        }
                        if (!abortedByPut) {
                            log.error('an error occurred when streaming data from S3',
                                Object.assign({
                                    method: 'CopyLocationTask._getAndPutObjectOnce',
                                    peer: this.sourceConfig.s3,
                                    error: err.message,
                                }, actionEntry.getLogInfo()));
                        }
                        return doneOnce(err);
                    }
                    return undefined;
                });

                const putDone = err => {
                    if (err && !sourceStreamAborted) {
                        // Abort the source stream on PUT error
                        abortedByPut = true;
                        sourceStreamAborted = true;
                        abortController.abort();
                        if (incomingMsg.destroy) {
                            incomingMsg.destroy();
                        }
                    }
                    return doneOnce(err);
                };

                log.debug('putting data', actionEntry.getLogInfo());
                return this._sendMultipleBackendPutObject(
                    actionEntry, objMD, size, incomingMsg, log, putDone);
            })
            .catch(err => {
                if (err.name === 'NoSuchVersion') {
                    log.info('source version no longer exists', Object.assign({
                        method: 'CopyLocationTask._getAndPutObjectOnce',
                        error: err.message,
                    }, actionEntry.getLogInfo()));
                    return doneOnce(errors.InvalidObjectState.customizeDescription(
                        'source version no longer exists'));
                }
                if (err.$metadata?.httpStatusCode === 404) {
                    log.error('the source object was not found', Object.assign({
                        method: 'CopyLocationTask._getAndPutObjectOnce',
                        peer: this.sourceConfig.s3,
                        error: err.message,
                        httpStatus: err.$metadata?.httpStatusCode,
                    }, actionEntry.getLogInfo()));
                    return doneOnce(err);
                }
                log.error('an error occurred on getObject from S3',
                          Object.assign({
                              method: 'CopyLocationTask._getAndPutObjectOnce',
                              peer: this.sourceConfig.s3,
                              error: err.message,
                              errorName: err.name,
                              httpStatus: err.$metadata?.httpStatusCode,
                          }, actionEntry.getLogInfo()));
                return doneOnce(err);
            });
    }

    /**
     * Send the put object request to Cloudserver.
     * @param {ActionQueueEntry} actionEntry - the action entry
     * @param {ObjectMD} objMD - metadata object
     * @param {Number} size - The size of object to stream
     * @param {StreamingBlobPayloadOutputTypes} incomingMsg - The stream of data to put
     * @param {Werelogs} log - The logger instance
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _sendMultipleBackendPutObject(actionEntry, objMD, size,
        incomingMsg, log, cb) {
        const { bucket, key, version } = actionEntry.getAttribute('target');
        const command = new MultipleBackendPutObjectCommand({
            Bucket: bucket,
            Key: key,
            CanonicalID: objMD.getOwnerId(),
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
            RequestUids: log.getSerializedUids(),
        });
        addContentLengthMiddleware(command, size);

        return this.backbeatClient.send(command)
            .then(data => {
                actionEntry.setSuccess({
                    location: data.location,
                });
                this._replicationMetric
                    .withEntry(actionEntry)
                    .withMetricType(metricsTypeCompleted)
                    .withObjectSize(size)
                    .publish();
                return cb(null, data);
            })
            .catch(err => {
                log.error('an error occurred on putObject to S3',
                Object.assign({
                    method: 'CopyLocationTask._sendMultipleBackendPutObject',
                    error: err.message,
                    httpStatus: err.$metadata?.httpStatusCode,
                }, actionEntry.getLogInfo()));
                return cb(err);
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
        if (size === 0) {
            return this._putMPUPart(actionEntry, objMD, undefined, 0,
                uploadId, partNumber, log, undefined, done);
        }

        const abortController = new AbortController();

        return this._sendGetObject(actionEntry, objMD, range, log, abortController)
            .then(response => this._putMPUPart(actionEntry, objMD, response.Body, size,
                    uploadId, partNumber, log, abortController, done))
            .catch(err => {
                if (err.name === 'NoSuchVersion') {
                    log.info('source version no longer exists', Object.assign({
                        method: 'CopyLocationTask._getRangeAndPutMPUPartOnce',
                        error: err.message,
                    }, actionEntry.getLogInfo()));
                    return done(errors.InvalidObjectState.customizeDescription(
                        'source version no longer exists'));
                }
                if (err.$metadata?.httpStatusCode === 404) {
                    return done(err);
                }
                log.error('an error occurred on getObject from S3',
                Object.assign({
                    method: 'CopyLocationTask._getRangeAndPutMPUPartOnce',
                    error: err.message,
                    errorName: err.name,
                    httpStatus: err.$metadata?.httpStatusCode,
                }, actionEntry.getLogInfo()));
                return done(err);
            });
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
        const command = new MultipleBackendAbortMPUCommand({
            Bucket: bucket,
            Key: key,
            StorageType: this.destType,
            StorageClass: this.site,
            UploadId: uploadId,
            RequestUids: log.getSerializedUids(),
        });
        
        return this.backbeatClient.send(command)
            .then(() => cb())
            .catch(err => {
                log.error('an error occurred aborting multipart upload', {
                    method: 'CopyLocationTask._multipleBackendAbortMPUOnce',
                    error: err.message,
                    httpStatus: err.$metadata?.httpStatusCode,
                }, actionEntry.getLogInfo());
                return cb(err);
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
        const command = new MultipleBackendCompleteMPUCommand({
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
            RequestUids: log.getSerializedUids(),
        });
        
        return this.backbeatClient.send(command)
            .then(data => {
                actionEntry.setSuccess({
                    location: data.location,
                });
                return doneOnce();
            })
            .catch(err => {
                log.error('an error occurred on completing MPU to S3',
                Object.assign({
                    method: 'CopyLocationTask._completeMPU',
                    error: err.message,
                    httpStatus: err.$metadata?.httpStatusCode,
                }, actionEntry.getLogInfo()));
                // Attempt to abort the MPU, but pass the error from
                // multipleBackendCompleteMPU because that operation's result
                // should determine the replication's success or failure.
                return this._multipleBackendAbortMPU(
                    actionEntry, objMD, uploadId, log, () => doneOnce(err));
            });
    }

    /**
     * Put the MPU part with the given data.
     * @param {ActionQueueEntry} actionEntry - the action entry
     * @param {ObjectMD} objMD - metadata object
     * @param {StreamingBlobPayloadOutputTypes} incomingMsg - The data to upload as the part
     * @param {Number} size - The size of the content
     * @param {String} uploadId - The upload ID of the initiated MPU
     * @param {Number} partNumber - The part number of the part
     * @param {Werelogs} log - The logger instance
     * @param {AbortController} abortController - The abort controller for the source GET request
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _putMPUPart(actionEntry, objMD, incomingMsg, size, uploadId, partNumber,
                log, abortController, cb) {
        const doneOnce = jsutil.once(cb);
        let sourceStreamAborted = false;
        let abortedByPut = false;

        if (incomingMsg) {
            incomingMsg.on('error', err => {
                if (!sourceStreamAborted) {
                    sourceStreamAborted = true;
                    abortController.abort();
                    if (err.$metadata?.httpStatusCode === 404) {
                        return doneOnce(errors.ObjNotFound);
                    }
                    if (!abortedByPut) {
                        log.error('an error occurred when streaming MPU part data from S3',
                        Object.assign({
                            method: 'CopyLocationTask._putMPUPart',
                            error: err.message,
                        }, actionEntry.getLogInfo()));
                    }
                    return doneOnce(err);
                }
                return undefined;
            });
            log.debug('putting data', actionEntry.getLogInfo());
        }

        const { bucket, key } = actionEntry.getAttribute('target');
        const command = new MultipleBackendPutMPUPartCommand({
            Bucket: bucket,
            Key: key,
            StorageType: this.destType,
            StorageClass: this.site,
            PartNumber: partNumber,
            UploadId: uploadId,
            Body: incomingMsg,
            RequestUids: log.getSerializedUids(),
        });
        addContentLengthMiddleware(command, size);

        return this.backbeatClient.send(command)
            .then(data => {
                this._replicationMetric
                    .withEntry(actionEntry)
                    .withMetricType(metricsTypeCompleted)
                    .withObjectSize(size)
                    .publish();
                return doneOnce(null, data);
            })
            .catch(err => {
                if (incomingMsg && !sourceStreamAborted) {
                    abortedByPut = true;
                    sourceStreamAborted = true;
                    abortController.abort();
                    incomingMsg.destroy();
                }
                log.error('an error occurred on putting MPU part to S3',
                Object.assign({
                    method: 'CopyLocationTask._putMPUPart',
                    error: err.message,
                    httpStatus: err.$metadata?.httpStatusCode,
                }, actionEntry.getLogInfo()));
                return doneOnce(err);
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
        const command = new MultipleBackendInitiateMPUCommand({
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
            RequestUids: log.getSerializedUids(),
        });

        return this.backbeatClient.send(command)
            .then(data => cb(null, data.uploadId))
            .catch(err => {
                log.error('an error occurred on initating MPU to S3',
                Object.assign({
                    method: 'CopyLocationTask._initiateMPU',
                    error: err.message,
                    httpStatus: err.$metadata?.httpStatusCode,
                }, actionEntry.getLogInfo()));
                return cb(err);
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
        const mpuConcLimit = this.repConfig.queueProcessor.mpuPartsConcurrency;
        return async.timesLimit(ranges.length, mpuConcLimit, (n, next) =>
            this._getRangeAndPutMPUPart(actionEntry, objMD, ranges[n],
                n + 1, uploadId, log, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const res = {
                        PartNumber: [parseInt(data.partNumber, 10)],
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
     * Ensure the latest object MD5 hash is the same as in the action entry,
     * and that the object has not already been transitioned to the destination
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
        if (objMD.getDataStoreName() === actionEntry.getAttribute('toLocation')) {
            // The object was already transitioned to the destination location
            return errors.InvalidObjectState.customizeDescription(
                'object already transitioned');
        }
        return null;
    }

    _publishCopyLocationStatus(err, actionEntry, kafkaEntry, log) {
        if (err && !actionEntry.getError()) {
            actionEntry.setError(err);
        }
        log.info('action execution ended', actionEntry.getLogInfo());
        // skip object if it was already transitioned
        if (err && (err.InvalidObjectState || err.name === 'InvalidObjectState')) {
            log.info('object skipped: invalid object state', actionEntry.getLogInfo());
            return { committable: true };
        }
        if (!actionEntry.getResultsTopic()) {
            // no result requested, we may commit immediately
            return { committable: true };
        }
        log.debug('sending to topic', {
            topic: actionEntry.getResultsTopic(),
            message: actionEntry.toKafkaMessage(),
        });
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
        return { committable: false };
    }
}

module.exports = CopyLocationTask;
