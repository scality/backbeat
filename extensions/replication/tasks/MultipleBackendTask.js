const async = require('async');
const uuid = require('uuid/v4');

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const ObjectMDLocation = require('arsenal').models.ObjectMDLocation;

const ReplicateObject = require('./ReplicateObject');
const { attachReqUids } = require('../../../lib/clients/utils');
const getExtMetrics = require('../utils/getExtMetrics');
const { metricsExtension, metricsTypeQueued, metricsTypeCompleted } =
    require('../constants');

const MPU_CONC_LIMIT = 10;
const MPU_GCP_MAX_PARTS = 1024;

class MultipleBackendTask extends ReplicateObject {

    _getReplicationEndpointType() {
        const replicationEndpoint = this.destConfig.bootstrapList
            .find(endpoint => endpoint.site === this.site);
        return replicationEndpoint.type;
    }

    _setupRolesOnce(entry, log, cb) {
        log.debug('getting bucket replication', { entry: entry.getLogInfo() });
        const entryRolesString = entry.getReplicationRoles();
        let errMessage;
        let entryRoles;
        if (entryRolesString !== undefined) {
            entryRoles = entryRolesString.split(',');
        }
        if (entryRoles === undefined || entryRoles.length > 2) {
            errMessage = 'expecting no more than two roles in bucket ' +
                'replication configuration when replicating to an external ' +
                'location';
            log.error(errMessage, {
                method: 'MultipleBackendTask._setupRolesOnce',
                entry: entry.getLogInfo(),
                roles: entryRolesString,
            });
            return cb(errors.BadRole.customizeDescription(errMessage));
        }
        this.sourceRole = entryRoles[0];

        this._setupSourceClients(this.sourceRole, log);

        const req = this.S3source.getBucketReplication({
            Bucket: entry.getBucket(),
        });
        attachReqUids(req, log);
        return req.send((err, data) => {
            if (err) {
                log.error('error getting replication configuration from S3', {
                    method: 'MultipleBackendTask._setupRolesOnce',
                    entry: entry.getLogInfo(),
                    origin: 'source',
                    peer: this.sourceConfig.s3,
                    error: err.message,
                    httpStatus: err.statusCode,
                });
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                return cb(err);
            }
            const replicationEnabled = data.ReplicationConfiguration.Rules
                .some(rule => rule.Status === 'Enabled' &&
                    entry.getObjectKey().startsWith(rule.Prefix));
            if (!replicationEnabled) {
                errMessage = 'replication disabled for object';
                log.debug(errMessage, {
                    method: 'MultipleBackendTask._setupRolesOnce',
                    entry: entry.getLogInfo(),
                });
                return cb(errors.PreconditionFailed.customizeDescription(
                    errMessage));
            }
            const roles = data.ReplicationConfiguration.Role.split(',');
            if (roles.length > 2) {
                errMessage = 'expecting no more than two roles in bucket ' +
                    'replication configuration when replicating to an ' +
                    'external location';
                log.error(errMessage, {
                    method: 'MultipleBackendTask._setupRolesOnce',
                    entry: entry.getLogInfo(),
                    roles,
                });
                return cb(errors.BadRole.customizeDescription(errMessage));
            }
            if (roles[0] !== entryRoles[0]) {
                log.error('role in replication entry for source does not ' +
                'match role in bucket replication configuration', {
                    method: 'MultipleBackendTask._setupRolesOnce',
                    entry: entry.getLogInfo(),
                    entryRole: entryRoles[0],
                    bucketRole: roles[0],
                });
                return cb(errors.BadRole);
            }
            return cb(null, roles[0]);
        });
    }

    /**
     * This is a retry wrapper for calling _getRangeAndPutMPUPartOnce.
     * @param {ObjectQueueEntry} sourceEntry - The source object entry
     * @param {ObjectQueueEntry} destEntry - The destination object entry
     * @param {String} range - The range to get an object with
     * @param {Number} partNumber - The part number for the current part
     * @param {String} uploadId - The upload ID of the initiated MPU
     * @param {Werelogs} log - The logger instance
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _getRangeAndPutMPUPart(sourceEntry, destEntry, range, partNumber, uploadId,
        log, cb) {
        this.retry({
            actionDesc: 'stream part data',
            logFields: { entry: sourceEntry.getLogInfo() },
            actionFunc: done => this._getRangeAndPutMPUPartOnce(sourceEntry,
                destEntry, range, partNumber, uploadId, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    /**
     * Get the ranged object, calculate the data's size, then put the part.
     * @param {ObjectQueueEntry} sourceEntry - The source object entry
     * @param {ObjectQueueEntry} destEntry - The destination object entry
     * @param {Object} [range] - The range to get an object with
     * @param {Number} range.start - The start byte range
     * @param {Number} range.end - The end byte range
     * @param {Number} partNumber - The part number for the current part
     * @param {String} uploadId - The upload ID of the initiated MPU
     * @param {Werelogs} log - The logger instance
     * @param {Function} done - The callback to call
     * @return {undefined}
     */
    _getRangeAndPutMPUPartOnce(sourceEntry, destEntry, range, partNumber,
        uploadId, log, done) {
        log.debug('getting object range', {
            entry: sourceEntry.getLogInfo(),
            range,
        });
        const doneOnce = jsutil.once(done);
        const sourceReq = this.backbeatSource.getObject({
            Bucket: sourceEntry.getBucket(),
            Key: sourceEntry.getObjectKey(),
            VersionId: sourceEntry.getEncodedVersionId(),
            // A 0-byte part has no range.
            Range: range && `bytes=${range.start}-${range.end}`,
            LocationConstraint: sourceEntry.getDataStoreName(),
        });
        // A 0-byte object has no range, otherwise range is inclusive.
        const size = range ? range.end - range.start + 1 : 0;
        return this._putMPUPart(sourceEntry, destEntry, sourceReq, size,
            uploadId, partNumber, log, doneOnce);
    }

   /**
    * Perform a multipart upload.
    * @param {ObjectQueueEntry} sourceEntry - The source object entry
    * @param {ObjectQueueEntry} destEntry - The destination object entry
    * @param {String} uploadId - The upload ID of the initiated MPU
    * @param {Stream} data - The incoming message of the get request
    * @param {Werelogs} log - The logger instance
    * @param {Function} doneOnce - The callback to call
    * @return {undefined}
    */
    _completeMPU(sourceEntry, destEntry, uploadId, data, log, doneOnce) {
        const destReq = this.backbeatSource.multipleBackendCompleteMPU({
            Bucket: destEntry.getBucket(),
            Key: destEntry.getObjectKey(),
            StorageType: destEntry.getReplicationStorageType(),
            StorageClass: this.site,
            VersionId: destEntry.getEncodedVersionId(),
            UserMetaData: sourceEntry.getUserMetadata(),
            ContentType: sourceEntry.getContentType(),
            CacheControl: sourceEntry.getCacheControl() || undefined,
            ContentDisposition: sourceEntry.getContentDisposition() ||
                undefined,
            ContentEncoding: sourceEntry.getContentEncoding() ||
                undefined,
            UploadId: uploadId,
            Body: JSON.stringify(data),
        });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                log.error('an error occurred on completing MPU to S3', {
                    method: 'MultipleBackendTask._completeMPU',
                    entry: destEntry.getLogInfo(),
                    origin: 'target',
                    peer: this.destBackbeatHost,
                    error: err.message,
                });
                return doneOnce(err);
            }
            sourceEntry.setReplicationSiteDataStoreVersionId(this.site,
                data.versionId);
            return doneOnce();
        });
    }

    /**
     * Get the ranged object, calculate the data's size, then put the part.
     * @param {ObjectQueueEntry} sourceEntry - The source object entry
     * @param {ObjectQueueEntry} destEntry - The destination object entry
     * @param {AWS.Request} sourceReq - The source request for getting data
     * @param {Number} size - The size of the content
     * @param {String} uploadId - The upload ID of the initiated MPU
     * @param {Number} partNumber - The part number of the part
     * @param {Werelogs} log - The logger instance
     * @param {Function} doneOnce - The callback to call
     * @return {undefined}
     */
    _putMPUPart(sourceEntry, destEntry, sourceReq, size, uploadId, partNumber,
        log, doneOnce) {
        attachReqUids(sourceReq, log);
        sourceReq.on('error', err => {
            // eslint-disable-next-line no-param-reassign
            err.origin = 'source';
            if (err.statusCode === 404) {
                return doneOnce(err);
            }
            log.error('an error occurred on getObject from S3', {
                method: 'MultipleBackendTask._putMPUPart',
                entry: sourceEntry.getLogInfo(),
                origin: 'source',
                peer: this.sourceConfig.s3,
                error: err.message,
                httpStatus: err.statusCode,
            });
            return doneOnce(err);
        });
        const incomingMsg = sourceReq.createReadStream();
        incomingMsg.on('error', err => {
            if (err.statusCode === 404) {
                return doneOnce(errors.ObjNotFound);
            }
            // eslint-disable-next-line no-param-reassign
            err.origin = 'source';
            log.error('an error occurred when streaming data from S3', {
                entry: destEntry.getLogInfo(),
                method: 'MultipleBackendTask._putMPUPart',
                origin: 'source',
                peer: this.sourceConfig.s3,
                error: err.message,
            });
            return doneOnce(err);
        });
        log.debug('putting data', { entry: destEntry.getLogInfo() });

        const destReq = this.backbeatSource.multipleBackendPutMPUPart({
            Bucket: destEntry.getBucket(),
            Key: destEntry.getObjectKey(),
            ContentLength: size,
            StorageType: destEntry.getReplicationStorageType(),
            StorageClass: this.site,
            PartNumber: partNumber,
            UploadId: uploadId,
            Body: incomingMsg,
        });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                log.error('an error occurred on putting MPU part to S3', {
                    method: 'MultipleBackendTask._putMPUPart',
                    entry: destEntry.getLogInfo(),
                    origin: 'target',
                    peer: this.destBackbeatHost,
                    error: err.message,
                });
                return doneOnce(err);
            }
            const extMetrics = getExtMetrics(this.site, size, sourceEntry);
            return this.mProducer.publishMetrics(extMetrics,
                metricsTypeCompleted, metricsExtension, () =>
                doneOnce(null, data));
        });
    }

    _getAndPutMultipartUpload(sourceEntry, destEntry, part, uploadId, log, cb) {
        this.retry({
            actionDesc: 'stream part data',
            logFields: { entry: sourceEntry.getLogInfo() },
            actionFunc: done => this._getAndPutMultipartUploadOnce(sourceEntry,
                destEntry, part, uploadId, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _initiateMPU(sourceEntry, destEntry, log, cb) {
        // If using Azure backend, create a unique ID to use as the block ID.
        if (this._getReplicationEndpointType() === 'azure') {
            const uploadId = uuid().replace(/-/g, '');
            return setImmediate(() => cb(null, uploadId));
        }
        const destReq = this.backbeatSource.multipleBackendInitiateMPU({
            Bucket: destEntry.getBucket(),
            Key: destEntry.getObjectKey(),
            StorageType: destEntry.getReplicationStorageType(),
            StorageClass: this.site,
            VersionId: destEntry.getEncodedVersionId(),
            UserMetaData: sourceEntry.getUserMetadata(),
            ContentType: sourceEntry.getContentType(),
            CacheControl: sourceEntry.getCacheControl() || undefined,
            ContentDisposition:
                sourceEntry.getContentDisposition() || undefined,
            ContentEncoding: sourceEntry.getContentEncoding() || undefined,
        });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                log.error('an error occurred on initating MPU to S3', {
                    method: 'MultipleBackendTask._initiateMPU',
                    entry: destEntry.getLogInfo(),
                    origin: 'target',
                    peer: this.destBackbeatHost,
                    error: err.message,
                });
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
        const pow = Math.pow(2, Math.ceil(Math.log(contentLen) / Math.log(2)));
        const rangeSize = Math.ceil(pow / MPU_GCP_MAX_PARTS);
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
     * @param {ObjectQueueEntry} sourceEntry - The source object entry
     * @param {ObjectQueueEntry} destEntry - The destination object entry
     * @param {String} uploadId - The upload ID of the initiated MPU
     * @param {Werelogs} log - The logger instance
     * @param {Function} doneOnce - The callback to call
     * @return {undefined}
     */
    _completeRangedMPU(sourceEntry, destEntry, uploadId, log, doneOnce) {
        const isGCP = this._getReplicationEndpointType() === 'gcp';
        const ranges = this._getRanges(sourceEntry.getContentLength(), isGCP);
        return async.timesLimit(ranges.length, MPU_CONC_LIMIT, (n, next) =>
            this._getRangeAndPutMPUPart(sourceEntry, destEntry, ranges[n],
                n + 1, uploadId, log, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const res = {
                        PartNumber: [data.partNumber],
                        ETag: [data.ETag],
                    };
                    return next(null, res);
                }),
            (err, data) => {
                if (err) {
                    // eslint-disable-next-line no-param-reassign
                    err.origin = 'source';
                    log.error('an error occurred on putting MPU part to S3', {
                        method:
                            'MultipleBackendTask._completeRangedMPU',
                        entry: destEntry.getLogInfo(),
                        origin: 'target',
                        peer: this.destBackbeatHost,
                        error: err.message,
                    });
                    return doneOnce(err);
                }
                return this._completeMPU(sourceEntry, destEntry, uploadId, data,
                    log, doneOnce);
            });
    }

    _getAndPutMultipartUploadOnce(sourceEntry, destEntry, log, cb) {
        const doneOnce = jsutil.once(cb);
        log.debug('replicating MPU data', { entry: sourceEntry.getLogInfo() });
        return this._initiateMPU(sourceEntry, destEntry, log,
        (err, uploadId) => {
            if (err) {
                return doneOnce(err);
            }
            const extMetrics = getExtMetrics(this.site,
                sourceEntry.getContentLength(), sourceEntry);
            return this.mProducer.publishMetrics(extMetrics, metricsTypeQueued,
                metricsExtension, () => this._completeRangedMPU(sourceEntry,
                    destEntry, uploadId, log, doneOnce));
        });
    }

    _getAndPutPartOnce(sourceEntry, destEntry, part, log, done) {
        log.debug('getting object part', { entry: sourceEntry.getLogInfo() });
        const doneOnce = jsutil.once(done);
        const partObj = part ? new ObjectMDLocation(part) : undefined;
        const sourceReq = this.backbeatSource.getObject({
            Bucket: sourceEntry.getBucket(),
            Key: sourceEntry.getObjectKey(),
            VersionId: sourceEntry.getEncodedVersionId(),
            PartNumber: part ? partObj.getPartNumber() : undefined,
            LocationConstraint: sourceEntry.getDataStoreName(),
        });
        attachReqUids(sourceReq, log);
        sourceReq.on('error', err => {
            // eslint-disable-next-line no-param-reassign
            err.origin = 'source';
            if (err.statusCode === 404) {
                log.error('the source object was not found', {
                    method: 'MultipleBackendTask._getAndPutPartOnce',
                    entry: sourceEntry.getLogInfo(),
                    origin: 'source',
                    peer: this.sourceConfig.s3,
                    error: err.message,
                    httpStatus: err.statusCode,
                });
                return doneOnce(err);
            }
            log.error('an error occurred on getObject from S3', {
                method: 'MultipleBackendTask._getAndPutPartOnce',
                entry: sourceEntry.getLogInfo(),
                origin: 'source',
                peer: this.sourceConfig.s3,
                error: err.message,
                httpStatus: err.statusCode,
            });
            return doneOnce(err);
        });
        const incomingMsg = sourceReq.createReadStream();
        incomingMsg.on('error', err => {
            if (err.statusCode === 404) {
                log.error('the source object was not found', {
                    method: 'MultipleBackendTask._getAndPutPartOnce',
                    entry: sourceEntry.getLogInfo(),
                    origin: 'source',
                    peer: this.sourceConfig.s3,
                    error: err.message,
                    httpStatus: err.statusCode,
                });
                return doneOnce(errors.ObjNotFound);
            }
            // eslint-disable-next-line no-param-reassign
            err.origin = 'source';
            log.error('an error occurred when streaming data from S3', {
                entry: destEntry.getLogInfo(),
                method: 'MultipleBackendTask._getAndPutPartOnce',
                origin: 'source',
                peer: this.sourceConfig.s3,
                error: err.message,
            });
            return doneOnce(err);
        });
        log.debug('putting data', { entry: destEntry.getLogInfo() });
        const size = part ? partObj.getPartSize() :
            destEntry.getContentLength();
        const destReq = this.backbeatSource.multipleBackendPutObject({
            Bucket: destEntry.getBucket(),
            Key: destEntry.getObjectKey(),
            CanonicalID: destEntry.getOwnerId(),
            ContentLength: size,
            ContentMD5: part ? partObj.getPartETag() :
                destEntry.getContentMd5(),
            StorageType: destEntry.getReplicationStorageType(),
            StorageClass: this.site,
            VersionId: destEntry.getEncodedVersionId(),
            UserMetaData: sourceEntry.getUserMetadata(),
            ContentType: sourceEntry.getContentType() || undefined,
            CacheControl: sourceEntry.getCacheControl() || undefined,
            ContentDisposition:
                sourceEntry.getContentDisposition() || undefined,
            ContentEncoding: sourceEntry.getContentEncoding() || undefined,
            Body: incomingMsg,
        });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                log.error('an error occurred on putData to S3', {
                    method: 'MultipleBackendTask._getAndPutPartOnce',
                    entry: destEntry.getLogInfo(),
                    origin: 'target',
                    peer: this.destBackbeatHost,
                    error: err.message,
                });
                return doneOnce(err);
            }
            sourceEntry.setReplicationSiteDataStoreVersionId(this.site,
                data.versionId);
            const extMetrics = getExtMetrics(this.site, size, sourceEntry);
            return this.mProducer.publishMetrics(extMetrics,
                metricsTypeCompleted, metricsExtension, () =>
                doneOnce(null, data));
        });
    }

    _putObjectTagging(sourceEntry, destEntry, log, cb) {
        this.retry({
            actionDesc: 'send object tagging XML data',
            entry: sourceEntry,
            logFields: { entry: sourceEntry.getLogInfo() },
            actionFunc: done => this._putObjectTaggingOnce(
                sourceEntry, destEntry, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _putObjectTaggingOnce(sourceEntry, destEntry, log, cb) {
        const doneOnce = jsutil.once(cb);
        log.debug('replicating object tags', {
            entry: sourceEntry.getLogInfo(),
        });
        const destReq = this.backbeatSource
            .multipleBackendPutObjectTagging({
                Bucket: destEntry.getBucket(),
                Key: destEntry.getObjectKey(),
                StorageType: destEntry.getReplicationStorageType(),
                StorageClass: this.site,
                DataStoreVersionId:
                    destEntry.getReplicationSiteDataStoreVersionId(this.site),
                Tags: JSON.stringify(destEntry.getTags()),
                SourceBucket: sourceEntry.getBucket(),
                SourceVersionId: sourceEntry.getVersionId(),
                ReplicationEndpointSite: this.site,
            });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                log.error('an error occurred putting object tagging to S3', {
                    method: 'MultipleBackendTask._putObjectTaggingOnce',
                    entry: destEntry.getLogInfo(),
                    origin: 'target',
                    error: err.message,
                });
                return doneOnce(err);
            }
            sourceEntry.setReplicationSiteDataStoreVersionId(this.site,
                data.versionId);
            return doneOnce();
        });
    }

    _deleteObjectTagging(sourceEntry, destEntry, log, cb) {
        this.retry({
            actionDesc: 'delete object tagging',
            logFields: { entry: sourceEntry.getLogInfo() },
            actionFunc: done => this._deleteObjectTaggingOnce(sourceEntry,
                destEntry, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _deleteObjectTaggingOnce(sourceEntry, destEntry, log, cb) {
        const doneOnce = jsutil.once(cb);
        log.debug('replicating delete object tagging', {
            entry: sourceEntry.getLogInfo(),
        });
        const destReq = this.backbeatSource.multipleBackendDeleteObjectTagging({
            Bucket: destEntry.getBucket(),
            Key: destEntry.getObjectKey(),
            StorageType: destEntry.getReplicationStorageType(),
            StorageClass: this.site,
            DataStoreVersionId:
                destEntry.getReplicationSiteDataStoreVersionId(this.site),
            SourceBucket: sourceEntry.getBucket(),
            SourceVersionId: sourceEntry.getVersionId(),
            ReplicationEndpointSite: this.site,
        });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                log.error('an error occurred on deleting object tagging', {
                    method: 'MultipleBackendTask._deleteObjectTaggingOnce',
                    entry: destEntry.getLogInfo(),
                    origin: 'target',
                    peer: this.destBackbeatHost,
                    error: err.message,
                });
                return doneOnce(err);
            }
            sourceEntry.setReplicationSiteDataStoreVersionId(this.site,
                data.versionId);
            return doneOnce();
        });
    }

    _getAndPutData(sourceEntry, destEntry, log, cb) {
        log.debug('replicating data', { entry: sourceEntry.getLogInfo() });
        if (sourceEntry.getLocation().some(part => {
            const partObj = new ObjectMDLocation(part);
            return partObj.getDataStoreETag() === undefined;
        })) {
            log.error('cannot replicate object without dataStoreETag property',
                {
                    method: 'MultipleBackendTask._getAndPutData',
                    entry: sourceEntry.getLogInfo(),
                });
            return cb(errors.InvalidObjectState);
        }
        const locations = sourceEntry.getReducedLocations();
        // Metadata-only operations have no part locations.
        if (locations.length === 0) {
            return this._getAndPutPart(sourceEntry, destEntry, null, log, cb);
        }
        const extMetrics = getExtMetrics(this.site,
            sourceEntry.getContentLength(), sourceEntry);
        return this.mProducer.publishMetrics(extMetrics,
            metricsTypeQueued, metricsExtension, () =>
            async.mapLimit(locations, MPU_CONC_LIMIT, (part, done) =>
            this._getAndPutPart(sourceEntry, destEntry, part, log, done), cb));
    }

    _putDeleteMarker(sourceEntry, destEntry, log, cb) {
        this.retry({
            actionDesc: 'put delete marker',
            logFields: { entry: sourceEntry.getLogInfo() },
            actionFunc: done => this._putDeleteMarkerOnce(
                sourceEntry, destEntry, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _putDeleteMarkerOnce(sourceEntry, destEntry, log, cb) {
        const doneOnce = jsutil.once(cb);
        log.debug('replicating delete marker', {
            entry: sourceEntry.getLogInfo(),
        });
        const destReq = this.backbeatSource.multipleBackendDeleteObject({
            Bucket: destEntry.getBucket(),
            Key: destEntry.getObjectKey(),
            StorageType: destEntry.getReplicationStorageType(),
            StorageClass: this.site,
        });
        attachReqUids(destReq, log);
        return destReq.send(err => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                log.error('an error occurred on putting delete marker to S3', {
                    method: 'MultipleBackendTask._putDeleteMarkerOnce',
                    entry: destEntry.getLogInfo(),
                    origin: 'target',
                    peer: this.destBackbeatHost,
                    error: err.message,
                });
                return doneOnce(err);
            }
            return doneOnce();
        });
    }

    processQueueEntry(sourceEntry, done) {
        const log = this.logger.newRequestLogger();
        const destEntry = sourceEntry.toMultipleBackendReplicaEntry(this.site);
        log.debug('processing entry', { entry: sourceEntry.getLogInfo() });

        return async.waterfall([
            next => this._setupRoles(sourceEntry, log, next),
            (sourceRole, next) => {
                if (sourceEntry.getIsDeleteMarker()) {
                    return this._putDeleteMarker(sourceEntry, destEntry, log,
                        next);
                }
                const content = sourceEntry.getReplicationContent();
                if (content.includes('MPU')) {
                    return this._getAndPutMultipartUpload(sourceEntry,
                        destEntry, log, next);
                }
                if (content.includes('PUT_TAGGING')) {
                    return this._putObjectTagging(sourceEntry, destEntry,
                        log, next);
                }
                if (content.includes('DELETE_TAGGING')) {
                    return this._deleteObjectTagging(sourceEntry, destEntry,
                        log, next);
                }
                return this._getAndPutData(sourceEntry, destEntry, log, next);
            },
        ], err => this._handleReplicationOutcome(err, sourceEntry, destEntry,
            log, done));
    }
}

module.exports = MultipleBackendTask;
