const async = require('async');
const uuid = require('uuid/v4');

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const ObjectMDLocation = require('arsenal').models.ObjectMDLocation;

const ReplicateObject = require('./ReplicateObject');
const attachReqUids = require('../utils/attachReqUids');

const MPU_CONC_LIMIT = 10;

class MultipleBackendTask extends ReplicateObject {

    _setupRolesOnce(entry, log, cb) {
        log.debug('getting bucket replication', { entry: entry.getLogInfo() });
        const entryRolesString = entry.getReplicationRoles();
        let errMessage;
        let entryRoles;
        if (entryRolesString !== undefined) {
            entryRoles = entryRolesString.split(',');
        }
        if (entryRoles === undefined || entryRoles.length !== 1) {
            errMessage = 'expecting a single role in bucket replication ' +
                'configuration when replicating to an external location';
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
            if (roles.length !== 1) {
                errMessage = 'expecting a single role in bucket replication ' +
                    'configuration when replicating to an external location';
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

    _getAndPutMPUPart(sourceEntry, destEntry, part, uploadId, log, cb) {
        this.retry({
            actionDesc: 'stream part data',
            logFields: { entry: sourceEntry.getLogInfo() },
            actionFunc: done => this._getAndPutMPUPartOnce(sourceEntry,
                destEntry, part, uploadId, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _getAndPutMPUPartOnce(sourceEntry, destEntry, part, uploadId, log, done) {
        log.debug('getting object part', { entry: sourceEntry.getLogInfo() });
        const doneOnce = jsutil.once(done);
        const partObj = new ObjectMDLocation(part);
        const sourceReq = this.S3source.getObject({
            Bucket: sourceEntry.getBucket(),
            Key: sourceEntry.getObjectKey(),
            VersionId: sourceEntry.getEncodedVersionId(),
            PartNumber: partObj.getPartNumber(),
        });
        attachReqUids(sourceReq, log);
        sourceReq.on('error', err => {
            // eslint-disable-next-line no-param-reassign
            err.origin = 'source';
            if (err.statusCode === 404) {
                return doneOnce(err);
            }
            log.error('an error occurred on getObject from S3', {
                method: 'MultipleBackendTask._getAndPutMPUPartOnce',
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
                method: 'MultipleBackendTask._getAndPutMPUPartOnce',
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
            ContentLength: partObj.getPartSize(),
            StorageType: destEntry.getReplicationStorageType(),
            StorageClass: destEntry.getReplicationStorageClass(),
            PartNumber: partObj.getPartNumber(),
            UploadId: uploadId,
            Body: incomingMsg,
        });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                log.error('an error occurred on putting MPU part to S3', {
                    method: 'MultipleBackendTask._getAndPutMPUPartOnce',
                    entry: destEntry.getLogInfo(),
                    origin: 'target',
                    peer: this.destBackbeatHost,
                    error: err.message,
                });
                return doneOnce(err);
            }
            return doneOnce(null, data);
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
        const storageType = destEntry.getReplicationStorageType();
        // If using Azure backend, create a unique ID to use as the block ID.
        if (storageType === 'azure') {
            const uploadId = uuid().replace(/-/g, '');
            return setImmediate(() => cb(null, uploadId));
        }
        const destReq = this.backbeatSource.multipleBackendInitiateMPU({
            Bucket: destEntry.getBucket(),
            Key: destEntry.getObjectKey(),
            StorageType: storageType,
            StorageClass: destEntry.getReplicationStorageClass(),
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
                    method: 'MultipleBackendTask._getAndPutMultipartUploadOnce',
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

    _getAndPutMultipartUploadOnce(sourceEntry, destEntry, log, cb) {
        const doneOnce = jsutil.once(cb);
        log.debug('replicating MPU data', { entry: sourceEntry.getLogInfo() });
        const missingDataStoreETag = sourceEntry.getLocation().some(part => {
            const partObj = new ObjectMDLocation(part);
            return partObj.getDataStoreETag() === undefined;
        });
        if (missingDataStoreETag) {
            log.error('cannot replicate object without dataStoreETag property',
                {
                    method: 'MultipleBackendTask._getAndPutMultipartUploadOnce',
                    entry: sourceEntry.getLogInfo(),
                });
            return cb(errors.InvalidObjectState);
        }

        return this._initiateMPU(sourceEntry, destEntry, log,
        (err, uploadId) => {
            if (err) {
                return doneOnce(err);
            }
            const locations = sourceEntry.getReducedLocations();
            return async.mapLimit(locations, MPU_CONC_LIMIT, (part, done) =>
                this._getAndPutMPUPart(sourceEntry, destEntry, part, uploadId,
                    log, (err, data) => {
                        if (err) {
                            return done(err);
                        }
                        const res = {
                            PartNumber: [data.partNumber],
                            ETag: [data.ETag],
                        };
                        if (destEntry.getReplicationStorageType() === 'azure') {
                            res.NumberSubParts = [data.numberSubParts];
                        }
                        return done(null, res);
                    }),
            (err, data) => {
                if (err) {
                    // eslint-disable-next-line no-param-reassign
                    err.origin = 'source';
                    log.error('an error occurred on putting MPU part to S3', {
                        method:
                            'MultipleBackendTask._getAndPutMultipartUploadOnce',
                        entry: destEntry.getLogInfo(),
                        origin: 'target',
                        peer: this.destBackbeatHost,
                        error: err.message,
                    });
                    return doneOnce(err);
                }
                const destReq = this.backbeatSource.multipleBackendCompleteMPU({
                    Bucket: destEntry.getBucket(),
                    Key: destEntry.getObjectKey(),
                    StorageType: destEntry.getReplicationStorageType(),
                    StorageClass: destEntry.getReplicationStorageClass(),
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
                            method: 'MultipleBackendTask.' +
                                '_getAndPutMultipartUploadOnce',
                            entry: destEntry.getLogInfo(),
                            origin: 'target',
                            peer: this.destBackbeatHost,
                            error: err.message,
                        });
                        return doneOnce(err);
                    }
                    sourceEntry
                        .setReplicationDataStoreVersionId(data.versionId);
                    return doneOnce();
                });
            });
        });
    }

    _getAndPutPartOnce(sourceEntry, destEntry, part, log, done) {
        log.debug('getting object part', { entry: sourceEntry.getLogInfo() });
        const doneOnce = jsutil.once(done);
        const partObj = part ? new ObjectMDLocation(part) : undefined;
        const sourceReq = this.S3source.getObject({
            Bucket: sourceEntry.getBucket(),
            Key: sourceEntry.getObjectKey(),
            VersionId: sourceEntry.getEncodedVersionId(),
            PartNumber: part ? partObj.getPartNumber() : undefined,
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
        const destReq = this.backbeatSource.multipleBackendPutObject({
            Bucket: destEntry.getBucket(),
            Key: destEntry.getObjectKey(),
            CanonicalID: destEntry.getOwnerId(),
            ContentLength: part ? partObj.getPartSize() :
                destEntry.getContentLength(),
            ContentMD5: part ? partObj.getPartETag() :
                destEntry.getContentMd5(),
            StorageType: destEntry.getReplicationStorageType(),
            StorageClass: destEntry.getReplicationStorageClass(),
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
            sourceEntry.setReplicationDataStoreVersionId(data.versionId);
            return doneOnce(null, data);
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
                StorageClass: destEntry.getReplicationStorageClass(),
                DataStoreVersionId:
                    destEntry.getReplicationDataStoreVersionId(),
                Tags: JSON.stringify(destEntry.getTags()),
                SourceBucket: sourceEntry.getBucket(),
                SourceVersionId: sourceEntry.getVersionId(),
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
            sourceEntry.setReplicationDataStoreVersionId(data.versionId);
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
            StorageClass: destEntry.getReplicationStorageClass(),
            DataStoreVersionId: destEntry.getReplicationDataStoreVersionId(),
            SourceBucket: sourceEntry.getBucket(),
            SourceVersionId: sourceEntry.getVersionId(),
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
            sourceEntry.setReplicationDataStoreVersionId(data.versionId);
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
        return async.mapLimit(locations, MPU_CONC_LIMIT, (part, done) =>
            this._getAndPutPart(sourceEntry, destEntry, part, log, done), cb);
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
            StorageClass: destEntry.getReplicationStorageClass(),
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
        const destEntry = sourceEntry.toReplicaEntry();
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
