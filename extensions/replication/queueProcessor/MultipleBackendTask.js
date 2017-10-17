const async = require('async');

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;

const QueueProcessorTask = require('./QueueProcessorTask');
const attachReqUids = require('../utils/attachReqUids');

class MultipleBackendTask extends QueueProcessorTask {

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

    _getAndPutPartOnce(sourceEntry, destEntry, part, log, done) {
        log.debug('getting object part', { entry: sourceEntry.getLogInfo() });
        const doneOnce = jsutil.once(done);
        // TODO handle zero-byte objects which have no parts
        const partNumber = sourceEntry.getPartNumber(part);
        const sourceReq = this.S3source.getObject({
            Bucket: sourceEntry.getBucket(),
            Key: sourceEntry.getObjectKey(),
            VersionId: sourceEntry.getEncodedVersionId(),
            PartNumber: partNumber,
        });
        attachReqUids(sourceReq, log);
        sourceReq.on('error', err => {
            // eslint-disable-next-line no-param-reassign
            err.origin = 'source';
            if (err.statusCode === 404) {
                log.error('the source object was not found', {
                    method: 'QueueProcessor._getAndPutData',
                    entry: sourceEntry.getLogInfo(),
                    origin: 'source',
                    peer: this.sourceConfig.s3,
                    error: err.message,
                    httpStatus: err.statusCode,
                });
                return doneOnce(err);
            }
            log.error('an error occurred on getObject from S3', {
                method: 'MultipleBackendTask._getAndPutData',
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
                    method: 'QueueProcessor._getAndPutData',
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
                method: 'MultipleBackendTask._getAndPutData',
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
            CanonicalID: destEntry.getOwnerCanonicalId(),
            ContentLength: destEntry.getPartSize(part),
            ContentMD5: destEntry.getPartETag(part),
            StorageType: destEntry.getReplicationStorageType(),
            StorageClass: destEntry.getReplicationStorageClass(),
            VersionId: destEntry.getEncodedVersionId(),
            Body: incomingMsg,
        });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'target';
                log.error('an error occurred on putData to S3', {
                    method: 'MultipleBackendTask._getAndPutData',
                    entry: destEntry.getLogInfo(),
                    origin: 'target',
                    peer: this.destBackbeatHost,
                    error: err.message,
                });
                return doneOnce(err);
            }
            // TODO Set target object's versionId and update source metadata
            // with value
            return doneOnce(null, data);
        });
    }

    processQueueEntry(sourceEntry, done) {
        const log = this.logger.newRequestLogger();
        const destEntry = sourceEntry.toReplicaEntry();
        log.debug('processing entry', { entry: sourceEntry.getLogInfo() });

        return async.waterfall([
            next => this._setupRoles(sourceEntry, log, next),
            (sourceRole, next) =>
                this._getAndPutData(sourceEntry, destEntry, log, next),
        ], err => this._handleReplicationOutcome(err, sourceEntry, destEntry,
            log, done));
    }
}

module.exports = MultipleBackendTask;
