const async = require('async');
const AWS = require('aws-sdk');

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const ObjectMDLocation = require('arsenal').models.ObjectMDLocation;

const BackbeatClient = require('../../../lib/clients/BackbeatClient');

const { attachReqUids } = require('../../../lib/clients/utils');
const getExtMetrics = require('../utils/getExtMetrics');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { getAccountCredentials } =
          require('../../../lib/credentials/AccountCredentials');
const RoleCredentials =
          require('../../../lib/credentials/RoleCredentials');
const { metricsExtension, metricsTypeQueued, metricsTypeCompleted,
    replicationBackends } = require('../constants');

const MPU_CONC_LIMIT = 10;
const MPU_GCP_MAX_PARTS = 1024;

function _extractAccountIdFromRole(role) {
    return role.split(':')[4];
}

class ReplicateObject extends BackbeatTask {
    /**
     * Process a single replication entry
     *
     * @constructor
     * @param {QueueProcessor} qp - queue processor instance
     */
    constructor(qp) {
        const qpState = qp.getStateVars();
        super({
            retryTimeoutS: qpState.repConfig.queueProcessor.retryTimeoutS,
        });
        Object.assign(this, qpState);

        this.sourceRole = null;
        this.targetRole = null;
        this.destBackbeatHost = null;
        this.s3sourceCredentials = null;
        this.s3destCredentials = null;
        this.S3source = null;
        this.backbeatSource = null;
        this.backbeatDest = null;
    }

    _createCredentials(where, authConfig, roleArn, log) {
        const accountCredentials = getAccountCredentials(authConfig, log);
        if (accountCredentials) {
            return accountCredentials;
        }
        let vaultclient;
        if (where === 'source') {
            vaultclient = this.vaultclientCache.getClient('source:s3');
        } else { // target
            const { host, port } = this.destHosts.pickHost();
            vaultclient = this.vaultclientCache.getClient('dest:s3',
                                                          host, port);
        }
        return new RoleCredentials(vaultclient, 'replication', roleArn, log);
    }

    _setupRoles(entry, log, cb) {
        this.retry({
            actionDesc: 'get bucket replication configuration',
            logFields: { entry: entry.getLogInfo() },
            actionFunc: done => this._setupRolesOnce(entry, log, done),
            // Rely on AWS SDK notion of retryable error to decide if
            // we should set the entry replication status to FAILED
            // (non retryable) or retry later.
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _setTargetAccountMd(destEntry, targetRole, log, cb) {
        if (!this.destHosts) {
            log.warn('cannot process entry: no target site configured',
                     { entry: destEntry.getLogInfo() });
            return cb(errors.InternalError);
        }
        this._setupDestClients(this.targetRole, log);

        return this.retry({
            actionDesc: 'lookup target account attributes',
            logFields: { entry: destEntry.getLogInfo() },
            actionFunc: done => this._setTargetAccountMdOnce(
                destEntry, targetRole, log, done),
            // this call uses our own Vault client which does not set
            // the 'retryable' field
            shouldRetryFunc: err =>
                (err.InternalError || err.code === 'InternalError' ||
                 err.ServiceUnavailable || err.code === 'ServiceUnavailable'),
            onRetryFunc: () => {
                this.destHosts.pickNextHost();
                this._setupDestClients(this.targetRole, log);
            },
            log,
        }, cb);
    }

    _getDataAndPutPart(params, log, cb) {
        const { sourceEntry, part, range } = params;
        const partLogger = this.logger.newRequestLogger(log.getUids());
        this.retry({
            actionDesc: 'stream part data',
            logFields: { entry: sourceEntry.getLogInfo(), part, range },
            actionFunc: (range ?
                done => this._getRangeAndPutPartOnce(params, log, done) :
                done => this._getPartAndPutPartOnce(params, log, done)),
            shouldRetryFunc: err => err.retryable,
            onRetryFunc: err => {
                if (err.origin === 'target') {
                    this.destHosts.pickNextHost();
                    this._setupDestClients(this.targetRole, partLogger);
                }
            },
            log: partLogger,
        }, cb);
    }

    _putMetadata(entry, mdOnly, log, cb) {
        this.retry({
            actionDesc: 'update metadata on target',
            logFields: { entry: entry.getLogInfo() },
            actionFunc: done => this._putMetadataOnce(entry, mdOnly,
                                                      log, done),
            shouldRetryFunc: err => err.retryable,
            onRetryFunc: err => {
                if (err.origin === 'target') {
                    this.destHosts.pickNextHost();
                    this._setupDestClients(this.targetRole, log);
                }
            },
            log,
        }, cb);
    }

    _publishReplicationStatus(sourceEntry, replicationStatus, params, cb) {
        const { log, reason } = params;
        const updatedSourceEntry = replicationStatus === 'COMPLETED' ?
            sourceEntry.toCompletedEntry(this.site) :
            sourceEntry.toFailedEntry(this.site);
        updatedSourceEntry.setReplicationSiteDataStoreVersionId(this.site,
            sourceEntry.getReplicationSiteDataStoreVersionId(this.site));
        const kafkaEntries = [updatedSourceEntry.toKafkaEntry(this.site)];
        return this.replicationStatusProducer.send(kafkaEntries, err => {
            if (err) {
                log.error(
                    'error publishing entry to replication status topic',
                    { method: 'ReplicateObject._publishReplicationStatus',
                      topic: this.repConfig.replicationStatusTopic,
                      entry: updatedSourceEntry.getLogInfo(),
                      replicationStatus:
                      updatedSourceEntry.getReplicationStatus(),
                      error: err });
                return cb(err);
            }
            log.end().info('replication status published', {
                topic: this.repConfig.replicationStatusTopic,
                entry: updatedSourceEntry.getLogInfo(),
                replicationStatus,
                reason,
            });
            return cb();
        });
    }

    _setupRolesOnce(entry, log, cb) {
        log.debug('getting bucket replication',
                  { entry: entry.getLogInfo() });
        const entryRolesString = entry.getReplicationRoles();
        let entryRoles;
        if (entryRolesString !== undefined) {
            entryRoles = entryRolesString.split(',');
        }
        if (entryRoles === undefined || entryRoles.length !== 2) {
            log.error('expecting two roles separated by a ' +
                      'comma in entry replication configuration',
                { method: 'ReplicateObject._setupRolesOnce',
                    entry: entry.getLogInfo(),
                    roles: entryRolesString });
            return cb(errors.BadRole);
        }
        this.sourceRole = entryRoles[0];
        this.targetRole = entryRoles[1];

        this._setupSourceClients(this.sourceRole, log);

        const req = this.S3source.getBucketReplication(
            { Bucket: entry.getBucket() });
        attachReqUids(req, log);
        return req.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                log.error('error getting replication ' +
                          'configuration from S3',
                    { method: 'ReplicateObject._setupRolesOnce',
                        entry: entry.getLogInfo(),
                        origin: 'source',
                        peer: this.sourceConfig.s3,
                        error: err.message,
                        httpStatus: err.statusCode });
                return cb(err);
            }
            const replicationEnabled = (
                data.ReplicationConfiguration.Rules.some(
                    rule => entry.getObjectKey().startsWith(rule.Prefix)
                        && rule.Status === 'Enabled'));
            if (!replicationEnabled) {
                log.debug('replication disabled for object',
                    { method: 'ReplicateObject._setupRolesOnce',
                        entry: entry.getLogInfo() });
                return cb(errors.PreconditionFailed.customizeDescription(
                    'replication disabled for object'));
            }
            const roles = data.ReplicationConfiguration.Role.split(',');
            if (roles.length !== 2) {
                log.error('expecting two roles separated by a ' +
                          'comma in bucket replication configuration',
                    { method: 'ReplicateObject._setupRolesOnce',
                        entry: entry.getLogInfo(),
                        roles });
                return cb(errors.BadRole);
            }
            if (roles[0] !== entryRoles[0]) {
                log.error('role in replication entry for source does ' +
                          'not match role in bucket replication ' +
                          'configuration ',
                    { method: 'ReplicateObject._setupRolesOnce',
                        entry: entry.getLogInfo(),
                        entryRole: entryRoles[0],
                        bucketRole: roles[0] });
                return cb(errors.BadRole);
            }
            if (roles[1] !== entryRoles[1]) {
                log.error('role in replication entry for target does ' +
                          'not match role in bucket replication ' +
                          'configuration ',
                    { method: 'ReplicateObject._setupRolesOnce',
                        entry: entry.getLogInfo(),
                        entryRole: entryRoles[1],
                        bucketRole: roles[1] });
                return cb(errors.BadRole);
            }
            return cb(null, roles[0], roles[1]);
        });
    }

    _setTargetAccountMdOnce(destEntry, targetRole, log, cb) {
        log.debug('changing target account owner',
                  { entry: destEntry.getLogInfo() });
        const targetAccountId = _extractAccountIdFromRole(targetRole);
        this.s3destCredentials.lookupAccountAttributes(
            targetAccountId, (err, accountAttr) => {
                if (err) {
                    // eslint-disable-next-line no-param-reassign
                    err.origin = 'target';
                    log.error('an error occurred when looking up target ' +
                              'account attributes',
                        { method: 'ReplicateObject._setTargetAccountMdOnce',
                            entry: destEntry.getLogInfo(),
                            origin: 'target',
                            peer: (this.destConfig.auth.type === 'role' ?
                                   this.destConfig.auth.vault : undefined),
                            error: err.message });
                    return cb(err);
                }
                log.debug('setting owner info in target metadata',
                    { entry: destEntry.getLogInfo(),
                        accountAttr });
                destEntry.setOwnerId(accountAttr.canonicalID);
                destEntry.setOwnerDisplayName(accountAttr.displayName);
                return cb();
            });
    }

    /**
     * Get byte ranges for an object of the given content length, such that the
     * range count does not exceed 1024 parts if replicating to GCP or does not
     * exceed 10000 parts otherwise. This method also optimizes for the
     * subsquent range requests by returning ranges that are sized as powers of
     * two (aside from the last part).
     * @param {Number} contentLen - The content length of the whole object
     * @param {Boolean} isGCP - Whether the object is being replicated to GCP
     * @return {Array} The array of byte ranges.
     */
    _getRanges(contentLen, isGCP) {
        // 5MB min part size rounded up to the nearest power of two is 8388608.
        let partCount = Math.pow(2,
            Math.ceil(Math.log(contentLen / 8388608) / Math.log(2)));
        // The GCP storage type does not accept an MPU that is > 1024 parts, so
        // we perform an MPU of <= 1024 parts if replicating to a GCP location.
        // Otherwise, we use a max part count of 8192 which is the AWS max part
        // count of 10000 rounded down to the nearest power of two. The AWS max
        // part size is 5GB, so an MPU of 8192 parts allows for reaching the AWS
        // max object size of 5T.
        const maxPartCount = isGCP ? MPU_GCP_MAX_PARTS : 8192;
        if (partCount > maxPartCount) {
            partCount = maxPartCount;
        }
        const pow = Math.pow(2, Math.ceil(Math.log(contentLen) / Math.log(2)));
        const range = Math.ceil(pow / partCount);
        const ranges = [];
        let start = 0;
        let end = 0;
        while (end < contentLen - 1) {
            end = start + range - 1;
            if (end < contentLen - 1) {
                ranges.push({ start, end });
                start = end + 1;
            }
        }
        ranges.push({ start, end: contentLen - 1 });
        return ranges;
    }

    _getAndPutData(sourceEntry, destEntry, log, cb) {
        log.debug('replicating data', { entry: sourceEntry.getLogInfo() });
        const isExternalMD = sourceEntry.getLocation().find(part => {
            const partObj = new ObjectMDLocation(part);
            const dataStoreType = partObj.getDataStoreType();
            return replicationBackends.includes(dataStoreType);
        });
        if (isExternalMD) {
            const contentLength = sourceEntry.getContentLength();
            const ranges = this._getRanges(contentLength, false);
            return async.eachLimit(ranges, MPU_CONC_LIMIT, (range, done) => {
                const params = { sourceEntry, destEntry, range };
                return this._getDataAndPutPart(params, log, done);
            }, err => cb(err, null));
        }
        if (sourceEntry.getLocation().some(part => {
            const partObj = new ObjectMDLocation(part);
            return partObj.getDataStoreETag() === undefined;
        })) {
            log.error('cannot replicate object without dataStoreETag ' +
                      'property',
                      { method: 'ReplicateObject._getAndPutData',
                        entry: sourceEntry.getLogInfo() });
            return cb(errors.InvalidObjectState);
        }
        const locations = sourceEntry.getReducedLocations();
        return async.mapLimit(locations, MPU_CONC_LIMIT, (part, done) => {
            const params = { sourceEntry, destEntry, part };
            return this._getDataAndPutPart(params, log, done);
        }, cb);
    }

    _getRangeAndPutPartOnce(params, log, done) {
        const { sourceEntry, range } = params;
        const sourceReq = this.S3source.getObject({
            Bucket: sourceEntry.getBucket(),
            Key: sourceEntry.getObjectKey(),
            VersionId: sourceEntry.getEncodedVersionId(),
            Range: `bytes=${range.start}-${range.end}`,
        });
        Object.assign(params, { sourceReq });
        return this._putPartOnce(params, log, done);
    }

    _getPartAndPutPartOnce(params, log, done) {
        const { sourceEntry, part } = params;
        const partObj = new ObjectMDLocation(part);
        const sourceReq = this.S3source.getObject({
            Bucket: sourceEntry.getBucket(),
            Key: sourceEntry.getObjectKey(),
            VersionId: sourceEntry.getEncodedVersionId(),
            PartNumber: partObj.getPartNumber(),
        });
        Object.assign(params, { partObj, sourceReq });
        return this._putPartOnce(params, log, done);
    }

    _putPartOnce(params, log, done) {
        const { sourceReq, sourceEntry, destEntry, part, partObj, range } =
            params;
        const doneOnce = jsutil.once(done);
        attachReqUids(sourceReq, log);
        sourceReq.on('error', err => {
            // eslint-disable-next-line no-param-reassign
            err.origin = 'source';
            if (err.statusCode === 404) {
                return doneOnce(err);
            }
            log.error('an error occurred on getObject from S3',
                { method: 'ReplicateObject._putPartOnce',
                    entry: sourceEntry.getLogInfo(),
                    part,
                    origin: 'source',
                    peer: this.sourceConfig.s3,
                    error: err.message,
                    httpStatus: err.statusCode });
            return doneOnce(err);
        });
        const incomingMsg = sourceReq.createReadStream();
        incomingMsg.on('error', err => {
            if (err.statusCode === 404) {
                return doneOnce(errors.ObjNotFound);
            }
            // eslint-disable-next-line no-param-reassign
            err.origin = 'source';
            log.error('an error occurred when streaming data from S3',
                { method: 'ReplicateObject._putPartOnce',
                    entry: destEntry.getLogInfo(),
                    part,
                    origin: 'source',
                    peer: this.sourceConfig.s3,
                    error: err.message });
            return doneOnce(err);
        });
        log.debug('putting data', {
            entry: destEntry.getLogInfo(),
            part,
            range,
        });
        const size = range ?
            range.end - range.start + 1 : partObj.getPartSize();
        const destReq = this.backbeatDest.putData({
            Bucket: destEntry.getBucket(),
            Key: destEntry.getObjectKey(),
            CanonicalID: destEntry.getOwnerId(),
            ContentLength: size,
            Body: incomingMsg,
        });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'target';
                log.error('an error occurred on putData to S3',
                    { method: 'ReplicateObject._putPartOnce',
                        entry: destEntry.getLogInfo(),
                        part,
                        range,
                        origin: 'target',
                        peer: this.destBackbeatHost,
                        error: err.message });
                return doneOnce(err);
            }
            let locationVal = null;
            if (partObj) {
                locationVal = partObj
                    .setDataLocation(data.Location[0])
                    .getValue();
            }
            const extMetrics = getExtMetrics(this.site, size, sourceEntry);
            return this.mProducer.publishMetrics(extMetrics,
                metricsTypeCompleted, metricsExtension, () =>
                doneOnce(null, locationVal));
        });
    }

    _putMetadataOnce(entry, mdOnly, log, cb) {
        log.debug('putting metadata',
                  { where: 'target', entry: entry.getLogInfo(),
                    replicationStatus:
                        entry.getReplicationSiteStatus(this.site) });
        const cbOnce = jsutil.once(cb);

        // sends extra header x-scal-replication-content to the target
        // if it's a metadata operation only
        const replicationContent = (mdOnly ? 'METADATA' : undefined);
        const mdBlob = entry.getSerialized();
        const req = this.backbeatDest.putMetadata({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            ContentLength: Buffer.byteLength(mdBlob),
            Body: mdBlob,
            ReplicationContent: replicationContent,
        });
        attachReqUids(req, log);
        req.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'target';
                if (err.ObjNotFound || err.code === 'ObjNotFound') {
                    return cbOnce(err);
                }
                log.error('an error occurred when putting metadata to S3',
                    { method: 'ReplicateObject._putMetadataOnce',
                      entry: entry.getLogInfo(),
                      origin: 'target',
                      peer: this.destBackbeatHost,
                      error: err.message });
                return cbOnce(err);
            }
            return cbOnce(null, data);
        });
    }

    _setupSourceClients(sourceRole, log) {
        this.s3sourceCredentials =
            this._createCredentials('source', this.sourceConfig.auth,
                                    sourceRole, log);

        // Disable retries, use our own retry policy (mandatory for
        // putData route in order to fetch data again from source).

        const sourceS3 = this.sourceConfig.s3;
        this.S3source = new AWS.S3({
            endpoint: `${this.sourceConfig.transport}://` +
                `${sourceS3.host}:${sourceS3.port}`,
            credentials: this.s3sourceCredentials,
            sslEnabled: this.sourceConfig.transport === 'https',
            s3ForcePathStyle: true,
            signatureVersion: 'v4',
            httpOptions: { agent: this.sourceHTTPAgent, timeout: 0 },
            maxRetries: 0,
        });
        this.backbeatSource = new BackbeatClient({
            endpoint: `${this.sourceConfig.transport}://` +
                `${sourceS3.host}:${sourceS3.port}`,
            credentials: this.s3sourceCredentials,
            sslEnabled: this.sourceConfig.transport === 'https',
            httpOptions: { agent: this.sourceHTTPAgent, timeout: 0 },
            maxRetries: 0,
        });
    }

    _setupDestClients(targetRole, log) {
        this.s3destCredentials =
            this._createCredentials('target', this.destConfig.auth,
                                    targetRole, log);

        this.destBackbeatHost = this.destHosts.pickHost();
        this.backbeatDest = new BackbeatClient({
            endpoint: `${this.destConfig.transport}://` +
                `${this.destBackbeatHost.host}:${this.destBackbeatHost.port}`,
            credentials: this.s3destCredentials,
            sslEnabled: this.destConfig.transport === 'https',
            httpOptions: { agent: this.destHTTPAgent, timeout: 0 },
            maxRetries: 0,
        });
    }

    processQueueEntry(sourceEntry, done) {
        const log = this.logger.newRequestLogger();
        const destEntry = sourceEntry.toReplicaEntry(this.site);

        log.debug('processing entry',
                  { entry: sourceEntry.getLogInfo() });


        if (sourceEntry.getIsDeleteMarker()) {
            return async.waterfall([
                next => {
                    this._setupRoles(sourceEntry, log, next);
                },
                (sourceRole, targetRole, next) => {
                    this._setTargetAccountMd(destEntry, targetRole, log,
                                             next);
                },
                // put metadata in target bucket
                next => {
                    // TODO check that bucket role matches role in metadata
                    this._putMetadata(destEntry, false, log, next);
                },
            ], err => this._handleReplicationOutcome(err, sourceEntry,
                                                     destEntry, log, done));
        }

        const mdOnly = !sourceEntry.getReplicationContent().includes('DATA');
        return async.waterfall([
            // get data stream from source bucket
            next => {
                this._setupRoles(sourceEntry, log, next);
            },
            (sourceRole, targetRole, next) => {
                this._setTargetAccountMd(destEntry, targetRole, log, next);
            },
            // Get data from source bucket and put it on the target bucket
            next => {
                if (!mdOnly) {
                    const extMetrics = getExtMetrics(this.site,
                        sourceEntry.getContentLength(), sourceEntry);
                    return this.mProducer.publishMetrics(extMetrics,
                        metricsTypeQueued, metricsExtension, () =>
                            this._getAndPutData(sourceEntry, destEntry, log,
                                next));
                }
                return next(null, []);
            },
            // update location, replication status and put metadata in
            // target bucket
            (location, next) => {
                if (location) {
                    destEntry.setLocation(location);
                }
                this._putMetadata(destEntry, mdOnly, log, next);
            },
        ], err => this._handleReplicationOutcome(err, sourceEntry, destEntry,
                                                 log, done));
    }

    _processQueueEntryRetryFull(sourceEntry, destEntry, log, done) {
        log.debug('reprocessing entry as full replication',
                  { entry: sourceEntry.getLogInfo() });

        return async.waterfall([
            next => this._getAndPutData(sourceEntry, destEntry, log, next),
            // update location, replication status and put metadata in
            // target bucket
            (location, next) => {
                destEntry.setLocation(location);
                this._putMetadata(destEntry, false, log, next);
            },
        ], err => this._handleReplicationOutcome(err, sourceEntry, destEntry,
                                                 log, done));
    }

    _handleReplicationOutcome(err, sourceEntry, destEntry, log, done) {
        if (!err) {
            log.debug('replication succeeded for object, publishing ' +
                      'replication status as COMPLETED',
                      { entry: sourceEntry.getLogInfo() });
            return this._publishReplicationStatus(sourceEntry, 'COMPLETED',
                { log }, done);
        }
        if (err.BadRole ||
            (err.origin === 'source' &&
             (err.NoSuchEntity || err.code === 'NoSuchEntity' ||
              err.AccessDenied || err.code === 'AccessDenied'))) {
            log.error('replication failed permanently for object, ' +
                      'processing skipped',
                { failMethod: err.method,
                    entry: sourceEntry.getLogInfo(),
                    origin: err.origin,
                    error: err.description });
            return done();
        }
        if (err.ObjNotFound || err.code === 'ObjNotFound') {
            if (err.origin === 'source') {
                log.info('replication skipped: ' +
                         'source object version does not exist',
                         { entry: sourceEntry.getLogInfo() });
                return done();
            }
            log.info('target object version does not exist, retrying ' +
                     'a full replication',
                     { entry: sourceEntry.getLogInfo() });
            return this._processQueueEntryRetryFull(sourceEntry, destEntry,
                                                    log, done);
        }
        log.debug('replication failed permanently for object, ' +
                  'publishing replication status as FAILED',
            { failMethod: err.method,
                entry: sourceEntry.getLogInfo(),
                error: err.description });
        return this._publishReplicationStatus(sourceEntry, 'FAILED',
            { log, reason: err.description }, done);
    }
}

module.exports = ReplicateObject;
