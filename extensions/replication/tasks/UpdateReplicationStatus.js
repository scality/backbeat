const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;

const ObjectQueueEntry = require('../../replication/utils/ObjectQueueEntry');
const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const attachReqUids = require('../utils/attachReqUids');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const {
    StaticFileAccountCredentials,
    ProvisionedServiceAccountCredentials,
} = require('../../../lib/credentials/AccountCredentials');
const RoleCredentials =
          require('../../../lib/credentials/RoleCredentials');

class UpdateReplicationStatus extends BackbeatTask {
    /**
     * Update a source object replication status from a kafka entry
     *
     * @constructor
     * @param {ReplicationStatusProcessor} rsp - replication status
     *   processor instance
     */
    constructor(rsp) {
        const rspState = rsp.getStateVars();
        super({
            retryTimeoutS:
            rspState.repConfig.replicationStatusProcessor.retryTimeoutS,
        });
        Object.assign(this, rspState);

        this.sourceRole = null;
        this.s3sourceCredentials = null;
        this.backbeatSource = null;
    }

    _createCredentials(authConfig, roleArn, log) {
        if (authConfig.type === 'account') {
            return new StaticFileAccountCredentials(authConfig, log);
        }
        if (authConfig.type === 'service') {
            return new ProvisionedServiceAccountCredentials(authConfig, log);
        }
        const vaultclient = this.vaultclientCache.getClient('source:s3');
        return new RoleCredentials(vaultclient, 'replication', roleArn, log);
    }

    _putMetadata(entry, log, cb) {
        this.retry({
            actionDesc: 'update metadata on source',
            logFields: { entry: entry.getLogInfo() },
            actionFunc: done => this._putMetadataOnce(entry, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _setupSourceRole(entry, log) {
        log.debug('getting bucket replication', { entry: entry.getLogInfo() });
        const entryRolesString = entry.getReplicationRoles();
        let errMessage;
        let entryRoles;
        if (entryRolesString !== undefined) {
            entryRoles = entryRolesString.split(',');
        }
        if (entryRoles === undefined ||
            (entryRoles.length !== 1 && entryRoles.length !== 2)) {
            errMessage =
                'expecting one or two roles in bucket replication ' +
                'configuration';
            log.error(errMessage, {
                method: 'UpdateReplicationStatus._setupSourceRole',
                entry: entry.getLogInfo(),
                roles: entryRolesString,
            });
            return errors.BadRole.customizeDescription(errMessage);
        }
        this.sourceRole = entryRoles[0];

        this._setupSourceClient(this.sourceRole, log);

        return null;
    }

    _putMetadataOnce(entry, log, cb) {
        log.debug('putting metadata',
                  { where: 'source', entry: entry.getLogInfo(),
                    replicationStatus: entry.getReplicationStatus() });

        // sends extra header x-scal-replication-content to the target
        // if it's a metadata operation only
        const mdBlob = entry.getSerialized();
        const req = this.backbeatSource.putMetadata({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            ContentLength: Buffer.byteLength(mdBlob),
            Body: mdBlob,
        });
        attachReqUids(req, log);
        req.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                if (err.ObjNotFound || err.code === 'ObjNotFound') {
                    return cb(err);
                }
                log.error('an error occurred when putting metadata to S3',
                    { method: 'UpdateReplicationStatus._putMetadataOnce',
                      entry: entry.getLogInfo(),
                      origin: 'source',
                      peer: this.sourceConfig.s3,
                      error: err.message });
                return cb(err);
            }
            return cb(null, data);
        });
    }

    _setupSourceClient(sourceRole, log) {
        this.s3sourceCredentials =
            this._createCredentials(this.sourceConfig.auth, sourceRole, log);

        // Disable retries, use our own retry policy
        const sourceS3 = this.sourceConfig.s3;
        this.backbeatSource = new BackbeatClient({
            endpoint: `${this.sourceConfig.transport}://` +
                `${sourceS3.host}:${sourceS3.port}`,
            credentials: this.s3sourceCredentials,
            sslEnabled: this.sourceConfig.transport === 'https',
            httpOptions: { agent: this.sourceHTTPAgent, timeout: 0 },
            maxRetries: 0,
        });
    }

    processQueueEntry(sourceEntry, done) {
        const log = this.logger.newRequestLogger();

        log.debug('updating replication status for entry',
                  { entry: sourceEntry.getLogInfo() });

        const err = this._setupSourceRole(sourceEntry, log);
        if (err) {
            return setImmediate(() => done(err));
        }
        return this._updateReplicationStatus(sourceEntry, log, done);
    }

    _getMetadata(entry, log, cb) {
        this.retry({
            actionDesc: 'get metadata from source',
            logFields: { entry: entry.getLogInfo() },
            actionFunc: done => this._getMetadataOnce(entry, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _getMetadataOnce(entry, log, cb) {
        log.debug('getting metadata', {
            where: 'source',
            entry: entry.getLogInfo(),
            method: 'ReplicateObject._getMetadataOnce',
        });
        const cbOnce = jsutil.once(cb);

        const req = this.backbeatSource.getMetadata({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            VersionId: entry.getEncodedVersionId(),
        });
        attachReqUids(req, log);
        req.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                if (err.ObjNotFound || err.code === 'ObjNotFound') {
                    return cbOnce(err);
                }
                log.error('an error occurred when getting metadata from S3', {
                    method: 'ReplicateObject._getMetadataOnce',
                    entry: entry.getLogInfo(),
                    origin: 'source',
                    error: err,
                    errMsg: err.message,
                    errCode: err.code,
                    errStack: err.stack,
                });
                return cbOnce(err);
            }
            return cbOnce(null, data);
        });
    }

    _refreshSourceEntry(sourceEntry, log, cb) {
        this._getMetadata(sourceEntry, log, (err, blob) => {
            if (err) {
                log.error('error getting metadata blob from S3', {
                    method: 'ReplicateObject._refreshSourceEntry',
                    error: err,
                });
                return cb(err);
            }
            const parsedEntry = ObjectQueueEntry.createFromBlob(blob.Body);
            if (parsedEntry.error) {
                log.error('error parsing metadata blob', {
                    error: parsedEntry.error,
                    method: 'ReplicateObject._refreshSourceEntry',
                });
                return cb(errors.InternalError.
                    customizeDescription('error parsing metadata blob'));
            }
            const refreshedEntry = new ObjectQueueEntry(sourceEntry.getBucket(),
                sourceEntry.getObjectVersionedKey(), parsedEntry.result);
            return cb(null, refreshedEntry);
        });
    }

    _updateReplicationStatus(sourceEntry, log, done) {
        return this._refreshSourceEntry(sourceEntry, log,
        (err, refreshedEntry) => {
            if (err) {
                return done(err);
            }
            const site = sourceEntry.getSite();
            const status = sourceEntry.getReplicationSiteStatus(site);
            let updatedSourceEntry;
            if (status === 'COMPLETED') {
                updatedSourceEntry = refreshedEntry.toCompletedEntry(site);
            } else if (status === 'FAILED') {
                updatedSourceEntry = refreshedEntry.toFailedEntry(site);
            } else if (status === 'PENDING') {
                updatedSourceEntry = refreshedEntry.toPendingEntry(site);
            } else {
                const msg = `unknown status in replication info: ${status}`;
                return done(errors.InternalError.customizeDescription(msg));
            }
            updatedSourceEntry.setSite(site);
            updatedSourceEntry.setReplicationSiteDataStoreVersionId(site,
                sourceEntry.getReplicationSiteDataStoreVersionId(site));
            return this._putMetadata(updatedSourceEntry, log, err => {
                if (err) {
                    log.error('an error occurred when writing replication ' +
                              'status',
                        { entry: updatedSourceEntry.getLogInfo(),
                          origin: 'source',
                          peer: this.sourceConfig.s3,
                          replicationStatus:
                            updatedSourceEntry.getReplicationStatus(),
                          error: err.message });
                    return done(err);
                }
                log.end().info('replication status updated', {
                    entry: updatedSourceEntry.getLogInfo(),
                    replicationStatus:
                        updatedSourceEntry.getReplicationStatus(),
                });
                return done();
            });
        });
    }
}

module.exports = UpdateReplicationStatus;
