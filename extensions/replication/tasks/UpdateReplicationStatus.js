const errors = require('arsenal').errors;

const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const attachReqUids = require('../utils/attachReqUids');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const AccountCredentials =
          require('../../../lib/credentials/AccountCredentials');
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
            return new AccountCredentials(authConfig, log);
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

    _updateReplicationStatus(sourceEntry, log, done) {
        return this._putMetadata(sourceEntry, log, err => {
            if (err) {
                log.error('an error occurred when writing replication ' +
                          'status',
                    { entry: sourceEntry.getLogInfo(),
                      origin: 'source',
                      peer: this.sourceConfig.s3,
                      replicationStatus: sourceEntry.getReplicationStatus(),
                      error: err.message });
                return done(err);
            }
            log.end().info('replication status updated',
                { entry: sourceEntry.getLogInfo(),
                  replicationStatus: sourceEntry.getReplicationStatus() });
            return done();
        });
    }
}

module.exports = UpdateReplicationStatus;
