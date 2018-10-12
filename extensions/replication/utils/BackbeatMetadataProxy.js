const http = require('http');
const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const VaultClientCache = require('../../../lib/clients/VaultClientCache');
const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { attachReqUids } = require('../../../lib/clients/utils');
const RoleCredentials = require('../../../lib/credentials/RoleCredentials');
const { getAccountCredentials } =
    require('../../../lib/credentials/AccountCredentials');

class BackbeatMetadataProxy extends BackbeatTask {
    constructor(sourceConfig, sourceHTTPAgent) {
        super();
        this.sourceConfig = sourceConfig;
        // TODO: For SSL support, create HTTPS agents instead.
        this.sourceHTTPAgent = sourceHTTPAgent ||
            new http.Agent({ keepAlive: true });
        this._setupVaultclientCache();
    }

    _setupVaultclientCache() {
        this.vaultclientCache = new VaultClientCache();
        if (this.sourceConfig.auth.type === 'role') {
            const { host, port } = this.sourceConfig.auth.vault;
            this.vaultclientCache
                .setHost('source:s3', host)
                .setPort('source:s3', port);
        }
    }

    _createCredentials(log) {
        const authConfig = this.sourceConfig.auth;
        const accountCredentials = getAccountCredentials(authConfig, log);
        if (accountCredentials) {
            return accountCredentials;
        }
        const vaultclient = this.vaultclientCache.getClient('source:s3');
        const extension = 'replication';
        const role = this.sourceRole;
        return new RoleCredentials(vaultclient, extension, role, log);
    }

    putMetadata(entry, log, cb) {
        this.retry({
            actionDesc: 'update metadata on source',
            logFields: { entry: entry.getLogInfo() },
            actionFunc: done => this._putMetadataOnce(entry, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
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
                    { method: 'BackbeatMetadataProxy._putMetadataOnce',
                      entry: entry.getLogInfo(),
                      origin: 'source',
                      peer: this.sourceConfig.s3,
                      error: err.message });
                return cb(err);
            }
            return cb(null, data);
        });
    }

    getMetadata(params, log, cb) {
        this.retry({
            actionDesc: 'get metadata from source',
            logFields: { entry: params },
            actionFunc: done => this._getMetadataOnce(params, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _getMetadataOnce(params, log, cb) {
        log.debug('getting metadata', {
            where: 'source',
            entry: params,
            method: 'BackbeatMetadataProxy._getMetadataOnce',
        });

        const cbOnce = jsutil.once(cb);

        const req = this.backbeatSource.getMetadata({
            Bucket: params.bucket,
            Key: params.objectKey,
            VersionId: params.encodedVersionId,
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
                    method: 'BackbeatMetadataProxy._getMetadataOnce',
                    entry: params.logInfo,
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

    setupSourceRole(entry, log) {
        log.debug('getting bucket replication', { entry: entry.getLogInfo() });
        const entryRolesString = entry.getReplicationRoles();
        let entryRoles;
        if (entryRolesString !== undefined) {
            entryRoles = entryRolesString.split(',');
        }
        if (entryRoles === undefined ||
            (entryRoles.length !== 1 && entryRoles.length !== 2)) {
            const errMessage = 'expecting one or two roles in bucket ' +
                'replication configuration';
            log.error(errMessage, {
                method: 'BackbeatMetadataProxy.setupSourceRole',
                entry: entry.getLogInfo(),
                roles: entryRolesString,
            });
            return { error: errors.BadRole.customizeDescription(errMessage) };
        }
        this.sourceRole = entryRoles[0];
        return this;
    }

    setSourceRole(sourceRole) {
        this.sourceRole = sourceRole;
    }

    setSourceClient(log) {
        const { transport, s3 } = this.sourceConfig;
        const { host, port } = s3;
        this.backbeatSource = new BackbeatClient({
            endpoint: `${transport}://${host}:${port}`,
            credentials: this._createCredentials(log),
            sslEnabled: transport === 'https',
            httpOptions: { agent: this.sourceHTTPAgent, timeout: 0 },
            maxRetries: 0, // Disable retries, use our own retry policy
        });
        return this;
    }
}

module.exports = BackbeatMetadataProxy;
