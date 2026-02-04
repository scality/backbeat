const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const { isRetryableMiddleware, TIMEOUT_MS } = require('./clients/utils');
const VaultClientCache = require('./clients/VaultClientCache');
const BackbeatTask = require('./tasks/BackbeatTask');
const RoleCredentials = require('./credentials/RoleCredentials');
const { getAccountCredentials } = require('./credentials/AccountCredentials');
const { http: HttpAgent } = require('httpagent');
const { lifecycleListing: { NON_CURRENT_TYPE, CURRENT_TYPE, ORPHAN_DM_TYPE } } = require('./constants');
const { 
    CloudserverClient,
    GetBucketIndexesCommand,
    PutBucketIndexesCommand,
    DeleteBucketIndexesCommand,
    ListLifecycleCurrentsCommand,
    ListLifecycleNonCurrentsCommand,
    ListLifecycleOrphansCommand,
    GetMetadataCommand,
    MultipleBackendHeadObjectCommand,
    PutMetadataCommand
} = require('@scality/cloudserverclient');

class BackbeatMetadataProxy extends BackbeatTask {
    constructor(s3Endpoint, s3Auth, sourceHTTPAgent) {
        super();
        this._s3Endpoint = s3Endpoint;
        this._s3Auth = s3Auth;
        // TODO: For SSL support, create HTTPS agents instead.
        this._sourceHTTPAgent = sourceHTTPAgent ||
            new HttpAgent.Agent({ keepAlive: true });
        this._setupVaultclientCache();
    }

    _setupVaultclientCache() {
        this.vaultclientCache = new VaultClientCache();
        if (this._s3Auth.type === 'role') {
            const { host, port } = this._s3Auth.vault;
            this.vaultclientCache
                .setHost('source:s3', host)
                .setPort('source:s3', port);
        }
    }

    _createCredentials(log) {
        const accountCredentials = getAccountCredentials(this._s3Auth, log);
        if (accountCredentials) {
            return accountCredentials;
        }
        const vaultclient = this.vaultclientCache.getClient('source:s3');
        const role = this.sourceRole;
        return new RoleCredentials(vaultclient, 'replication', role, log);
    }

    /**
     * Write raw object metadata blob in JSON to MongoDB
     *
     * @param {object} params - params object
     * @param {string} params.bucket - bucket name
     * @param {string} params.objectKey - object key
     * @param {string} [params.versionId] - encoded version ID
     * @param {Buffer} params.mdBlob - raw metadata blob
     * @param {Logger} log - logger object
     * @param {function} cb - callback: cb(error, { versionId })
     * @return {undefined}
     */
    putMetadata(params, log, cb) {
        this.retry({
            actionDesc: 'update metadata on source',
            logFields: { bucket: params.bucket,
                         objectKey: params.objectKey,
                         versionId: params.versionId },
            actionFunc: done =>
                this._putMetadataOnce(params, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _putMetadataOnce(params, log, cb) {
        const { bucket, objectKey, versionId, mdBlob } = params;
        log.debug('putting metadata', {
            where: 'source',
            bucket, objectKey, versionId,
        });

        // sends extra header x-scal-replication-content to the target
        // if it's a metadata operation only
        const command = new PutMetadataCommand({
            Bucket: bucket,
            Key: objectKey,
            VersionId: versionId,
            Body: mdBlob,
            RequestUids: log.getSerializedUids(),
        });

        return this.backbeatSource.send(command)
            .then(data => {
                log.debug('PutMetadata returned with payload', {
                    method: 'BackbeatMetadataProxy._putMetadataOnce',
                    bucket, objectKey, versionId,
                    endpoint: this._s3Endpoint,
                    payload: data,
                });
                return cb(null, data);
            })
            .catch(err => {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                if (err.ObjNotFound || err.name === 'ObjNotFound') {
                    return cb(err);
                }
                log.error('an error occurred when putting metadata to S3',
                    { method: 'BackbeatMetadataProxy._putMetadataOnce',
                      bucket, objectKey, versionId,
                      origin: 'source',
                      endpoint: this._s3Endpoint,
                      error: err.message });
                return cb(err);
            });
    }

    headLocation(params, log, cb) {
        this.retry({
            actionDesc: 'head object location from source',
            logFields: {
                bucket: params.bucket,
                objectKey: params.objectKey
            },
            actionFunc: done => this._headLocationOnce(params, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _headLocationOnce(params, log, cb) {
        const { bucket, objectKey, locations } = params;
        log.debug('heading object location', {
            where: 'source',
            bucket, objectKey, locations,
            method: 'BackbeatMetadataProxy._headLocationOnce',
        });
        const cbOnce = jsutil.once(cb);
        const command = new MultipleBackendHeadObjectCommand({
            Bucket: bucket,
            Key: objectKey,
            Locations: JSON.stringify([{
                dataStoreName: locations[0].dataStoreName,
                key: locations[0].key,
            }]),
            RequestUids: log.getSerializedUids(),
        });

        return this.backbeatSource.send(command)
            .then(data => cbOnce(null, data))
            .catch(err => {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                if (err.ObjNotFound || err.name === 'ObjNotFound') {
                    return cbOnce(err);
                }
                log.error(
                    'an error occurred during head object location request', {
                    method: 'BackbeatMetadataProxy._headLocationOnce',
                    origin: 'source',
                    endpoint: this._s3Endpoint,
                    error: err,
                    errMsg: err.message,
                    errCode: err.name,
                    errStack: err.stack,
                });
                return cbOnce(err);
            });
    }

    /**
     * Retrieve raw object metadata in JSON from MongoDB
     *
     * @param {object} params - params object
     * @param {string} params.bucket - bucket name
     * @param {string} params.objectKey - object key
     * @param {string} [params.versionId] - encoded version ID
     * @param {Logger} log - logger object
     * @param {function} cb - callback: cb(error, { Body: mdBlob })
     * @return {undefined}
     */
    getMetadata(params, log, cb) {
        this.retry({
            actionDesc: 'get metadata from source',
            logFields: { bucket: params.bucket,
                         objectKey: params.objectKey,
                         versionId: params.versionId },
            actionFunc: done => this._getMetadataOnce(params, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _getMetadataOnce(params, log, cb) {
        const { bucket, objectKey, versionId } = params;
        log.debug('getting metadata', {
            where: 'source',
            bucket, objectKey, versionId,
            method: 'BackbeatMetadataProxy._getMetadataOnce',
        });

        const cbOnce = jsutil.once(cb);

        const command = new GetMetadataCommand({
            Bucket: bucket,
            Key: objectKey,
            VersionId: versionId,
            RequestUids: log.getSerializedUids(),
        });

    return this.backbeatSource.send(command)
            .then(data => cbOnce(null, data))
            .catch(err => {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                // <!> Only in S3C <!> Backbeat API returns 'InvalidBucketState' error if the bucket is not versioned.
                // In this case, instead of logging an error, it should be logged as a debug message,
                // to avoid causing unnecessary concern to the customer.
                // TODO: BB-612
                if (err.ObjNotFound || err.name === 'ObjNotFound' || err.name === 'InvalidBucketState') {
                    return cbOnce(err);
                }
                log.error('an error occurred when getting metadata from S3', {
                    method: 'BackbeatMetadataProxy._getMetadataOnce',
                    bucket, objectKey, versionId,
                    origin: 'source',
                    endpoint: this._s3Endpoint,
                    error: err,
                    errMsg: err.message,
                    errCode: err.name,
                    errStack: err.stack,
                });
                return cbOnce(err);
            });
    }

    listLifecycle(listType, params, log, cb) {
        if (listType === CURRENT_TYPE) {
            const command = new ListLifecycleCurrentsCommand(params);
            return this.backbeatSource.send(command)
                .then(data => cb(null, data.Contents, data.IsTruncated, {
                    marker: data.NextMarker,
                }))
                .catch(err => cb(err));
        }

        if (listType === NON_CURRENT_TYPE) {
            const command = new ListLifecycleNonCurrentsCommand(params);
            return this.backbeatSource.send(command)
                .then(data => cb(null, data.Contents, data.IsTruncated, {
                    keyMarker: data.NextKeyMarker,
                    versionIdMarker: data.NextVersionIdMarker,
                }))
                .catch(err => cb(err));
        }

        if (listType === ORPHAN_DM_TYPE) {
            const command = new ListLifecycleOrphansCommand(params);
            return this.backbeatSource.send(command)
                .then(data => cb(null, data.Contents, data.IsTruncated, {
                    marker: data.NextMarker,
                }))
                .catch(err => cb(err));
        }

        log.error('invalid listType', {
            method: 'LifecycleTaskV2._listLifecycle',
            params,
            listType,
        });

        return cb(errors.InternalError.customizeDescription('invalid listing type'));
    }

    getBucketIndexes(bucket, log, cb) {
        const command = new GetBucketIndexesCommand({
            Bucket: bucket,
        });
        
        return this.backbeatSource.send(command)
            .then(res => cb(null, res.Indexes))
            .catch(err => cb(err));
    }

    putBucketIndexes(bucket, indexes, log, cb) {
        const command = new PutBucketIndexesCommand({
            Bucket: bucket,
            Body: JSON.stringify(indexes),
        });
        
        return this.backbeatSource.send(command)
            .then(() => cb(null))
            .catch(err => cb(err));
    }

    deleteBucketIndexes(bucket, indexes, log, cb) {
        const command = new DeleteBucketIndexesCommand({
            Bucket: bucket,
            Body: JSON.stringify(indexes),
        });
        
        return this.backbeatSource.send(command)
            .then(() => cb(null))
            .catch(err => cb(err));
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
        return this;
    }

    setSourceClient(log) {
        const requestHandler = {
            [this._s3Endpoint.startsWith('https:') ? 'httpsAgent' : 'httpAgent']: this._sourceHTTPAgent,
            requestTimeout: TIMEOUT_MS,
            connectionTimeout: TIMEOUT_MS
        };
        const creds = this._createCredentials(log);
        this.backbeatSource = new CloudserverClient({
            endpoint: this._s3Endpoint,
            credentials: creds.getCredentialsProvider(),
            region: 'us-east-1',
            maxAttempts: 1, // Disable retries, use our own retry policy
            requestHandler,
        });
        this.backbeatSource.middlewareStack.add(isRetryableMiddleware(), {
            step: 'deserialize',
            priority: 'high',
        });
        return this;
    }

    setBackbeatClient(client) {
        this.backbeatSource = client;
        return this;
    }
}

module.exports = BackbeatMetadataProxy;
