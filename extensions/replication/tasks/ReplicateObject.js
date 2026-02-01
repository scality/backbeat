const async = require('async');
const { S3Client, GetBucketReplicationCommand, GetObjectCommand } = require('@aws-sdk/client-s3');

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const ObjectMDLocation = require('arsenal').models.ObjectMDLocation;

const ClientManager = require('../../../lib/clients/ClientManager');
const BackbeatMetadataProxy = require('../../../lib/BackbeatMetadataProxy');
const { 
    BackbeatRoutesClient,
    PutDataCommand,
    BatchDeleteCommand,
    PutMetadataCommand,
    GetMetadataCommand,
    addContentLengthMiddleware,
    attachReqUids,
} = require('@scality/cloudserverclient');

const mapLimitWaitPendingIfError = require('../../../lib/util/mapLimitWaitPendingIfError');
const { isRetryableMiddleware, TIMEOUT_MS } = require('../../../lib/clients/utils');
const { isAccessDeniedError, getAccessDeniedLogFields } = require('../../../lib/util/replicationPermissionError');
const getExtMetrics = require('../utils/getExtMetrics');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { getAccountCredentials } = require('../../../lib/credentials/AccountCredentials');
const RoleCredentials = require('../../../lib/credentials/RoleCredentials');
const { metricsExtension, metricsTypeQueued, metricsTypeCompleted, replicationStages } = require('../constants');

const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const { authTypeAssumeRole } = require('../../../lib/constants');

const errorAlreadyCompleted = {};

function _extractAccountIdFromRole(role) {
    return role.split(':')[4];
}

function _extractRoleNameFromRole(role) {
    return role.split(':role/')[1];
}

// BACKBEAT_INJECT_REPLICATION_ERROR_RATE variable can be set to randomly introduce errors.
// When set, the value is the target percentage of errors.
const BACKBEAT_INJECT_REPLICATION_ERROR_RATE =
    process.env.BACKBEAT_INJECT_REPLICATION_ERROR_RATE / 100;

class ReplicateObject extends BackbeatTask {
    /**
     * Process a single replication entry
     *
     * @constructor
     * @param {QueueProcessor} qp - queue processor instance
     */
    constructor(qp) {
        const qpState = qp.getStateVars();
        const { repConfig, destConfig } = qpState;

        let retryParams = repConfig.queueProcessor.retry.scality;

        if (destConfig?.replicationEndpoint) {
            if (repConfig.queueProcessor.retry[destConfig.replicationEndpoint.type]) {
                retryParams = repConfig.queueProcessor.retry[destConfig.replicationEndpoint.type];
            }
        }

        super(retryParams);

        Object.assign(this, qpState);

        this.sourceRole = null;
        this.targetRole = null;
        this.destBackbeatHost = null;
        this.s3sourceCredentials = null;
        this.s3destCredentials = null;
        this.S3source = null;
        this.backbeatSource = null;
        this.backbeatSourceProxy = null;
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

        // Destination Vault admin API is not accessible when
        // using assumeRole i.e when targeting an Zenko
        // We delegate this task to the destination's Cloudserver
        if (this.destConfig.auth.type === authTypeAssumeRole) {
            return process.nextTick(cb);
        }

        return this.retry({
            actionDesc: 'lookup target account attributes',
            logFields: { entry: destEntry.getLogInfo() },
            actionFunc: done => this._setTargetAccountMdOnce(
                destEntry, targetRole, log, done),
            // this call uses our own Vault client which does not set
            // the 'retryable' field
            shouldRetryFunc: err =>
            (err.InternalError || err.name === 'InternalError' ||
                err.ServiceUnavailable || err.name === 'ServiceUnavailable'),
            onRetryFunc: () => {
                this.destHosts.pickNextHost();
                this._setupDestClients(this.targetRole, log);
            },
            log,
        }, cb);
    }

    _getAndPutPart(sourceEntry, destEntry, part, log, cb) {
        const partLogger = this.logger.newRequestLogger(log.getUids());
        this.retry({
            actionDesc: 'stream part data',
            logFields: { entry: sourceEntry.getLogInfo(), part },
            actionFunc: done => this._getAndPutPartOnce(
                sourceEntry, destEntry, part, partLogger, done),
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

    _getUpdatedSourceEntry(params) {
        const { sourceEntry, replicationStatus } = params;
        const entry = replicationStatus === 'COMPLETED' ?
              sourceEntry.toCompletedEntry(this.site) :
              sourceEntry.toFailedEntry(this.site);
        const versionId =
              sourceEntry.getReplicationSiteDataStoreVersionId(this.site);
        return entry.setReplicationSiteDataStoreVersionId(this.site,
            versionId);
    }

    _publishReplicationStatus(sourceEntry, replicationStatus, params) {
        const { log, reason, kafkaEntry } = params;
        const entryParams = { sourceEntry, replicationStatus };
        const updatedSourceEntry = this._getUpdatedSourceEntry(entryParams);
        const updateData = sourceEntry.getReplicationContent().includes('DATA');
        const kafkaEntries = [updatedSourceEntry.toKafkaEntry(this.site)];
        this.replicationStatusProducer.send(kafkaEntries, err => {
            if (err) {
                log.error('error in entry delivery to replication status topic', {
                    method: 'ReplicateObject._publishReplicationStatus',
                    topic: this.repConfig.replicationStatusTopic,
                    entry: updatedSourceEntry.getLogInfo(),
                    replicationStatus,
                    error: err,
                });
            } else {
                log.info('replication status published', {
                    topic: this.repConfig.replicationStatusTopic,
                    entry: updatedSourceEntry.getLogInfo(),
                    replicationStatus,
                    reason,
                });
                this.metricsHandler.metadataReplicationStatus({ replicationStatus, location: this.site });
                if (updateData) {
                    this.metricsHandler.dataReplicationStatus({ replicationStatus, location: this.site });
                }
            }
            // Commit whether there was an error or not to allow
            // progress of the consumer, as best effort measure when
            // there are errors. We can count on the sweeper to retry
            // entries that failed to be published to kafka (because
            // they will keep their PENDING status).
            if (this.consumer) {
                this.consumer.onEntryCommittable(kafkaEntry);
            }
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
                {
                    method: 'ReplicateObject._setupRolesOnce',
                    entry: entry.getLogInfo(),
                    roles: entryRolesString,
                });
            return cb(errors.BadRole);
        }
        this.sourceRole = entryRoles[0];
        this.targetRole = entryRoles[1];

        this._setupSourceClients(this.sourceRole, log);

        const command = new GetBucketReplicationCommand(
            { Bucket: entry.getBucket() });
        attachReqUids(command, log.getSerializedUids());
        return this.S3source.send(command)
            .then(data => {
                const replicationEnabled = (
                data.ReplicationConfiguration.Rules.some(
                    rule => entry.getObjectKey().startsWith(rule.Prefix)
                        && rule.Status === 'Enabled'));
            if (!replicationEnabled) {
                log.debug('replication disabled for object',
                    {
                        method: 'ReplicateObject._setupRolesOnce',
                        entry: entry.getLogInfo(),
                    });
                return cb(errors.PreconditionFailed.customizeDescription(
                    'replication disabled for object'));
            }
            const roles = data.ReplicationConfiguration.Role.split(',');
            if (roles.length !== 2) {
                log.error('expecting two roles separated by a ' +
                    'comma in bucket replication configuration',
                    {
                        method: 'ReplicateObject._setupRolesOnce',
                        entry: entry.getLogInfo(),
                        roles,
                    });
                return cb(errors.BadRole);
            }
            if (roles[0] !== entryRoles[0]) {
                log.error('role in replication entry for source does ' +
                    'not match role in bucket replication configuration ',
                    {
                        method: 'ReplicateObject._setupRolesOnce',
                        entry: entry.getLogInfo(),
                        entryRole: entryRoles[0],
                        bucketRole: roles[0],
                    });
                return cb(errors.BadRole);
            }
            if (roles[1] !== entryRoles[1]) {
                log.error('role in replication entry for target does ' +
                    'not match role in bucket replication configuration ',
                    {
                        method: 'ReplicateObject._setupRolesOnce',
                        entry: entry.getLogInfo(),
                        entryRole: entryRoles[1],
                        bucketRole: roles[1],
                    });
                return cb(errors.BadRole);
            }
            return cb(null, roles[0], roles[1]);
        })
        .catch(err => {
            // eslint-disable-next-line no-param-reassign
            err.origin = 'source';
            log.error('error getting replication ' +
                'configuration from S3',
                {
                    method: 'ReplicateObject._setupRolesOnce',
                    entry: entry.getLogInfo(),
                    origin: 'source',
                    peer: this.sourceConfig.s3,
                    error: err.message,
                    err,
                    httpStatus: err.$metadata?.httpStatusCode,
                });
            return cb(err);
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
                    let peer;
                    if (this.destConfig.auth.type === 'role') {
                        peer = this.destBackbeatHost;
                        if (this.destConfig.auth.vault) {
                            const { host, port } = this.destConfig.auth.vault;
                            if (host) {
                                // no proxy is used, log the vault host/port
                                peer = { host, port };
                            }
                        }
                    }
                    log.error('an error occurred when looking up target ' +
                        'account attributes',
                        {
                            method: 'ReplicateObject._setTargetAccountMdOnce',
                            entry: destEntry.getLogInfo(),
                            origin: 'target',
                            peer,
                            error: err.message,
                            err,
                        });
                    return cb(err);
                }
                log.debug('setting owner info in target metadata',
                    {
                        entry: destEntry.getLogInfo(),
                        accountAttr,
                    });
                destEntry.setOwnerId(accountAttr.canonicalID);
                destEntry.setOwnerDisplayName(accountAttr.displayName);
                return cb();
            });
    }

    _refreshSourceEntry(sourceEntry, log, cb) {
        const params = {
            Bucket: sourceEntry.getBucket(),
            Key: sourceEntry.getObjectKey(),
            VersionId: sourceEntry.getEncodedVersionId(),
            RequestUids: log.getSerializedUids(),
        };
        return this.backbeatSource.send(new GetMetadataCommand(params))
            .then(data => {
                const parsedEntry = ObjectQueueEntry.createFromBlob(data.Body);
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
            })
            .catch(err => {
                err.origin = 'source'; // eslint-disable-line no-param-reassign
                const logFields = {
                    method: 'ReplicateObject._refreshSourceEntry',
                    error: err,
                };
                if (isAccessDeniedError(err)) {
                    Object.assign(logFields, getAccessDeniedLogFields(
                        sourceEntry.getBucket(), this.sourceRole));
                }
                log.error('error getting metadata blob from S3', logFields);
                return cb(err);
            });
    }

    _getAndPutData(sourceEntry, destEntry, log, cb) {
        log.debug('replicating data', { entry: sourceEntry.getLogInfo() });
        if (sourceEntry.getLocation().some(part => {
            const partObj = new ObjectMDLocation(part);
            return partObj.getDataStoreETag() === undefined;
        })) {
            const errMessage =
                  'cannot replicate object without dataStoreETag property';
            log.error(errMessage, {
                method: 'ReplicateObject._getAndPutData',
                entry: sourceEntry.getLogInfo(),
            });
            return cb(errors.InternalError.customizeDescription(errMessage));
        }
        // For Replication Replay testing, set the BACKBEAT_INJECT_REPLICATION_ERROR_RATE variable
        if (BACKBEAT_INJECT_REPLICATION_ERROR_RATE) {
            if (Math.random() < BACKBEAT_INJECT_REPLICATION_ERROR_RATE) {
                return process.nextTick(() => cb(new Error('Replication error')));
            }
        }
        const locations = sourceEntry.getReducedLocations();
        const mpuConcLimit = this.repConfig.queueProcessor.mpuPartsConcurrency;
        return mapLimitWaitPendingIfError(locations, mpuConcLimit, (part, done) => {
            this._getAndPutPart(sourceEntry, destEntry, part, log, done);
        }, (err, destLocations) => {
            if (err) {
                return this._deleteOrphans(destEntry, destLocations, log, () => cb(err));
            }
            return cb(null, destLocations);
        });
    }

    _publishReadMetrics(size, readStartTime) {
        const serviceName = this.serviceName;
        this.metricsHandler.timeElapsed({
            serviceName,
            location: this.site,
            replicationStage: replicationStages.sourceDataRead,
        }, Date.now() - readStartTime);
        this.metricsHandler.sourceDataBytes({ serviceName, location: this.site }, size);
        this.metricsHandler.reads({ serviceName, location: this.site });
    }

    _publishDataWriteMetrics(size, sourceEntry, writeStartTime) {
        const serviceName = this.serviceName;
        this.metricsHandler.timeElapsed({
            serviceName,
            location: this.site,
            replicationStage: replicationStages.destinationDataWrite,
        }, Date.now() - writeStartTime);
        this.metricsHandler.dataReplicationBytes({ serviceName, location: this.site }, size);
        this.metricsHandler.writes({
            serviceName,
            location: this.site,
            replicationContent: 'data',
        });
        const extMetrics = getExtMetrics(this.site, size, sourceEntry);
        this.mProducer.publishMetrics(extMetrics,
            metricsTypeCompleted, metricsExtension, () => {});
    }

    _publishMetadataWriteMetrics(buffer, writeStartTime) {
        const serviceName = this.serviceName;
        this.metricsHandler.timeElapsed({
            serviceName,
            location: this.site,
            replicationStage: replicationStages.destinationMetadataWrite,
        }, Date.now() - writeStartTime);
        this.metricsHandler.metadataReplicationBytes({
            serviceName,
            location: this.site,
        }, Buffer.byteLength(buffer));
        this.metricsHandler.writes({
            serviceName,
            location: this.site,
            replicationContent: 'metadata',
        });
    }

    _getAndPutPartOnce(sourceEntry, destEntry, part, log, done) {
        const doneOnce = jsutil.once(done);
        const partObj = new ObjectMDLocation(part);
        const partNumber = partObj.getPartNumber();
        const partSize = partObj.getPartSize();
        
        const abortController = new AbortController();
        let sourceStreamAborted = false;
        let destRequestAborted = false;
        
        const command = new GetObjectCommand({
            Bucket: sourceEntry.getBucket(),
            Key: sourceEntry.getObjectKey(),
            VersionId: sourceEntry.getEncodedVersionId(),
            PartNumber: partNumber,
        });
        attachReqUids(command, log.getSerializedUids());
        const readStartTime = Date.now();
        
        this.S3source.send(command, { abortSignal: abortController.signal })
            .then(response => {
                const incomingMsg = response.Body;
                incomingMsg.on('error', err => {
                    if (!sourceStreamAborted && !destRequestAborted) {
                        abortController.abort();
                        destRequestAborted = true;
                    }
                    if (err.$metadata?.httpStatusCode === 404) {
                        return doneOnce(errors.ObjNotFound);
                    }
                    if (!sourceStreamAborted) {
                        // eslint-disable-next-line no-param-reassign
                        err.origin = 'source';
                        // eslint-disable-next-line no-param-reassign
                        err.retryable = true;
                        log.error('an error occurred when streaming data from S3',
                            {
                                method: 'ReplicateObject._getAndPutPartOnce',
                                entry: destEntry.getLogInfo(),
                                part,
                                origin: 'source',
                                peer: this.sourceConfig.s3,
                                error: err.message,
                                err,
                            });
                    }
                    return doneOnce(err);
                });
                
                incomingMsg.on('end', () => {
                    this._publishReadMetrics(partSize, readStartTime);
                });
                
                log.debug('putting data', { entry: destEntry.getLogInfo(), part });
                const putCommand = new PutDataCommand({
                    Bucket: destEntry.getBucket(),
                    Key: destEntry.getObjectKey(),
                    CanonicalID: destEntry.getOwnerId(),
                    ContentMD5: partObj.getPartETag(),
                    Body: incomingMsg,
                    // destination bucket has to be versioning enabled.
                    VersioningRequired: true,
                    RequestUids: log.getSerializedUids(),
                });
                addContentLengthMiddleware(
                    putCommand,
                    response.ContentLength,
                );
                const writeStartTime = Date.now();
                return this.backbeatDest.send(putCommand, { abortSignal: abortController.signal })
                    .then(data => {
                        partObj.setDataLocation(data.Location[0]);

                        // Set encryption parameters that were used to encrypt the
                        // target data in the object metadata, or reset them if
                        // there was no encryption
                        const { ServerSideEncryption, SSECustomerAlgorithm, SSEKMSKeyId } = data;
                        destEntry.setAmzServerSideEncryption(ServerSideEncryption || '');
                        destEntry.setAmzEncryptionCustomerAlgorithm(SSECustomerAlgorithm || '');
                        destEntry.setAmzEncryptionKeyId(SSEKMSKeyId || '');

                        this._publishDataWriteMetrics(partSize, sourceEntry, writeStartTime);
                        return doneOnce(null, partObj.getValue());
                    })
                    .catch(err => {
                        if (!destRequestAborted) {
                            // Abort the source stream
                            abortController.abort();
                            sourceStreamAborted = true;
                            if (incomingMsg.destroy) {
                                incomingMsg.destroy();
                            }
                        }
                        // eslint-disable-next-line no-param-reassign
                        err.origin = 'target';
                        log.error('an error occurred on putData to S3',
                            {
                                method: 'ReplicateObject._getAndPutPartOnce',
                                entry: destEntry.getLogInfo(),
                                part,
                                origin: 'target',
                                peer: this.destBackbeatHost,
                                error: err.message,
                                httpStatus: err.$metadata?.httpStatusCode,
                                err,
                            });
                        return doneOnce(err);
                    });
            })
            .catch(err => {
                if (!sourceStreamAborted) {
                    // Abort controller in case the destination request is still pending
                    abortController.abort();
                    destRequestAborted = true;
                }
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                if (err.$metadata?.httpStatusCode === 404) {
                    return doneOnce(err);
                }
                log.error('an error occurred on getObject from S3',
                    {
                        method: 'ReplicateObject._getAndPutPartOnce',
                        entry: sourceEntry.getLogInfo(),
                        part,
                        origin: 'source',
                        peer: this.sourceConfig.s3,
                        error: err.message,
                        err,
                        httpStatus: err.$metadata?.httpStatusCode,
                    });
                return doneOnce(err);
            });
    }

    _putMetadataOnce(entry, mdOnly, log, cb) {
        log.debug('putting metadata', {
            where: 'target', entry: entry.getLogInfo(),
            replicationStatus: entry.getReplicationSiteStatus(this.site),
        });
        const cbOnce = jsutil.once(cb);

        // accountid is only needed when using assumeRole auth
        // to delegate the task of updating metadata with
        // the target account info to the destination's Cloudserver
        let accountId = undefined;
        if (this.destConfig.auth.type === authTypeAssumeRole) {
            accountId = _extractAccountIdFromRole(this.targetRole);
        }

        // sends extra header x-scal-replication-content to the target
        // if it's a metadata operation only
        const replicationContent = (mdOnly ? 'METADATA' : undefined);
        const mdBlob = entry.getSerialized();
        const command = new PutMetadataCommand({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            VersionId: entry.getEncodedVersionId(),
            AccountId: accountId,
            Body: mdBlob,
            ReplicationContent: replicationContent,
            // destination bucket has to be versioning enabled.
            VersioningRequired: true,
            RequestUids: log.getSerializedUids(),
        });
        const writeStartTime = Date.now();
        return this.backbeatDest.send(command)
            .then(data => {
                this._publishMetadataWriteMetrics(mdBlob, writeStartTime);
                return cbOnce(null, data);
            })
            .catch(err => {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'target';
                if (err.ObjNotFound || err.name === 'ObjNotFound') {
                    return cbOnce(err);
                }
                log.error('an error occurred when putting metadata to S3',
                    {
                        method: 'ReplicateObject._putMetadataOnce',
                        entry: entry.getLogInfo(),
                        origin: 'target',
                        peer: this.destBackbeatHost,
                        error: err.message,
                        err,
                    });
                return cbOnce(err);
            });
    }

    _deleteOrphans(entry, locations, log, cb) {
        const writtenLocations = locations
            .filter(loc => loc)
            .map(loc => ({ key: loc.key, dataStoreName: loc.dataStoreName }));
        if (writtenLocations.length === 0) {
            return process.nextTick(cb);
        }
        log.info('deleting orphan data after replication failure',
            {
                method: 'ReplicateObject._deleteOrphans',
                entry: entry.getLogInfo(),
                peer: this.destBackbeatHost,
            });
        const command = new BatchDeleteCommand({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            Locations: writtenLocations,
            RequestUids: log.getSerializedUids(),
        });
        
        return this.backbeatDest.send(command)
            .then(() => cb())
            .catch(err => {
                log.error('an error occurred during batch delete of orphan data',
                    {
                        method: 'ReplicateObject._deleteOrphans',
                        entry: entry.getLogInfo(),
                        origin: 'target',
                        peer: this.destBackbeatHost,
                        error: err.message,
                        httpStatus: err.$metadata?.httpStatusCode,
                        err,
                    });
                writtenLocations.forEach(location => {
                    log.error('orphan data location was not deleted', {
                        method: 'ReplicateObject._deleteOrphans',
                        entry: entry.getLogInfo(),
                        location,
                    });
                });
                // do not return the batch delete error, only log it
                return cb();
            });
    }

    _setupSourceClients(sourceRole, log) {
        this.s3sourceCredentials =
            this._createCredentials('source', this.sourceConfig.auth,
                sourceRole, log);

        // Disable retries, use our own retry policy (mandatory for
        // putData route in order to fetch data again from source).
        const sourceS3 = this.sourceConfig.s3;
        this.S3source = new S3Client({
            endpoint: `${this.sourceConfig.transport}://` +
                `${sourceS3.host}:${sourceS3.port}`,
            credentials: this.s3sourceCredentials.getCredentialsProvider(),
            region: 'us-east-1',
            tls: this.sourceConfig.transport === 'https',
            forcePathStyle: true,
            requestHandler: {
                [this.sourceConfig.transport === 'https' ? 'httpsAgent' : 'httpAgent']: this.sourceHTTPAgent,
                connectionTimeout: 0,
            },
            maxAttempts: 1,
        });
        this.S3source.middlewareStack.add(isRetryableMiddleware(), {
            step: 'deserialize',
            priority: 'high',
        });

        const requestHandler = {
            [this.sourceConfig.transport === 'https' ? 'httpsAgent' : 'httpAgent']: this.sourceHTTPAgent,
            requestTimeout: TIMEOUT_MS,
            connectionTimeout: TIMEOUT_MS,
        };
        this.backbeatSource = new BackbeatRoutesClient({
            endpoint: `${this.sourceConfig.transport}://` +
            `${sourceS3.host}:${sourceS3.port}`,
            credentials: this.s3sourceCredentials.getCredentialsProvider(),
            region: 'us-east-1',
            maxAttempts: 1,
            requestHandler,
            disableHostPrefix: true,
            signingEscapePath: false,
        });
        this.backbeatSource.middlewareStack.add(isRetryableMiddleware(), {
            step: 'deserialize',
            priority: 'high',
        });
        this.backbeatSourceProxy = new BackbeatMetadataProxy(
            `${this.sourceConfig.transport}://` +
                `${sourceS3.host}:${sourceS3.port}`,
            this.sourceConfig.auth, this.sourceHTTPAgent);
        this.backbeatSourceProxy.setSourceRole(sourceRole);
        this.backbeatSourceProxy.setBackbeatClient(this.backbeatSource);
    }

    _setupDestClients(targetRole, log) {
        this.destBackbeatHost = this.destHosts.pickHost();

        if (this.destConfig.auth.type === authTypeAssumeRole) {
            const accountId = _extractAccountIdFromRole(targetRole);
            const roleName = _extractRoleNameFromRole(targetRole);
            this.clientManager = new ClientManager({
                id: accountId,
                authConfig: {
                    type: authTypeAssumeRole,
                    roleName,
                    sts: this.destConfig.auth.sts,
                },
                s3Config: {
                    host: this.destBackbeatHost.host,
                    port: this.destBackbeatHost.port,
                },
                transport: this.destConfig.transport,
            }, this.logger);
            this.clientManager.initSTSConfig();
            this.clientManager.initCredentialsManager();
            this.backbeatDest = this.clientManager.getBackbeatClient(accountId);
            return;
        }

        this.s3destCredentials =
            this._createCredentials('target', this.destConfig.auth,
                targetRole, log);

        const requestHandler = {
            [this.destConfig.transport === 'https' ? 'httpsAgent' : 'httpAgent']: this.destHTTPAgent,
            requestTimeout: TIMEOUT_MS,
        };
        this.backbeatDest = new BackbeatRoutesClient({
            endpoint: `${this.destConfig.transport}://` +
                `${this.destBackbeatHost.host}:${this.destBackbeatHost.port}`,
            credentials: this.s3destCredentials.getCredentialsProvider(),
            region: 'us-east-1',
            maxAttempts: 1,
            requestHandler,
        });
        this.backbeatDest.middlewareStack.add(isRetryableMiddleware(), {
            step: 'deserialize',
            priority: 'high',
        });
    }

    processQueueEntry(_sourceEntry, kafkaEntry, done) {
        let sourceEntry = _sourceEntry;
        const log = this.logger.newRequestLogger();
        const destEntry = sourceEntry.toReplicaEntry(this.site);

        log.debug('processing entry',
            { entry: sourceEntry.getLogInfo() });

        const lastModified = new Date(sourceEntry.getLastModified());
        this.metricsHandler.rpo({
            serviceName: this.serviceName,
            location: this.site,
        }, (Date.now() - lastModified) / 1000);

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
            ], err => this._handleReplicationOutcome(
                err, sourceEntry, destEntry, kafkaEntry, log, done));
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
            next => {
                if (mdOnly) {
                    return next();
                }
                const isLargeObject = sourceEntry.getContentLength() / 1000000 >=
                    this.repConfig.queueProcessor.sourceCheckIfSizeGreaterThanMB;
                const isLocationStripped = sourceEntry.getContentLength() > 0 && sourceEntry.getLocation().length === 0;
                if (!isLargeObject && !isLocationStripped) {
                    return next();
                }
                return this._refreshSourceEntry(sourceEntry, log, (err, refreshedEntry) => {
                    if (err) {
                        return next(err);
                    }
                    const status = refreshedEntry.getReplicationSiteStatus(this.site);
                    if (status === 'COMPLETED') {
                        log.info('replication already completed, skipping', {
                            entry: sourceEntry.getLogInfo(),
                        });
                        return next(errorAlreadyCompleted);
                    }
                    // Reassign sourceEntry to use fresh metadata
                    sourceEntry = refreshedEntry;
                    return next();
                });
            },
            // Get data from source bucket and put it on the target bucket
            next => {
                if (!mdOnly) {
                    const extMetrics = getExtMetrics(this.site,
                        sourceEntry.getContentLength(), sourceEntry);
                    this.mProducer.publishMetrics(extMetrics,
                        metricsTypeQueued, metricsExtension, () => {});
                    return this._getAndPutData(sourceEntry, destEntry, log,
                                               next);
                }
                return next(null, []);
            },
            // update location, replication status and put metadata in
            // target bucket
            (destLocations, next) => {
                destEntry.setLocation(destLocations);
                this._putMetadata(destEntry, mdOnly, log, err => {
                    if (err) {
                        return this._deleteOrphans(
                            destEntry, destLocations, log, () => next(err));
                    }
                    return next();
                });
            },
        ], err => this._handleReplicationOutcome(
            err, sourceEntry, destEntry, kafkaEntry, log, done));
    }

    _processQueueEntryRetryFull(sourceEntry, destEntry, kafkaEntry, log, done) {
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
        ], err => this._handleReplicationOutcome(
            err, sourceEntry, destEntry, kafkaEntry, log, done));
    }

    _handleReplicationOutcome(err, sourceEntry, destEntry, kafkaEntry,
        log, done) {
        if (!err) {
            log.debug('replication succeeded for object, publishing ' +
                'replication status as COMPLETED',
                { entry: sourceEntry.getLogInfo() });
            this._publishReplicationStatus(
                sourceEntry, 'COMPLETED', { kafkaEntry, log });
            return done(null, { committable: false });
        }
        if (err.BadRole || err.name === 'BadRole' ||
            (err.origin === 'source' &&
                (err.NoSuchEntity || err.name === 'NoSuchEntity' ||
                    err.AccessDenied || err.name === 'AccessDenied'))) {
            log.error('replication failed permanently for object, ' +
                'processing skipped',
                {
                    failMethod: err.method,
                    entry: sourceEntry.getLogInfo(),
                    origin: err.origin,
                    error: err.description,
                });
            return done();
        }
        if (err === errorAlreadyCompleted) {
            log.warn('replication skipped: ' +
                     'source object version already COMPLETED',
                     { entry: sourceEntry.getLogInfo() });
            return done();
        }
        if (err.ObjNotFound || err.name === 'ObjNotFound') {
            if (err.origin === 'source') {
                log.info('replication skipped: ' +
                    'source object version does not exist',
                    { entry: sourceEntry.getLogInfo() });
                return done();
            }
            log.info('target object version does not exist, retrying ' +
                'a full replication',
                { entry: sourceEntry.getLogInfo() });
            // TODO: Is this the right place to capture retry metrics?
            return this._processQueueEntryRetryFull(
                sourceEntry, destEntry, kafkaEntry, log, done);
        }
        if (err.InvalidObjectState || err.name === 'InvalidObjectState') {
            log.info('replication skipped: invalid object state',
                     { entry: sourceEntry.getLogInfo() });
            return done();
        }
        log.debug('replication failed permanently for object, ' +
            'publishing replication status as FAILED',
            {
                failMethod: err.method,
                entry: sourceEntry.getLogInfo(),
                error: err.description,
            });
        this._publishReplicationStatus(sourceEntry, 'FAILED', {
            log,
            reason: err.description,
            kafkaEntry,
        });
        return done(null, { committable: false });
    }
}

module.exports = ReplicateObject;
