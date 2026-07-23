const { promisify } = require('util');
const { S3Client, GetBucketReplicationCommand, GetObjectCommand } = require('@aws-sdk/client-s3');

const { errors, versioning } = require('arsenal');
const { ObjectMDLocation, ReplicationConfiguration } = require('arsenal').models;
const {
    encode: encodeMicroVersionId,
    decode: decodeMicroVersionId,
    compare: compareMicroVersionId,
    Ordering,
} = versioning.VersionID;

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
    attachExpectContinueMiddleware,
    VersionIdCollisionException,
    StaleMicroVersionIdException,
    MicroVersionIdAlreadyStoredException,
} = require('@scality/cloudserverclient');

const runTasksWithConcurrency = require('../../../lib/util/runTasksWithConcurrency');
const { isRetryableMiddleware, TIMEOUT_MS } = require('../../../lib/clients/utils');
const { isAccessDeniedError, getAccessDeniedLogFields } = require('../../../lib/util/replicationPermissionError');
const getExtMetrics = require('../utils/getExtMetrics');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { getAccountCredentials } = require('../../../lib/credentials/AccountCredentials');
const RoleCredentials = require('../../../lib/credentials/RoleCredentials');
const { metricsExtension, metricsTypeQueued, metricsTypeCompleted, replicationStages } = require('../constants');

const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const { authTypeAssumeRole, replicationExpectContinueThreshold } = require('../../../lib/constants');

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

    async _setupRoles(entry, log) {
        return await this.retry({
            actionDesc: 'get bucket replication configuration',
            logFields: { entry: entry.getLogInfo() },
            actionFunc: done => this._setupRolesOnce(entry, log)
                .then(roles => done(null, roles), done),
            // Rely on AWS SDK notion of retryable error to decide if
            // we should set the entry replication status to FAILED
            // (non retryable) or retry later.
            shouldRetryFunc: err => err.retryable,
            log,
        });
    }

    async _setTargetAccountMd(destEntry, targetRole, log) {
        if (!this.destHosts) {
            log.warn('cannot process entry: no target site configured',
                { entry: destEntry.getLogInfo() });
            throw errors.InternalError;
        }
        this._setupDestClients(this.targetRole, log);

        // Destination Vault admin API is not accessible when
        // using assumeRole i.e when targeting an Zenko
        // We delegate this task to the destination's Cloudserver
        if (this.destConfig.auth.type === authTypeAssumeRole) {
            return;
        }

        await this.retry({
            actionDesc: 'lookup target account attributes',
            logFields: { entry: destEntry.getLogInfo() },
            actionFunc: done => this._setTargetAccountMdOnce(destEntry, targetRole, log)
                .then(() => done(), done),
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
        });
    }

    async _getAndPutPart(sourceEntry, destEntry, part, log) {
        const partLogger = this.logger.newRequestLogger(log.getUids());
        return await this.retry({
            actionDesc: 'stream part data',
            logFields: { entry: sourceEntry.getLogInfo(), part },
            actionFunc: done => this._getAndPutPartOnce(sourceEntry, destEntry, part, partLogger)
                .then(r => done(null, r), done),
            shouldRetryFunc: err => err.retryable,
            onRetryFunc: err => {
                if (err.origin === 'target') {
                    this.destHosts.pickNextHost();
                    this._setupDestClients(this.targetRole, partLogger);
                }
            },
            log: partLogger,
        });
    }

    async _putMetadata(entry, mdOnly, conflict, log) {
        return await this.retry({
            actionDesc: 'update metadata on target',
            logFields: { entry: entry.getLogInfo() },
            actionFunc: done => this._putMetadataOnce(entry, mdOnly, conflict, log)
                .then(data => done(null, data), done),
            shouldRetryFunc: err => err.retryable,
            onRetryFunc: err => {
                if (err.origin === 'target') {
                    this.destHosts.pickNextHost();
                    this._setupDestClients(this.targetRole, log);
                }
            },
            log,
        });
    }

    _getUpdatedSourceEntry(params) {
        const { sourceEntry, replicationStatus } = params;
        const backend = sourceEntry.getReplicationBackend();
        const entry = replicationStatus === 'COMPLETED' ?
              sourceEntry.toCompletedEntry(backend) :
              sourceEntry.toFailedEntry(backend);
        const versionId = sourceEntry.getReplicationSiteDataStoreVersionId(backend);
        return entry.setReplicationSiteDataStoreVersionId(backend, versionId);
    }

    _publishReplicationStatus(sourceEntry, replicationStatus, params) {
        const { log, reason, kafkaEntry } = params;
        const entryParams = { sourceEntry, replicationStatus };
        const updatedSourceEntry = this._getUpdatedSourceEntry(entryParams);
        const updateData = sourceEntry.getReplicationContent().includes('DATA');
        const kafkaEntries = [updatedSourceEntry.toKafkaEntry(
            sourceEntry.getReplicationBackend())];
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

    async _setupRolesOnce(entry, log) {
        log.debug('getting bucket replication',
            { entry: entry.getLogInfo() });
        const entryRolesString = entry.getReplicationRoles(entry.getReplicationBackend());
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
            throw errors.BadRole;
        }
        this.sourceRole = entryRoles[0];
        this.targetRole = entryRoles[1];

        this._setupSourceClients(this.sourceRole, log);

        const command = new GetBucketReplicationCommand(
            { Bucket: entry.getBucket() });
        attachReqUids(command, log.getSerializedUids());

        let data;
        try {
            data = await this.S3source.send(command);
        } catch (err) {
            err.origin = 'source';
            log.error('error getting replication configuration from S3',
                {
                    method: 'ReplicateObject._setupRolesOnce',
                    entry: entry.getLogInfo(),
                    origin: 'source',
                    peer: this.sourceConfig.s3,
                    error: err.message,
                    err,
                    httpStatus: err.$metadata?.httpStatusCode,
                });
            throw err;
        }

        const replicationEnabled = data.ReplicationConfiguration.Rules.some(
            rule => entry.getObjectKey().startsWith(
                rule.Filter?.Prefix ?? rule.Prefix ?? '')
            && rule.Status === 'Enabled');
        if (!replicationEnabled) {
            log.debug('replication disabled for object',
                {
                    method: 'ReplicateObject._setupRolesOnce',
                    entry: entry.getLogInfo(),
                });
            throw errors.PreconditionFailed.customizeDescription(
                'replication disabled for object');
        }
        const roles = data.ReplicationConfiguration.Role.split(',');
        if (roles.length > 2) {
            log.error('expecting one or two roles in bucket ' +
                'replication configuration',
                {
                    method: 'ReplicateObject._setupRolesOnce',
                    entry: entry.getLogInfo(),
                    roles,
                });
            throw errors.BadRole;
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
            throw errors.BadRole;
        }
        // Pick the rule for this specific backend (multi-destination
        // configs may share a StorageClass with distinct Bucket /
        // Account), then derive the expected destination role from
        // its Account; fall back to literal role[1] for legacy
        // configs without Account.
        const entryDestination = entry.getDestination();
        const entryRoleAccount = entry.getRole()?.split(':')[4];
        const matchingRule = data.ReplicationConfiguration.Rules.find(rule => {
            const prefix = rule.Filter?.Prefix ?? rule.Prefix ?? '';
            return rule.Status === 'Enabled' &&
                rule.Destination?.StorageClass === this.site &&
                entry.getObjectKey().startsWith(prefix) &&
                (!entryDestination || rule.Destination?.Bucket === entryDestination) &&
                (!entryRoleAccount || !rule.Destination?.Account
                    || rule.Destination.Account === entryRoleAccount);
        });
        let expectedDestRole;
        if (matchingRule && matchingRule.Destination.Account) {
            expectedDestRole = ReplicationConfiguration
                .resolveDestinationRole(
                    data.ReplicationConfiguration.Role,
                    matchingRule.Destination.Account);
        } else if (roles.length === 2) {
            expectedDestRole = roles[1];
        } else {
            expectedDestRole = roles[0];
        }
        if (expectedDestRole !== entryRoles[1]) {
            log.error('role in replication entry for target does ' +
                'not match role in bucket replication configuration ',
                {
                    method: 'ReplicateObject._setupRolesOnce',
                    entry: entry.getLogInfo(),
                    entryRole: entryRoles[1],
                    bucketRole: expectedDestRole,
                });
            throw errors.BadRole;
        }
        return [entryRoles[0], entryRoles[1]];
    }

    async _setTargetAccountMdOnce(destEntry, targetRole, log) {
        log.debug('changing target account owner',
            { entry: destEntry.getLogInfo() });
        const targetAccountId = _extractAccountIdFromRole(targetRole);
        const lookupAccountAttributes = promisify(
            this.s3destCredentials.lookupAccountAttributes.bind(this.s3destCredentials));
        let accountAttr;
        try {
            accountAttr = await lookupAccountAttributes(targetAccountId);
        } catch (err) {
            err.origin = 'target';
            let peer;
            if (this.destConfig.auth.type === 'role') {
                peer = this.destBackbeatHost;
                if (this.destConfig.auth.vault) {
                    const { host, port } = this.destConfig.auth.vault;
                    if (host) {
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
            throw err;
        }
        log.debug('setting owner info in target metadata',
            {
                entry: destEntry.getLogInfo(),
                accountAttr,
            });
        destEntry.setOwnerId(accountAttr.canonicalID);
        destEntry.setOwnerDisplayName(accountAttr.displayName);
    }

    async _refreshSourceEntry(sourceEntry, log) {
        const params = {
            Bucket: sourceEntry.getBucket(),
            Key: sourceEntry.getObjectKey(),
            VersionId: sourceEntry.getEncodedVersionId(),
            RequestUids: log.getSerializedUids(),
        };
        let data;
        try {
            data = await this.backbeatSource.send(new GetMetadataCommand(params));
        } catch (err) {
            err.origin = 'source';
            const logFields = {
                method: 'ReplicateObject._refreshSourceEntry',
                error: err,
            };
            if (isAccessDeniedError(err)) {
                Object.assign(logFields, getAccessDeniedLogFields(
                    sourceEntry.getBucket(), this.sourceRole));
            }
            log.error('error getting metadata blob from S3', logFields);
            throw err;
        }
        const parsedEntry = ObjectQueueEntry.createFromBlob(data.Body);
        if (parsedEntry.error) {
            log.error('error parsing metadata blob', {
                error: parsedEntry.error,
                method: 'ReplicateObject._refreshSourceEntry',
            });
            throw errors.InternalError.customizeDescription('error parsing metadata blob');
        }
        return new ObjectQueueEntry(sourceEntry.getBucket(),
            sourceEntry.getObjectVersionedKey(), parsedEntry.result)
            .setReplicationBackend(sourceEntry.getReplicationBackend());
    }

    async _getAndPutData(sourceEntry, destEntry, log) {
        log.debug('replicating data', { entry: sourceEntry.getLogInfo() });
        const missingETag = sourceEntry.getLocation().some(part =>
            new ObjectMDLocation(part).getDataStoreETag() === undefined);
        if (missingETag) {
            const errMessage = 'cannot replicate object without dataStoreETag property';
            log.error(errMessage, {
                method: 'ReplicateObject._getAndPutData',
                entry: sourceEntry.getLogInfo(),
            });
            throw errors.InternalError.customizeDescription(errMessage);
        }
        // For Replication Replay testing, set the BACKBEAT_INJECT_REPLICATION_ERROR_RATE variable
        if (BACKBEAT_INJECT_REPLICATION_ERROR_RATE) {
            if (Math.random() < BACKBEAT_INJECT_REPLICATION_ERROR_RATE) {
                throw new Error('Replication error');
            }
        }
        const locations = sourceEntry.getReducedLocations();
        const mpuConcLimit = this.repConfig.queueProcessor.mpuPartsConcurrency;
        const [mapErr, partResults] = await runTasksWithConcurrency(
            locations, mpuConcLimit,
            part => this._getAndPutPart(sourceEntry, destEntry, part, log));

        let collisionResult;
        const uploadedParts = [];
        for (const result of (partResults || [])) {
            if (!result) {
                continue;
            }
            if (result.isCollision) {
                collisionResult = collisionResult || result;
            } else {
                uploadedParts.push(result);
            }
        }
        const hasPutDataConflict = collisionResult !== undefined;
        // On error or conflict, drop all parts written
        if (mapErr || hasPutDataConflict) {
            await this._deleteOrphans(destEntry, uploadedParts, log);
            if (hasPutDataConflict) {
                return [[], collisionResult];
            }
            throw mapErr;
        }
        return [partResults, undefined];
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

    async _getAndPutPartOnce(sourceEntry, destEntry, part, log) {
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

        let response;
        try {
            response = await this.S3source.send(command, { abortSignal: abortController.signal });
        } catch (err) {
            if (!sourceStreamAborted) {
                abortController.abort();
                destRequestAborted = true;
            }
            err.origin = 'source';
            if (err.$metadata?.httpStatusCode !== 404) {
                log.error('an error occurred on getObject from S3', {
                    method: 'ReplicateObject._getAndPutPartOnce',
                    entry: sourceEntry.getLogInfo(),
                    part,
                    origin: 'source',
                    peer: this.sourceConfig.s3,
                    error: err.message,
                    err,
                    httpStatus: err.$metadata?.httpStatusCode,
                });
            }
            throw err;
        }

        const incomingMsg = response.Body;
        const putCommand = new PutDataCommand({
            Bucket: destEntry.getBucket(),
            Key: destEntry.getObjectKey(),
            CanonicalID: destEntry.getOwnerId(),
            ContentMD5: partObj.getPartETag(),
            Body: incomingMsg,
            // destination bucket has to be versioning enabled.
            VersioningRequired: true,
            RequestUids: log.getSerializedUids(),
            VersionId: sourceEntry.getEncodedVersionId(),
        });
        addContentLengthMiddleware(putCommand, response.ContentLength);
        attachExpectContinueMiddleware(
            putCommand,
            this.backbeatDest.config?.requestHandler,
            replicationExpectContinueThreshold,
        );
        log.debug('putting data', { entry: destEntry.getLogInfo(), part });
        const writeStartTime = Date.now();

        return await new Promise((resolve, reject) => {
            incomingMsg.on('error', err => {
                if (!sourceStreamAborted && !destRequestAborted) {
                    abortController.abort();
                    destRequestAborted = true;
                }
                if (err.$metadata?.httpStatusCode === 404) {
                    const objNotFound = errors.ObjNotFound;
                    objNotFound.origin = 'source';
                    return reject(objNotFound);
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
                return reject(err);
            });

            incomingMsg.on('end', () => {
                this._publishReadMetrics(partSize, readStartTime);
            });

            this.backbeatDest.send(putCommand, { abortSignal: abortController.signal })
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
                    resolve(partObj.getValue());
                })
                .catch(err => {
                    if (!destRequestAborted) {
                        abortController.abort();
                        sourceStreamAborted = true;
                        if (incomingMsg.destroy) {
                            incomingMsg.destroy();
                        }
                    }
                    if (err instanceof VersionIdCollisionException) {
                        log.info('cascade putData: data already at destination', {
                            method: 'ReplicateObject._getAndPutPartOnce',
                            entry: destEntry.getLogInfo(),
                        });
                        return resolve({ isCollision: true, microVersionId: err.microVersionId });
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
                    return reject(err);
                });
        });
    }

    async _putMetadataOnce(entry, mdOnly, conflict, log) {
        if (this._shouldSkipMetadata(entry.getMicroVersionId(), conflict, log)) {
            log.info('skipping putMetadata: destination already has same or newer revision', {
                entry: entry.getLogInfo(),
            });
            return;
        }
        log.debug('putting metadata', {
            where: 'target', entry: entry.getLogInfo(),
            replicationStatus: entry.getReplicationSiteStatus(entry.getReplicationBackend()),
        });

        // accountid is only needed when using assumeRole auth
        // to delegate the task of updating metadata with
        // the target account info to the destination's Cloudserver
        let accountId = undefined;
        if (this.destConfig.auth.type === authTypeAssumeRole) {
            accountId = _extractAccountIdFromRole(this.targetRole);
        }

        // METADATA: update existing document (preserve stored location).
        // DATA,METADATA: create a new document.
        const localMdOnly = mdOnly || !!conflict;
        const replicationContent = (localMdOnly ? 'METADATA' : 'DATA,METADATA');
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
            MicroVersionId: entry.getMicroVersionId()
                ? encodeMicroVersionId(entry.getMicroVersionId()) : '',
        });
        const writeStartTime = Date.now();
        try {
            await this.backbeatDest.send(command);
        } catch (err) {
            err.origin = 'target';
            if (err.ObjNotFound || err.name === 'ObjNotFound' ||
                err instanceof MicroVersionIdAlreadyStoredException ||
                err instanceof StaleMicroVersionIdException) {
                throw err;
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
            throw err;
        }
        this._publishMetadataWriteMetrics(mdBlob, writeStartTime);
    }

    async _deleteOrphans(entry, locations, log) {
        const writtenLocations = locations
            .filter(loc => loc)
            .map(loc => ({ key: loc.key, dataStoreName: loc.dataStoreName }));
        if (writtenLocations.length === 0) {
            return;
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
        try {
            await this.backbeatDest.send(command);
        } catch (err) {
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
            // do not propagate the batch delete error, only log it
        }
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

    async _processQueueEntry(_sourceEntry, kafkaEntry) {
        let sourceEntry = _sourceEntry;
        const log = this.logger.newRequestLogger();
        const destEntry = sourceEntry.toReplicaEntry(sourceEntry.getReplicationBackend());

        log.debug('processing entry',
            { entry: sourceEntry.getLogInfo() });

        const lastModified = new Date(sourceEntry.getLastModified());
        this.metricsHandler.rpo({
            serviceName: this.serviceName,
            location: this.site,
        }, (Date.now() - lastModified) / 1000);

        const isDeleteMarker = sourceEntry.getIsDeleteMarker();
        const mdOnly = !sourceEntry.getReplicationContent().includes('DATA');

        try {
            const [, targetRole] = await this._setupRoles(sourceEntry, log);
            await this._setTargetAccountMd(destEntry, targetRole, log);

            if (isDeleteMarker) {
                // TODO check that bucket role matches role in metadata
                await this._putMetadata(destEntry, false, null, log);
            } else {
                if (!mdOnly) {
                    const isLargeObject = sourceEntry.getContentLength() / 1000000 >=
                        this.repConfig.queueProcessor.sourceCheckIfSizeGreaterThanMB;
                    const isLocationStripped = sourceEntry.getContentLength() > 0 &&
                        sourceEntry.getLocation().length === 0;
                    if (isLargeObject || isLocationStripped) {
                        const refreshedEntry = await this._refreshSourceEntry(sourceEntry, log);
                        const status = refreshedEntry.getReplicationSiteStatus(
                            sourceEntry.getReplicationBackend());
                        if (status === 'COMPLETED') {
                            log.info('replication already completed, skipping', {
                                entry: sourceEntry.getLogInfo(),
                            });
                            throw errorAlreadyCompleted;
                        }
                        sourceEntry = refreshedEntry;
                    }
                    const extMetrics = getExtMetrics(this.site,
                        sourceEntry.getContentLength(), sourceEntry);
                    this.mProducer.publishMetrics(extMetrics,
                        metricsTypeQueued, metricsExtension, () => {});
                }

                const [destLocations, conflict] = !mdOnly
                    ? await this._getAndPutData(sourceEntry, destEntry, log)
                    : [[], undefined];

                destEntry.setLocation(destLocations);
                try {
                    await this._putMetadata(destEntry, mdOnly, conflict, log);
                } catch (err) {
                    await this._deleteOrphans(destEntry, destLocations, log);
                    throw err;
                }
            }
        } catch (err) {
            return await this._handleReplicationOutcome(err, sourceEntry, destEntry, kafkaEntry, log);
        }
        return await this._handleReplicationOutcome(null, sourceEntry, destEntry, kafkaEntry, log);
    }

    processQueueEntry(_sourceEntry, kafkaEntry, done) {
        this._processQueueEntry(_sourceEntry, kafkaEntry).then(
            result => result === null ? done() : done(null, result),
            err => done(err),
        );
    }

    // Returns true if putMetadata can be skipped because the destination already
    // holds this revision or a newer one. Returns false when there is no conflict,
    // when the destination microVersionId is absent or can't be parsed (proceed
    // conservatively), or when the source holds a newer revision.
    _shouldSkipMetadata(sourceMicroVersionId, conflict, log) {
        if (!conflict) {
            return false;
        }
        let destMvId = null;
        if (conflict.microVersionId) {
            const decoded = decodeMicroVersionId(conflict.microVersionId);
            if (decoded instanceof Error) {
                log.warn('could not decode microVersionId from putData 409, ' +
                    'proceeding to putMetadata without skip optimisation', {
                    error: decoded.message,
                });
            } else {
                destMvId = decoded;
            }
        }
        const comparison = compareMicroVersionId(sourceMicroVersionId, destMvId);
        return destMvId !== null &&
            (comparison === Ordering.OLDER || comparison === Ordering.EQUAL);
    }

    async _processQueueEntryRetryFull(sourceEntry, destEntry, kafkaEntry, log) {
        let destLocations = null;
        try {
            let conflict;
            [destLocations, conflict] = await this._getAndPutData(sourceEntry, destEntry, log);
            destEntry.setLocation(destLocations);
            await this._putMetadata(destEntry, false, conflict, log);
        } catch (err) {
            if (destLocations !== null) {
                log.warn('putMetadata failed during full retry, cleaning up orphan data', {
                    method: 'ReplicateObject._processQueueEntryRetryFull',
                    entry: destEntry.getLogInfo(),
                    error: err.message,
                });
                await this._deleteOrphans(destEntry, destLocations, log);
            }
            return await this._handleReplicationOutcome(err, sourceEntry, destEntry, kafkaEntry, log);
        }
        return await this._handleReplicationOutcome(null, sourceEntry, destEntry, kafkaEntry, log);
    }

    async _handleReplicationOutcome(err, sourceEntry, destEntry, kafkaEntry, log) {
        if (err instanceof MicroVersionIdAlreadyStoredException ||
            err instanceof StaleMicroVersionIdException) {
            log.info('replication completed: metadata revision already at destination',
                { entry: sourceEntry.getLogInfo(), reason: err.name });
            this._publishReplicationStatus(sourceEntry, 'COMPLETED', { kafkaEntry, log });
            return { committable: false };
        }
        if (!err) {
            log.debug('replication succeeded for object, publishing ' +
                'replication status as COMPLETED',
                { entry: sourceEntry.getLogInfo() });
            this._publishReplicationStatus(sourceEntry, 'COMPLETED', { kafkaEntry, log });
            return { committable: false };
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
            return null;
        }
        if (err === errorAlreadyCompleted) {
            log.warn('replication skipped: ' +
                     'source object version already COMPLETED',
                     { entry: sourceEntry.getLogInfo() });
            return null;
        }
        if (err.ObjNotFound || err.name === 'ObjNotFound') {
            if (err.origin === 'source') {
                log.info('replication skipped: ' +
                    'source object version does not exist',
                    { entry: sourceEntry.getLogInfo() });
                return null;
            }
            log.info('replication target object not found, retrying with full data write',
                { entry: sourceEntry.getLogInfo() });
            // TODO: Is this the right place to capture retry metrics?
            return await this._processQueueEntryRetryFull(
                sourceEntry, destEntry, kafkaEntry, log);
        }
        if (err.InvalidObjectState || err.name === 'InvalidObjectState') {
            log.info('replication skipped: invalid object state',
                     { entry: sourceEntry.getLogInfo() });
            return null;
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
        return { committable: false };
    }
}

module.exports = ReplicateObject;
