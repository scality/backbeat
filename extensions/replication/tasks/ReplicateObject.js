const async = require('async');
const AWS = require('aws-sdk');

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const ObjectMDLocation = require('arsenal').models.ObjectMDLocation;

const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const mapLimitWaitPendingIfError = require('../../../lib/util/mapLimitWaitPendingIfError');
const { attachReqUids } = require('../../../lib/clients/utils');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const AccountCredentials =
    require('../../../lib/credentials/AccountCredentials');
const RoleCredentials =
    require('../../../lib/credentials/RoleCredentials');
const {
    metricsExtension,
    metricsTypeProcessed,
    replicationStages,
} = require('../constants');
const ObjectQueueEntry = require('../utils/ObjectQueueEntry');

const errorAlreadyCompleted = {};

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
        if (authConfig.type === 'account') {
            return new AccountCredentials(authConfig, log);
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

    _publishReplicationStatus(sourceEntry, replicationStatus, params) {
        const { log, reason, kafkaEntry } = params;
        const updatedSourceEntry = replicationStatus === 'COMPLETED' ?
            sourceEntry.toCompletedEntry(this.site) :
            sourceEntry.toFailedEntry(this.site);
        updatedSourceEntry.setReplicationSiteDataStoreVersionId(this.site,
            sourceEntry.getReplicationSiteDataStoreVersionId(this.site));
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
                this.metricsHandler.metadataReplicationStatus({ replicationStatus });
                if (updateData) {
                    this.metricsHandler.dataReplicationStatus({ replicationStatus });
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

        const req = this.S3source.getBucketReplication(
            { Bucket: entry.getBucket() });
        attachReqUids(req, log);
        return req.send((err, data) => {
            if (err) {
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
                        httpStatus: err.statusCode,
                    });
                return cb(err);
            }
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
        };
        return this.backbeatSource.getMetadata(params, (err, blob) => {
            if (err) {
                err.origin = 'source';
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

    _checkSourceReplication(sourceEntry, log, cb) {
        this._refreshSourceEntry(sourceEntry, log, (err, refreshedEntry) => {
            if (err) {
                return cb(err);
            }
            const status = refreshedEntry.getReplicationSiteStatus(this.site);
            if (status === 'COMPLETED') {
                return cb(errorAlreadyCompleted);
            }
            return cb();
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
                    method: 'ReplicateObject._getAndPutData',
                    entry: sourceEntry.getLogInfo(),
                });
            return cb(errors.InvalidObjectState);
        }
        const locations = sourceEntry.getReducedLocations();
        const mpuConcLimit = this.repConfig.queueProcessor.mpuPartsConcurrency;
        return mapLimitWaitPendingIfError(locations, mpuConcLimit, (part, done) => {
            this._getAndPutPart(sourceEntry, destEntry, part, log, done);
        }, (err, destLocations) => {
            if (err) {
                return this._deleteOrphans(sourceEntry, destLocations, log, () => cb(err));
            }
            return cb(null, destLocations);
        });
    }

    _getAndPutPartOnce(sourceEntry, destEntry, part, log, done) {
        const serviceName = this.serviceName;
        const doneOnce = jsutil.once(done);
        const partObj = new ObjectMDLocation(part);
        const partNumber = partObj.getPartNumber();
        const partSize = partObj.getPartSize();
        let destReq = null;
        let sourceReqAborted = false;
        let destReqAborted = false;
        const sourceReq = this.S3source.getObject({
            Bucket: sourceEntry.getBucket(),
            Key: sourceEntry.getObjectKey(),
            VersionId: sourceEntry.getEncodedVersionId(),
            PartNumber: partNumber,
        });
        attachReqUids(sourceReq, log);
        sourceReq.on('error', err => {
            if (!sourceReqAborted && !destReqAborted) {
                destReq.abort();
                destReqAborted = true;
            }
            // eslint-disable-next-line no-param-reassign
            err.origin = 'source';
            if (err.statusCode === 404) {
                return doneOnce(err);
            }
            if (!sourceReqAborted) {
                log.error('an error occurred on getObject from S3',
                    {
                        method: 'ReplicateObject._getAndPutPartOnce',
                        entry: sourceEntry.getLogInfo(),
                        part,
                        origin: 'source',
                        peer: this.sourceConfig.s3,
                        error: err.message,
                        httpStatus: err.statusCode,
                    });
            }
            return doneOnce(err);
        });
        const incomingMsg = sourceReq.createReadStream();
        const readStartTime = Date.now();
        incomingMsg.on('error', err => {
            if (!sourceReqAborted && !destReqAborted) {
                destReq.abort();
                destReqAborted = true;
            }
            if (err.statusCode === 404) {
                return doneOnce(errors.ObjNotFound);
            }
            if (!sourceReqAborted) {
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
                    });
            }
            return doneOnce(err);
        });
        incomingMsg.on('end', () => {
            this.metricsHandler.timeElapsed({
                serviceName,
                replicationStage: replicationStages.sourceDataRead,
            }, Date.now() - readStartTime);
            this.metricsHandler.sourceDataBytes({ serviceName }, partSize);
            this.metricsHandler.reads({ serviceName });
        });
        log.debug('putting data', { entry: destEntry.getLogInfo(), part });
        destReq = this.backbeatDest.putData({
            Bucket: destEntry.getBucket(),
            Key: destEntry.getObjectKey(),
            CanonicalID: destEntry.getOwnerId(),
            ContentLength: partSize,
            ContentMD5: partObj.getPartETag(),
            Body: incomingMsg,
        });
        attachReqUids(destReq, log);
        const writeStartTime = Date.now();
        return destReq.send((err, data) => {
            if (err) {
                if (!destReqAborted) {
                    sourceReq.abort();
                    sourceReqAborted = true;
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
                        });
                }
                return doneOnce(err);
            }
            partObj.setDataLocation(data.Location[0]);

            // Set encryption parameters that were used to encrypt the
            // target data in the object metadata, or reset them if
            // there was no encryption
            const { ServerSideEncryption, SSECustomerAlgorithm, SSEKMSKeyId } = data;
            destEntry.setAmzServerSideEncryption(ServerSideEncryption || '');
            destEntry.setAmzEncryptionCustomerAlgorithm(SSECustomerAlgorithm || '');
            destEntry.setAmzEncryptionKeyId(SSEKMSKeyId || '');

            const extMetrics = {};
            extMetrics[this.site] = {
                ops: 1,
                bytes: partSize,
            };
            this.metricsHandler.timeElapsed({
                serviceName,
                replicationStage: replicationStages.destinationDataWrite,
            }, Date.now() - writeStartTime);
            this.mProducer.publishMetrics(
                extMetrics, metricsTypeProcessed, metricsExtension, () => { });
            this.metricsHandler.dataReplicationBytes({ serviceName }, partSize);
            this.metricsHandler.writes({
                serviceName,
                replicationContent: 'data',
            });
            return doneOnce(null, partObj.getValue());
        });
    }

    _putMetadataOnce(entry, mdOnly, log, cb) {
        log.debug('putting metadata', {
            where: 'target', entry: entry.getLogInfo(),
            replicationStatus: entry.getReplicationSiteStatus(this.site),
        });
        const cbOnce = jsutil.once(cb);
        const serviceName = this.serviceName;

        // sends extra header x-scal-replication-content to the target
        // if it's a metadata operation only
        const replicationContent = (mdOnly ? 'METADATA' : undefined);
        const mdBlob = entry.getSerialized();
        const req = this.backbeatDest.putMetadata({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            VersionId: entry.getEncodedVersionId(),
            ContentLength: Buffer.byteLength(mdBlob),
            Body: mdBlob,
            ReplicationContent: replicationContent,
        });
        attachReqUids(req, log);
        const writeStartTime = Date.now();
        req.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'target';
                if (err.ObjNotFound || err.code === 'ObjNotFound') {
                    return cbOnce(err);
                }
                log.error('an error occurred when putting metadata to S3',
                    {
                        method: 'ReplicateObject._putMetadataOnce',
                        entry: entry.getLogInfo(),
                        origin: 'target',
                        peer: this.destBackbeatHost,
                        error: err.message,
                    });
                return cbOnce(err);
            }
            this.metricsHandler.timeElapsed({
                serviceName,
                replicationStage: replicationStages.destinationMetadataWrite,
            }, Date.now() - writeStartTime);
            this.metricsHandler.metadataReplicationBytes({
                serviceName,
            }, Buffer.byteLength(mdBlob));
            this.metricsHandler.writes({
                serviceName,
                replicationContent: 'metadata',
            });
            return cbOnce(null, data);
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
        const req = this.backbeatDest.batchDelete({
            Locations: writtenLocations,
        });
        attachReqUids(req, log);
        return req.send(err => {
            if (err) {
                log.error('an error occurred during batch delete of orphan data',
                    {
                        method: 'ReplicateObject._deleteOrphans',
                        entry: entry.getLogInfo(),
                        origin: 'target',
                        peer: this.destBackbeatHost,
                        error: err.message,
                    });
                writtenLocations.forEach(location => {
                    log.error('orphan data location was not deleted', {
                        method: 'ReplicateObject._deleteOrphans',
                        entry: entry.getLogInfo(),
                        location,
                    });
                });
            }
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

    processQueueEntry(sourceEntry, kafkaEntry, done) {
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
            // Get data from source bucket and put it on the target bucket
            next => {
                if (!mdOnly &&
                    sourceEntry.getContentLength() / 1000000 >=
                    this.repConfig.queueProcessor.sourceCheckIfSizeGreaterThanMB) {
                    return this._checkSourceReplication(sourceEntry, log, next);
                }
                return next();
            },
            next => {
                if (!mdOnly) {
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
                            sourceEntry, destLocations, log, () => next(err));
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
        if (err.BadRole ||
            (err.origin === 'source' &&
                (err.NoSuchEntity || err.code === 'NoSuchEntity' ||
                    err.AccessDenied || err.code === 'AccessDenied'))) {
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
            // TODO: Is this the right place to capture retry metrics?
            return this._processQueueEntryRetryFull(
                sourceEntry, destEntry, kafkaEntry, log, done);
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
