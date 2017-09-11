'use strict'; // eslint-disable-line

const async = require('async');
const assert = require('assert');
const AWS = require('aws-sdk');
const http = require('http');
const BackOff = require('backo');

const Logger = require('werelogs').Logger;

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const RoundRobin = require('arsenal').network.RoundRobin;
const VaultClient = require('vaultclient').Client;
const { proxyPath } = require('../constants');

const authdata = require('../../../conf/authdata.json');

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const QueueEntry = require('../utils/QueueEntry');
const CredentialsManager = require('../../../credentials/CredentialsManager');

const MPU_CONC_LIMIT = 10;
/**
* Given that the largest object JSON from S3 is about 1.6 MB and adding some
* padding to it, Backbeat replication topic is currently setup with a config
* max.message.bytes.limit to 5MB. Consumers need to update their fetchMaxBytes
* to get atleast 5MB put in the Kafka topic, adding a little extra bytes of
* padding for approximation.
*/
const CONSUMER_FETCH_MAX_BYTES = 5000020;
const BACKOFF_PARAMS = { min: 1000, max: 300000, jitter: 0.1, factor: 1.5 };

class _AccountAuthManager {
    constructor(authConfig, log) {
        assert.strictEqual(authConfig.type, 'account');

        this._log = log;
        const accountInfo = authdata.accounts.find(
            account => account.name === authConfig.account);
        if (accountInfo === undefined) {
            throw Error(`No such account registered: ${authConfig.account}`);
        }
        if (accountInfo.arn === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"arn" property defined');
        }
        if (accountInfo.canonicalID === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"canonicalID" property defined');
        }
        if (accountInfo.displayName === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"displayName" property defined');
        }
        this._accountArn = accountInfo.arn;
        this._canonicalID = accountInfo.canonicalID;
        this._displayName = accountInfo.displayName;
        this._credentials = new AWS.Credentials(accountInfo.keys.access,
                                                accountInfo.keys.secret);
    }

    getCredentials() {
        return this._credentials;
    }

    lookupAccountAttributes(accountId, cb) {
        const localAccountId = this._accountArn.split(':')[4];
        if (localAccountId !== accountId) {
            this._log.error('Target account for replication must match ' +
                            'configured destination account ARN',
                            { targetAccountId: accountId,
                              localAccountId });
            return process.nextTick(() => cb(errors.AccountNotFound));
        }
        // return local account's attributes
        return process.nextTick(
            () => cb(null, { canonicalID: this._canonicalID,
                             displayName: this._displayName }));
    }
}

class _RoleAuthManager {
    constructor(vaultClient, roleArn, log) {
        this._log = log;
        this._vaultclient = vaultClient;
        this._credentials = new CredentialsManager(
            vaultClient, 'replication', roleArn, log.getUids());
    }

    getCredentials() {
        return this._credentials;
    }

    lookupAccountAttributes(accountId, cb) {
        this._vaultclient.getCanonicalIdsByAccountIds(
            [accountId], { reqUid: this._log.getSerializedUids() },
            (err, res) => {
                if (err) {
                    return cb(err);
                }
                if (!res || !res.message || !res.message.body
                    || res.message.body.length === 0) {
                    return cb(errors.AccountNotFound);
                }
                return cb(null, {
                    canonicalID: res.message.body[0].canonicalId,
                    displayName: res.message.body[0].name,
                });
            });
    }
}


function _extractAccountIdFromRole(role) {
    return role.split(':')[4];
}

function attachReqUids(s3req, log) {
    s3req.on('build', () => {
        // eslint-disable-next-line no-param-reassign
        s3req.httpRequest.headers['X-Scal-Request-Uids'] =
            log.getSerializedUids();
    });
}

class QueueProcessor {

    /**
     * Create a queue processor object to activate Cross-Region
     * Replication from a kafka topic dedicated to store replication
     * entries to a target S3 endpoint.
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {string} zkConfig.connectionString - zookeeper connection string
     *   as "host:port[/chroot]"
     * @param {Object} sourceConfig - source S3 configuration
     * @param {Object} sourceConfig.s3 - s3 endpoint configuration object
     * @param {Object} sourceConfig.auth - authentication info on source
     * @param {Object} destConfig - target S3 configuration
     * @param {Object} destConfig.auth - authentication info on target
     * @param {Object} repConfig - replication configuration object
     * @param {String} repConfig.topic - replication topic name
     * @param {String} repConfig.queueProcessor - config object
     *   specific to queue processor
     * @param {String} repConfig.queueProcessor.groupId - kafka
     *   consumer group ID
     * @param {String} repConfig.queueProcessor.retryTimeoutS -
     *   number of seconds before giving up retries of an entry
     *   replication
     */
    constructor(zkConfig, sourceConfig, destConfig, repConfig) {
        this.zkConfig = zkConfig;
        this.sourceConfig = sourceConfig;
        this.destConfig = destConfig;
        this.repConfig = repConfig;

        this.logger = new Logger('Backbeat:Replication:QueueProcessor');

        // global variables
        // TODO: for SSL support, create HTTPS agents instead
        this.sourceHTTPAgent = new http.Agent({ keepAlive: true });
        this.destHTTPAgent = new http.Agent({ keepAlive: true });
        // FIXME support multiple destination sites
        if (destConfig.bootstrapList.length > 0) {
            this.destHosts =
                new RoundRobin(destConfig.bootstrapList[0].servers);
        } else {
            this.destHosts = null;
        }
        // per-request state variables
        this.sourceRole = null;
        this.targetRole = null;
        this.destBackbeatHost = null;
        this.s3sourceAuthManager = null;
        this.s3destAuthManager = null;
        this.S3source = null;
        this.backbeatSource = null;
        this.backbeatDest = null;
    }

    _createAuthManager(where, authConfig, roleArn, log) {
        if (authConfig.type === 'account') {
            return new _AccountAuthManager(authConfig, log);
        }
        let vaultClient;
        if (where === 'source') {
            const { host, port } = this.sourceConfig.auth.vault;
            vaultClient = new VaultClient(host, port);
        } else { // target
            const { host, port } = this.destHosts.pickHost();
            vaultClient = new VaultClient(
                host, port,
                undefined, undefined, undefined, undefined,
                undefined, undefined, undefined, undefined,
                proxyPath);
        }
        return new _RoleAuthManager(vaultClient, roleArn, log);
    }

    _retry(args, done) {
        const { actionDesc, entry,
                actionFunc, shouldRetryFunc, onRetryFunc, log } = args;
        const backoffCtx = new BackOff(BACKOFF_PARAMS);
        let nbRetries = 0;
        const startTime = Date.now();
        const self = this;

        function _handleRes(...args) {
            const err = args[0];
            if (!err) {
                if (nbRetries > 0) {
                    log.info(`succeeded to ${actionDesc} after retries`,
                             { entry: entry.getLogInfo(), nbRetries });
                }
                return done.apply(null, args);
            }
            if (!shouldRetryFunc(err)) {
                return done(err);
            }
            if (onRetryFunc) {
                onRetryFunc(err);
            }

            const now = Date.now();
            if (now > (startTime +
                       self.repConfig.queueProcessor.retryTimeoutS * 1000)) {
                log.error(`retried for too long to ${actionDesc}, giving up`,
                          { entry: entry.getLogInfo(),
                            nbRetries,
                            retryTotalMs: `${now - startTime}` });
                return done(err);
            }
            const retryDelayMs = backoffCtx.duration();
            log.info(`temporary failure to ${actionDesc}, scheduled retry`,
                     { entry: entry.getLogInfo(),
                       nbRetries, retryDelay: `${retryDelayMs}ms` });
            nbRetries += 1;
            return setTimeout(() => actionFunc(_handleRes), retryDelayMs);
        }
        actionFunc(_handleRes);
    }

    _setupRoles(entry, log, cb) {
        this._retry({
            actionDesc: 'get bucket replication configuration',
            entry,
            actionFunc: done => this._setupRolesOnce(entry, log, done),
            // Rely on AWS SDK notion of retryable error to decide if
            // we should set the entry replication status to FAILED
            // (non retryable) or retry later.
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _setTargetAccountMd(destEntry, targetRole, log, cb) {
        this._retry({
            actionDesc: 'lookup target account attributes',
            entry: destEntry,
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
        this._retry({
            actionDesc: 'stream part data',
            entry: sourceEntry,
            actionFunc: done => this._getAndPutPartOnce(
                sourceEntry, destEntry, part, log, done),
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

    _putMetadata(where, entry, mdOnly, log, cb) {
        this._retry({
            actionDesc: `update metadata on ${where}`,
            entry,
            actionFunc: done => this._putMetadataOnce(where, entry, mdOnly,
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

    _updateReplicationStatus(updatedSourceEntry, params, cb) {
        this._retry({
            actionDesc: 'write replication status',
            entry: updatedSourceEntry,
            actionFunc: done => this._updateReplicationStatusOnce(
                updatedSourceEntry, params, done),
            shouldRetryFunc: err => err.retryable,
            log: params.log,
        }, cb);
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
                      { method: 'QueueProcessor._setupRoles',
                        entry: entry.getLogInfo(),
                        roles: entryRolesString });
            return cb(errors.BadRole);
        }
        this.sourceRole = entryRoles[0];
        this.targetRole = entryRoles[1];

        this._setupSourceClients(this.sourceRole, log);
        this._setupDestClients(this.targetRole, log);

        const req = this.S3source.getBucketReplication(
            { Bucket: entry.getBucket() });
        attachReqUids(req, log);
        return req.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                log.error('error getting replication ' +
                          'configuration from S3',
                          { method: 'QueueProcessor._setupRoles',
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
                          { method: 'QueueProcessor._setupRoles',
                            entry: entry.getLogInfo() });
                return cb(errors.PreconditionFailed.customizeDescription(
                    'replication disabled for object'));
            }
            const roles = data.ReplicationConfiguration.Role.split(',');
            if (roles.length !== 2) {
                log.error('expecting two roles separated by a ' +
                          'comma in bucket replication configuration',
                          { method: 'QueueProcessor._setupRoles',
                            entry: entry.getLogInfo(),
                            roles });
                return cb(errors.BadRole);
            }
            if (roles[0] !== entryRoles[0]) {
                log.error('role in replication entry for source does ' +
                          'not match role in bucket replication ' +
                          'configuration ',
                          { method: 'QueueProcessor._setupRoles',
                            entry: entry.getLogInfo(),
                            entryRole: entryRoles[0],
                            bucketRole: roles[0] });
                return cb(errors.BadRole);
            }
            if (roles[1] !== entryRoles[1]) {
                log.error('role in replication entry for target does ' +
                          'not match role in bucket replication ' +
                          'configuration ',
                          { method: 'QueueProcessor._setupRoles',
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
        this.s3destAuthManager.lookupAccountAttributes(
            targetAccountId, (err, accountAttr) => {
                if (err) {
                    // eslint-disable-next-line no-param-reassign
                    err.origin = 'target';
                    log.error('an error occurred when looking up target ' +
                              'account attributes',
                              { method: 'QueueProcessor._setTargetAccountMd',
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
                destEntry.setOwner(accountAttr.canonicalID,
                                   accountAttr.displayName);
                return cb();
            });
    }

    // Object metadata may contain multiple elements for a single part if
    // the part was originally copied from another MPU. Here we reduce the
    // locations array to a single element for each part.
    _getLocations(sourceEntry, log) {
        const locations = sourceEntry.getLocation();
        const reducedLocations = [];
        let partTotal = 0;
        if (locations.some(
            part => sourceEntry.getDataStoreETag(part) === undefined)) {
            log.error('cannot replicate object without dataStoreETag ' +
                      'property',
                      { method: 'QueueProcessor._getLocations',
                        entry: sourceEntry.getLogInfo() });
            return undefined;
        }
        for (let i = 0; i < locations.length; i++) {
            const currPart = locations[i];
            const currPartNum = sourceEntry.getPartNumber(currPart);
            const nextPart = locations[i + 1];
            const nextPartNum = nextPart ?
                sourceEntry.getPartNumber(nextPart) : undefined;
            if (currPartNum === nextPartNum) {
                partTotal += sourceEntry.getPartSize(currPart);
            } else {
                currPart.size = partTotal += sourceEntry.getPartSize(currPart);
                reducedLocations.push(currPart);
                partTotal = 0;
            }
        }
        return reducedLocations;
    }

    _getAndPutData(sourceEntry, destEntry, log, cb) {
        log.debug('replicating data', { entry: sourceEntry.getLogInfo() });
        const locations = this._getLocations(sourceEntry, log);
        if (!locations) {
            return cb(errors.InvalidObjectState);
        }
        return async.mapLimit(locations, MPU_CONC_LIMIT, (part, done) => {
            this._getAndPutPart(sourceEntry, destEntry, part, log, done);
        }, cb);
    }

    _getAndPutPartOnce(sourceEntry, destEntry, part, log, done) {
        const doneOnce = jsutil.once(done);
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
                return doneOnce(err);
            }
            log.error('an error occurred on getObject from S3',
                      { method: 'QueueProcessor._getAndPutData',
                        entry: sourceEntry.getLogInfo(),
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
                      { method: 'QueueProcessor._getAndPutData',
                        entry: destEntry.getLogInfo(),
                        origin: 'source',
                        peer: this.sourceConfig.s3,
                        error: err.message });
            return doneOnce(err);
        });
        log.debug('putting data', { entry: destEntry.getLogInfo() });
        const destReq = this.backbeatDest.putData({
            Bucket: destEntry.getBucket(),
            Key: destEntry.getObjectKey(),
            CanonicalID: destEntry.getOwnerCanonicalId(),
            ContentLength: destEntry.getPartSize(part),
            ContentMD5: destEntry.getPartETag(part),
            Body: incomingMsg,
        });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'target';
                log.error('an error occurred on putData to S3',
                          { method: 'QueueProcessor._getAndPutData',
                            entry: destEntry.getLogInfo(),
                            origin: 'target',
                            peer: this.destBackbeatHost,
                            error: err.message });
                return doneOnce(err);
            }
            return doneOnce(null,
                            destEntry.buildLocationKey(part, data.Location[0]));
        });
    }

    _putMetadataOnce(where, entry, mdOnly, log, cb) {
        log.debug('putting metadata',
                  { where, entry: entry.getLogInfo(),
                    replicationStatus: entry.getReplicationStatus() });
        const cbOnce = jsutil.once(cb);
        const target = where === 'source' ?
                  this.backbeatSource : this.backbeatDest;

        // sends extra header x-scal-replication-content to the target
        // if it's a metadata operation only
        const replicationContent = (mdOnly ? 'METADATA' : undefined);
        const mdBlob = entry.getMetadataBlob();
        const req = target.putMetadata({
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
                err.origin = where;
                if (err.ObjNotFound || err.code === 'ObjNotFound') {
                    return cbOnce(err);
                }
                log.error('an error occurred when putting metadata to S3',
                          { method: 'QueueProcessor._putMetadata',
                            entry: entry.getLogInfo(),
                            origin: where,
                            peer: (where === 'source' ?
                                   this.sourceConfig.s3 :
                                   this.destBackbeatHost),
                            error: err.message });
                return cbOnce(err);
            }
            return cbOnce(null, data);
        });
    }

    _setupSourceClients(sourceRole, log) {
        this.s3sourceAuthManager =
            this._createAuthManager('source', this.sourceConfig.auth,
                                    sourceRole, log);

        // Disable retries, use our own retry policy (mandatory for
        // putData route in order to fetch data again from source).

        const sourceS3 = this.sourceConfig.s3;
        this.S3source = new AWS.S3({
            endpoint: `${this.sourceConfig.transport}://` +
                `${sourceS3.host}:${sourceS3.port}`,
            credentials:
            this.s3sourceAuthManager.getCredentials(),
            sslEnabled: this.sourceConfig.transport === 'https',
            s3ForcePathStyle: true,
            signatureVersion: 'v4',
            httpOptions: { agent: this.sourceHTTPAgent, timeout: 0 },
            maxRetries: 0,
        });
        this.backbeatSource = new BackbeatClient({
            endpoint: `${this.sourceConfig.transport}://` +
                `${sourceS3.host}:${sourceS3.port}`,
            credentials: this.s3sourceAuthManager.getCredentials(),
            sslEnabled: this.sourceConfig.transport === 'https',
            httpOptions: { agent: this.sourceHTTPAgent, timeout: 0 },
            maxRetries: 0,
        });
    }

    _setupDestClients(targetRole, log) {
        this.s3destAuthManager =
            this._createAuthManager('target', this.destConfig.auth,
                                    targetRole, log);

        this.destBackbeatHost = this.destHosts.pickHost();
        this.backbeatDest = new BackbeatClient({
            endpoint: `${this.destConfig.transport}://` +
                `${this.destBackbeatHost.host}:${this.destBackbeatHost.port}`,
            credentials: this.s3destAuthManager.getCredentials(),
            sslEnabled: this.destConfig.transport === 'https',
            httpOptions: { agent: this.destHTTPAgent },
            maxRetries: 0,
        });
    }

    /**
     * Proceed to the replication of an object given a kafka
     * replication queue entry
     *
     * @param {object} kafkaEntry - entry generated by the queue populator
     * @param {string} kafkaEntry.key - kafka entry key
     * @param {string} kafkaEntry.value - kafka entry value
     * @param {function} done - callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, done) {
        const log = this.logger.newRequestLogger();

        const sourceEntry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        if (sourceEntry.error) {
            log.error('error processing source entry',
                      { error: sourceEntry.error });
            return process.nextTick(() => done(errors.InternalError));
        }
        return this._processQueueEntry(sourceEntry, log, done);
    }

    _processQueueEntry(sourceEntry, log, done) {
        const destEntry = sourceEntry.toReplicaEntry();

        log.debug('processing entry',
                  { entry: sourceEntry.getLogInfo() });


        if (sourceEntry.isDeleteMarker()) {
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
                    this._putMetadata('target', destEntry, false, log, next);
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
                    return this._getAndPutData(sourceEntry, destEntry, log,
                        next);
                }
                return next(null, []);
            },
            // update location, replication status and put metadata in
            // target bucket
            (location, next) => {
                destEntry.setLocation(location);
                this._putMetadata('target', destEntry, mdOnly, log, next);
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
                this._putMetadata('target', destEntry, false, log, next);
            },
        ], err => this._handleReplicationOutcome(err, sourceEntry, destEntry,
                                                 log, done));
    }

    _handleReplicationOutcome(err, sourceEntry, destEntry, log, done) {
        if (!err) {
            log.debug('replication succeeded for object, updating ' +
                      'source replication status to COMPLETED',
                      { entry: sourceEntry.getLogInfo() });
            return this._updateReplicationStatus(
                sourceEntry.toCompletedEntry(), { log }, done);
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
                  'updating replication status to FAILED',
                  { failMethod: err.method,
                    entry: sourceEntry.getLogInfo(),
                    error: err.description });
        return this._updateReplicationStatus(
            sourceEntry.toFailedEntry(),
            { log, reason: err.description }, done);
    }

    _updateReplicationStatusOnce(updatedSourceEntry, params, done) {
        const { log, reason } = params;

        const _doneUpdate = err => {
            if (err) {
                log.error('an error occurred when writing replication ' +
                          'status',
                          { entry: updatedSourceEntry.getLogInfo(),
                            origin: 'source',
                            peer: this.sourceConfig.s3,
                            replicationStatus:
                            updatedSourceEntry.getReplicationStatus() });
                return done(err);
            }
            log.end().info('replication status updated',
                           { entry: updatedSourceEntry.getLogInfo(),
                             replicationStatus:
                             updatedSourceEntry.getReplicationStatus(),
                             reason });
            return done();
        };

        if (this.backbeatSource !== null) {
            return this._putMetadata(
                'source', updatedSourceEntry, false, log, _doneUpdate);
        }
        log.end().info('replication status update skipped',
                       { entry: updatedSourceEntry.getLogInfo(),
                         replicationStatus:
                         updatedSourceEntry.getReplicationStatus() });
        return done();
    }

    start() {
        const consumer = new BackbeatConsumer({
            zookeeper: this.zkConfig,
            topic: this.repConfig.topic,
            groupId: this.repConfig.queueProcessor.groupId,
            concurrency: 10, // versioning can support out of order updates
            queueProcessor: this.processKafkaEntry.bind(this),
            fetchMaxBytes: CONSUMER_FETCH_MAX_BYTES,
        });
        consumer.on('error', () => {});
        consumer.subscribe();

        this.logger.info('queue processor is ready to consume ' +
                         'replication entries');
    }
}

module.exports = QueueProcessor;
