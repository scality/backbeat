'use strict'; // eslint-disable-line

const async = require('async');
const assert = require('assert');
const AWS = require('aws-sdk');
const http = require('http');
const BackOff = require('backo');

const Logger = require('werelogs').Logger;

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const VaultClient = require('vaultclient').Client;

const authdata = require('../../../conf/authdata.json');

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const QueueEntry = require('../utils/QueueEntry');
const CredentialsManager = require('../../../credentials/CredentialsManager');

const MPU_CONC_LIMIT = 10;
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
        const localAccountId = this._accountArn.split(':')[3];
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
    constructor(authConfig, roleArn, log) {
        this._log = log;
        // FIXME use bootstrap list
        const { host, port } = authConfig.vault.hosts[0];
        this._vaultclient = new VaultClient(host, port);
        this._credentials = new CredentialsManager(host, port,
                                                   'replication', roleArn);
    }

    getCredentials() {
        return this._credentials;
    }

    lookupAccountAttributes(accountId, cb) {
        this._vaultclient.getCanonicalIdsByAccountIds([accountId], {},
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
     * @param {Object} destConfig.s3 - s3 endpoint configuration object
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

        this.s3sourceAuthManager = null;
        this.s3destAuthManager = null;
        this.S3source = null;
        this.backbeatSource = null;
        this.backbeatDest = null;

        // TODO: for SSL support, create HTTPS agents instead
        this.sourceHTTPAgent = new http.Agent({ keepAlive: true });
        this.destHTTPAgent = new http.Agent({ keepAlive: true });
    }

    _createAuthManager(authConfig, roleArn, log) {
        if (authConfig.type === 'account') {
            return new _AccountAuthManager(authConfig, log);
        }
        return new _RoleAuthManager(authConfig, roleArn, log);
    }

    _retry(actionDesc, entry, shouldRetryFunc, func, log, done) {
        const backoffCtx = new BackOff(BACKOFF_PARAMS);
        let nbRetries = 0;
        const startTime = Date.now();
        const self = this;

        function _handleRes(...args) {
            const err = args[0];
            if (!err) {
                if (nbRetries > 0) {
                    log.info(`succeeded to ${actionDesc}`,
                             { entry: entry.getLogInfo(), nbRetries });
                }
                return done.apply(null, args);
            }
            if (!shouldRetryFunc(err)) {
                return done(err);
            }
            const now = Date.now();
            if (now > (startTime +
                       self.repConfig.queueProcessor.retryTimeoutS * 1000)) {
                log.error(`retried for too long to ${actionDesc}, giving up`,
                          { entry: entry.getLogInfo(),
                            nbRetries,
                            retryTotalTime: `${(now - startTime) / 1000}s` });
                return done(err);
            }
            const retryDelayMs = backoffCtx.duration();
            log.info(`temporary failure to ${actionDesc}, scheduled retry`,
                     { entry: entry.getLogInfo(),
                       nbRetries, retryDelay: `${retryDelayMs}ms` });
            nbRetries += 1;
            return setTimeout(() => func(_handleRes), retryDelayMs);
        }
        func(_handleRes);
    }

    _setupRoles(entry, log, cb) {
        this._retry(
            'get bucket replication configuration', entry,
            // Rely on AWS SDK notion of retryable error to decide if
            // we should set the entry replication status to FAILED
            // (non retryable) or retry later.
            err => err.retryable,
            done => this._setupRolesOnce(entry, log, done), log, cb);
    }

    _setTargetAccountMd(destEntry, targetRole, log, cb) {
        this._retry(
            'lookup target account attributes', destEntry,
            // this call uses our own Vault client which does not set
            // the 'retryable' field
            err => (err.InternalError || err.code === 'InternalError' ||
                    err.ServiceUnavailable ||
                    err.code === 'ServiceUnavailable'),
            done => this._setTargetAccountMdOnce(destEntry, targetRole,
                                                 log, done), log, cb);
    }

    _getAndPutPart(sourceEntry, destEntry, part, log, cb) {
        this._retry(
            'stream part data', sourceEntry,
            err => err.retryable,
            done => this._getAndPutPartOnce(sourceEntry, destEntry, part,
                                            log, done), log, cb);
    }

    _putMetadata(where, entry, log, cb) {
        this._retry(
            `update metadata on ${where}`, entry,
            err => err.retryable,
            done => this._putMetadataOnce(where, entry, log, done),
            log, cb);
    }

    _updateReplicationStatus(updatedSourceEntry, params, _done) {
        this._retry(
            'write replication status', updatedSourceEntry,
            err => err.retryable,
            done => this._updateReplicationStatusOnce(updatedSourceEntry,
                                                      params, done),
            params.log, _done);
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
                        origin: this.sourceConfig.s3,
                        roles: entryRolesString });
            return cb(errors.BadRole);
        }
        this._setupClients(entryRoles[0], entryRoles[1], log);
        return this.S3source.getBucketReplication(
            { Bucket: entry.getBucket() }, (err, data) => {
                if (err) {
                    // eslint-disable-next-line no-param-reassign
                    err.origin = 'source';
                    log.error('error getting replication ' +
                              'configuration from S3',
                              { method: 'QueueProcessor._setupRoles',
                                entry: entry.getLogInfo(),
                                origin: this.sourceConfig.s3,
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
                                entry: entry.getLogInfo(),
                                origin: this.sourceConfig.s3 });
                    return cb(errors.PreconditionFailed.customizeDescription(
                        'replication disabled for object'));
                }
                const roles = data.ReplicationConfiguration.Role.split(',');
                if (roles.length !== 2) {
                    log.error('expecting two roles separated by a ' +
                              'comma in bucket replication configuration',
                              { method: 'QueueProcessor._setupRoles',
                                entry: entry.getLogInfo(),
                                origin: this.sourceConfig.s3,
                                roles });
                    return cb(errors.BadRole);
                }
                if (roles[0] !== entryRoles[0]) {
                    log.error('role in replication entry for source does ' +
                              'not match role in bucket replication ' +
                              'configuration ',
                              { method: 'QueueProcessor._setupRoles',
                                entry: entry.getLogInfo(),
                                origin: this.sourceConfig.s3,
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
                                origin: this.sourceConfig.s3,
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


    _getAndPutData(sourceEntry, destEntry, log, cb) {
        log.debug('replicating data', { entry: sourceEntry.getLogInfo() });
        const locations = sourceEntry.getLocation();
        return async.mapLimit(locations, MPU_CONC_LIMIT, (part, done) => {
            this._getAndPutPart(sourceEntry, destEntry, part, log, done);
        }, cb);
    }

    _getAndPutPartOnce(sourceEntry, destEntry, part, log, done) {
        const doneOnce = jsutil.once(done);
        if (sourceEntry.getDataStoreETag(part) === undefined) {
            log.error('cannot replicate object without dataStoreETag ' +
                      'property',
                      { method: 'QueueProcessor._getAndPutData',
                        entry: destEntry.getLogInfo() });
            return doneOnce(errors.InvalidObjectState);
        }
        const partNumber = sourceEntry.getPartNumber(part);
        const req = this.S3source.getObject({
            Bucket: sourceEntry.getBucket(),
            Key: sourceEntry.getObjectKey(),
            VersionId: sourceEntry.getEncodedVersionId(),
            PartNumber: partNumber,
        });
        req.on('error', err => {
            // eslint-disable-next-line no-param-reassign
            err.origin = 'source';
            log.error('an error occurred on getObject from S3',
                      { method: 'QueueProcessor._getAndPutData',
                        entry: sourceEntry.getLogInfo(),
                        origin: this.sourceConfig.s3,
                        error: err.message,
                        httpStatus: err.statusCode });
            return doneOnce(err);
        });
        const incomingMsg = req.createReadStream();
        incomingMsg.on('error', err => {
            // eslint-disable-next-line no-param-reassign
            err.origin = 'source';
            log.error('an error occurred when streaming data from S3',
                      { method: 'QueueProcessor._getAndPutData',
                        entry: destEntry.getLogInfo(),
                        error: err.message });
            return doneOnce(err);
        });
        log.debug('putting data', { entry: destEntry.getLogInfo() });
        return this.backbeatDest.putData({
            Bucket: destEntry.getBucket(),
            Key: destEntry.getObjectKey(),
            CanonicalID: destEntry.getOwnerCanonicalId(),
            ContentLength: destEntry.getPartSize(part),
            ContentMD5: destEntry.getPartETag(part),
            Body: incomingMsg,
        }, (err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'target';
                log.error('an error occurred on putData to S3',
                          { method: 'QueueProcessor._getAndPutData',
                            entry: destEntry.getLogInfo(),
                            origin: this.destConfig.s3,
                            error: err.message });
                return doneOnce(err);
            }
            return doneOnce(null,
                            destEntry.buildLocationKey(part, data.Location[0]));
        });
    }

    _putMetadataOnce(where, entry, log, cb) {
        log.debug('putting metadata',
                  { where, entry: entry.getLogInfo(),
                    replicationStatus: entry.getReplicationStatus() });
        const cbOnce = jsutil.once(cb);
        const target = where === 'source' ?
                  this.backbeatSource : this.backbeatDest;
        const mdBlob = entry.getMetadataBlob();
        target.putMetadata({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            ContentLength: Buffer.byteLength(mdBlob),
            Body: mdBlob,
        }, (err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = where;
                log.error('an error occurred when putting metadata to S3',
                          { method: 'QueueProcessor._putMetadata',
                            entry: entry.getLogInfo(),
                            origin: this.destConfig.s3,
                            error: err.message });
                return cbOnce(err);
            }
            return cbOnce(null, data);
        });
    }

    _setupClients(sourceRole, targetRole, log) {
        const sourceS3 = this.sourceConfig.s3.hosts[0];
        // FIXME use bootstrap list
        const destS3 = this.destConfig.s3.hosts[0];

        this.s3sourceAuthManager =
            this._createAuthManager(this.sourceConfig.auth, sourceRole, log);
        this.s3destAuthManager =
            this._createAuthManager(this.destConfig.auth, targetRole, log);

        // Disable retries, use our own retry policy (mandatory for
        // putData route in order to fetch data again from source).

        this.S3source = new AWS.S3({
            endpoint: `${this.sourceConfig.s3.transport}://` +
                `${sourceS3.host}:${sourceS3.port}`,
            credentials:
            this.s3sourceAuthManager.getCredentials(),
            sslEnabled: true,
            s3ForcePathStyle: true,
            signatureVersion: 'v4',
            httpOptions: { agent: this.sourceHTTPAgent },
            maxRetries: 0,
        });
        this.backbeatSource = new BackbeatClient({
            endpoint: `${this.sourceConfig.s3.transport}://` +
                `${sourceS3.host}:${sourceS3.port}`,
            credentials:
            this.s3sourceAuthManager.getCredentials(),
            sslEnabled: true,
            httpOptions: { agent: this.sourceHTTPAgent },
            maxRetries: 0,
        });

        this.backbeatDest = new BackbeatClient({
            endpoint: `${this.destConfig.s3.transport}://` +
                `${destS3.host}:${destS3.port}`,
            credentials:
            this.s3destAuthManager.getCredentials(),
            sslEnabled: true,
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

        const _handleReplicationOutcome = err => {
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
                            error: err.description });
                return done();
            }
            log.debug('replication failed permanently for object, ' +
                      'updating replication status to FAILED',
                      { failMethod: err.method,
                        entry: sourceEntry.getLogInfo(),
                        error: err.description });
            return this._updateReplicationStatus(
                sourceEntry.toFailedEntry(),
                { log, reason: err.description }, done);
        };

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
                    this._putMetadata('target', destEntry, log, next);
                },
            ], _handleReplicationOutcome);
        }
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
                this._getAndPutData(sourceEntry, destEntry, log, next);
            },
            // update location, replication status and put metadata in
            // target bucket
            (location, next) => {
                destEntry.setLocation(location);
                this._putMetadata('target', destEntry, log, next);
            },
        ], _handleReplicationOutcome);
    }

    _updateReplicationStatusOnce(updatedSourceEntry, params, done) {
        const { log, reason } = params;

        const _doneUpdate = err => {
            if (err) {
                log.error('an error occurred when writing replication ' +
                          'status',
                          { entry: updatedSourceEntry.getLogInfo(),
                            replicationStatus:
                            updatedSourceEntry.getReplicationStatus() });
                return done(err);
            }
            log.info('replication status updated',
                     { entry: updatedSourceEntry.getLogInfo(),
                       replicationStatus:
                       updatedSourceEntry.getReplicationStatus(),
                       reason });
            return done();
        };

        if (this.backbeatSource !== null) {
            return this._putMetadata(
                'source', updatedSourceEntry, log, _doneUpdate);
        }
        log.info('replication status update skipped',
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
            concurrency: 1, // replication has to process entries in
                            // order, so one at a time
            queueProcessor: this.processKafkaEntry.bind(this),
        });
        consumer.on('error', () => {});
        consumer.subscribe();

        this.logger.info('queue processor is ready to consume ' +
                         'replication entries');
    }
}

module.exports = QueueProcessor;
