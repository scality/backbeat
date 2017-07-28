'use strict'; // eslint-disable-line

const async = require('async');
const assert = require('assert');
const AWS = require('aws-sdk');
const http = require('http');

const Logger = require('werelogs').Logger;

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const VaultClient = require('vaultclient').Client;

const authdata = require('../../../conf/authdata.json');

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const QueueEntry = require('../utils/QueueEntry');
const CredentialsManager = require('../../../credentials/CredentialsManager');

const LIMIT = 10;

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
        const { host, port } = authConfig.vault;
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
     * @param {string} zkConfig.endpoint - zookeeper endpoint string
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
     * @param {Logger} logConfig - logging configuration object
     * @param {String} logConfig.logLevel - logging level
     * @param {Logger} logConfig.dumpLevel - dump level
     */
    constructor(zkConfig, sourceConfig, destConfig, repConfig, logConfig) {
        this.zkConfig = zkConfig;
        this.sourceConfig = sourceConfig;
        this.destConfig = destConfig;
        this.repConfig = repConfig;
        this.logConfig = logConfig;

        this.logger = new Logger('Backbeat:Replication:QueueProcessor',
                                 { level: logConfig.logLevel,
                                   dump: logConfig.dumpLevel });

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

    _getBucketReplicationRoles(entry, log, cb) {
        log.debug('getting bucket replication',
                  { entry: entry.getLogInfo() });
        const entryRolesString = entry.getReplicationRoles();
        let entryRoles;
        if (entryRolesString !== undefined) {
            entryRoles = entryRolesString.split(',');
        }
        if (entryRoles === undefined || entryRoles.length !== 2) {
            log.error('expecting two roles separated by a ' +
                      'comma in replication configuration',
                      { entry: entry.getLogInfo(),
                        origin: this.sourceConfig.s3 });
            return cb(errors.InternalError);
        }
        this._setupClients(entryRoles[0], entryRoles[1], log);
        return this.S3source.getBucketReplication(
            { Bucket: entry.getBucket() }, (err, data) => {
                if (err) {
                    log.error('error getting replication ' +
                              'configuration from S3',
                              { entry: entry.getLogInfo(),
                                origin: this.sourceConfig.s3,
                                error: err.message,
                                errorStack: err.stack,
                                httpStatus: err.statusCode });
                    return cb(err);
                }
                const roles = data.ReplicationConfiguration.Role.split(',');
                if (roles.length !== 2) {
                    log.error('expecting two roles separated by a ' +
                              'comma in replication configuration',
                              { entry: entry.getLogInfo(),
                                origin: this.sourceConfig.s3 });
                    return cb(errors.InternalError);
                }
                return cb(null, roles[0], roles[1]);
            });
    }

    _processRoles(sourceEntry, sourceRole, destEntry, targetRole, log, cb) {
        log.debug('processing role for destination',
                  { entry: destEntry.getLogInfo(),
                    role: targetRole });
        const targetAccountId = _extractAccountIdFromRole(targetRole);
        this.s3destAuthManager.lookupAccountAttributes(
            targetAccountId, (err, accountAttr) => {
                if (err) {
                    return cb(err);
                }
                log.debug('setting owner info in target metadata',
                          { entry: destEntry.getLogInfo(),
                            accountAttr });
                destEntry.setOwner(accountAttr.canonicalID,
                                   accountAttr.displayName);
                return cb(null, accountAttr);
            });
    }

    _getAndPutData(sourceEntry, destEntry, log, cb) {
        log.debug('getting data', { entry: sourceEntry.getLogInfo() });
        const locations = sourceEntry.getLocation();
        return async.mapLimit(locations, LIMIT, (part, done) => {
            const doneOnce = jsutil.once(done);
            if (sourceEntry.getDataStoreETag(part) === undefined) {
                log.error('cannot replicate object without dataStoreETag ' +
                          'property',
                          { entry: destEntry.getLogInfo() });
                return doneOnce(errors.InvalidObjectState);
            }
            const partNumber = sourceEntry.getPartNumber(part);
            const req = this.S3source.getObject({
                Bucket: sourceEntry.getBucket(),
                Key: sourceEntry.getObjectKey(),
                VersionId: sourceEntry.getEncodedVersionId(),
                PartNumber: partNumber,
            });
            const incomingMsg = req.createReadStream();
            req.on('error', err => {
                log.error('error response getting data from S3',
                          { entry: sourceEntry.getLogInfo(),
                            origin: this.sourceConfig.s3,
                            error: err, httpStatus: err.statusCode });
                return doneOnce(err);
            });
            log.debug('putting data', { entry: destEntry.getLogInfo() });
            incomingMsg.on('error', err => {
                log.error('error from source S3 server',
                          { entry: destEntry.getLogInfo(), error: err });
                return doneOnce(err);
            });
            return this.backbeatDest.putData({
                Bucket: destEntry.getBucket(),
                Key: destEntry.getObjectKey(),
                CanonicalID: destEntry.getOwnerCanonicalId(),
                ContentLength: destEntry.getPartSize(part),
                ContentMD5: destEntry.getPartETag(part),
                Body: incomingMsg,
            }, (err, data) => {
                if (err) {
                    log.error('error response from destination S3 server', {
                        method: 'QueueProcessor._getAndPutData',
                        entry: destEntry.getLogInfo(),
                        origin: this.destConfig.s3,
                        error: err,
                    });
                    return doneOnce(err);
                }
                return doneOnce(null,
                    destEntry.buildLocationKey(part, data.Location[0]));
            });
        }, cb);
    }

    _putMetaData(where, entry, log, cb) {
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
                log.error('error response from S3',
                          { entry: entry.getLogInfo(),
                            origin: this.destConfig.s3,
                            error: err.message,
                            errorStack: err.stack });
                return cbOnce(err);
            }
            return cbOnce(null, data);
        });
    }

    _setupClients(sourceRole, targetRole, log) {
        this.s3sourceAuthManager =
            this._createAuthManager(this.sourceConfig.auth, sourceRole, log);
        this.s3destAuthManager =
            this._createAuthManager(this.destConfig.auth, targetRole, log);
        this.S3source = new AWS.S3({
            endpoint: `${this.sourceConfig.s3.transport}://` +
                `${this.sourceConfig.s3.host}:${this.sourceConfig.s3.port}`,
            credentials:
            this.s3sourceAuthManager.getCredentials(),
            sslEnabled: true,
            s3ForcePathStyle: true,
            signatureVersion: 'v4',
            httpOptions: { agent: this.sourceHTTPAgent },
        });
        this.backbeatSource = new BackbeatClient({
            endpoint: `${this.sourceConfig.s3.transport}://` +
                `${this.sourceConfig.s3.host}:${this.sourceConfig.s3.port}`,
            credentials:
            this.s3sourceAuthManager.getCredentials(),
            sslEnabled: true,
            httpOptions: { agent: this.sourceHTTPAgent },
        });

        this.backbeatDest = new BackbeatClient({
            endpoint: `${this.destConfig.s3.transport}://` +
                `${this.destConfig.s3.host}:${this.destConfig.s3.port}`,
            credentials:
            this.s3destAuthManager.getCredentials(),
            sslEnabled: true,
            httpOptions: { agent: this.destHTTPAgent },
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
        const destEntry = sourceEntry.toReplicaEntry();

        log.debug('processing entry',
                  { entry: sourceEntry.getLogInfo() });

        const _doneProcessingCompletedEntry = err => {
            if (err) {
                log.error('an error occurred while writing ' +
                          'replication status to COMPLETED',
                          { entry: sourceEntry.getLogInfo() });
                return done(err);
            }
            log.info('entry replicated successfully',
                     { entry: sourceEntry.getLogInfo() });
            return done();
        };

        const _doneProcessingFailedEntry = err => {
            if (err) {
                log.error('an error occurred while writing ' +
                          'replication status to FAILED',
                          { entry: sourceEntry.getLogInfo() });
                return done(err);
            }
            log.info('replication status set to FAILED',
                     { entry: sourceEntry.getLogInfo() });
            return done();
        };

        const _writeReplicationStatus = err => {
            if (err) {
                log.warn('replication failed for object',
                         { entry: sourceEntry.getLogInfo(),
                           error: err });
                if (this.backbeatSource !== null) {
                    return this._putMetaData('source',
                                             sourceEntry.toFailedEntry(),
                                             log,
                                             _doneProcessingFailedEntry);
                }
                log.info('replication status update skipped',
                         { entry: sourceEntry.getLogInfo() });
                return done();
                // TODO: queue entry back in kafka for later retry
            }
            log.debug('replication succeeded for object, updating ' +
                      'source replication status to COMPLETED',
                      { entry: sourceEntry.getLogInfo() });
            return this._putMetaData('source',
                                     sourceEntry.toCompletedEntry(),
                                     log,
                                     _doneProcessingCompletedEntry);
        };

        if (sourceEntry.isDeleteMarker()) {
            return async.waterfall([
                next => {
                    this._getBucketReplicationRoles(sourceEntry, log, next);
                },
                (sourceRole, targetRole, next) => {
                    this._processRoles(sourceEntry, sourceRole,
                                       destEntry, targetRole, log, next);
                },
                // put metadata in target bucket
                (accountAttr, next) => {
                    // TODO check that bucket role matches role in metadata
                    this._putMetaData('target', destEntry, log, next);
                },
            ], _writeReplicationStatus);
        }
        return async.waterfall([
            // get data stream from source bucket
            next => {
                this._getBucketReplicationRoles(sourceEntry, log, next);
            },
            (sourceRole, targetRole, next) => {
                this._processRoles(sourceEntry, sourceRole,
                                   destEntry, targetRole, log, next);
            },
            // Get data from source bucket and put it on the target bucket
            (accountAttr, next) => {
                // TODO check that bucket role matches role in metadata
                this._getAndPutData(sourceEntry, destEntry, log, next);
            },
            // update location, replication status and put metadata in
            // target bucket
            (location, next) => {
                destEntry.setLocation(location);
                this._putMetaData('target', destEntry, log, next);
            },
        ], _writeReplicationStatus);
    }

    start() {
        const consumer = new BackbeatConsumer({
            zookeeper: this.zkConfig,
            log: this.logConfig,
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
