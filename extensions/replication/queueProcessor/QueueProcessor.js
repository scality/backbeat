'use strict'; // eslint-disable-line

const async = require('async');
const assert = require('assert');
const AWS = require('aws-sdk');

const Logger = require('werelogs').Logger;

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;

const authdata = require('../../../conf/authdata.json');

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const QueueEntry = require('../utils/QueueEntry');

class QueueProcessor {

    constructor(sourceConfig, destConfig, repConfig, logConfig) {
        this.sourceConfig = sourceConfig;
        this.destConfig = destConfig;
        this.repConfig = repConfig;
        this.logConfig = logConfig;

        this.logger = new Logger('Backbeat:Replication:QueueProcessor',
                                 { level: logConfig.logLevel,
                                   dump: logConfig.dumpLevel });
        this.log = this.logger.newRequestLogger();

        const s3sourceCredProvider =
                  this._setupCredentialProvider(sourceConfig.auth);
        const s3destCredProvider =
                  this._setupCredentialProvider(destConfig.auth);

        this.S3source = new AWS.S3({
            endpoint: `${sourceConfig.s3.transport}://` +
                `${sourceConfig.s3.host}:${sourceConfig.s3.port}`,
            credentialProvider: s3sourceCredProvider,
            sslEnabled: true,
            s3ForcePathStyle: true,
            signatureVersion: 'v4',
        });
        this.backbeatSource = new BackbeatClient({
            endpoint: `${sourceConfig.s3.transport}://` +
                `${sourceConfig.s3.host}:${sourceConfig.s3.port}`,
            credentialProvider: s3sourceCredProvider,
            sslEnabled: true,
        });

        this.backbeatDest = new BackbeatClient({
            endpoint: `${destConfig.s3.transport}://` +
                `${destConfig.s3.host}:${destConfig.s3.port}`,
            credentialProvider: s3destCredProvider,
            sslEnabled: true,
        });
    }

    _setupCredentialProvider(authConfig) {
        if (authConfig.type === 'remote') {
            throw Error('Vault authentication not yet implemented');
        } else {
            assert.strictEqual(authConfig.type, 'local');
            const userInfo = authdata.users.find(
                user => user.name === authConfig.user);
            if (userInfo === undefined) {
                throw Error(`No such user registered: ${authConfig.user}`);
            }
            return new AWS.CredentialProviderChain([
                new AWS.Credentials(userInfo.keys.access,
                                    userInfo.keys.secret),
            ]);
        }
    }

    _getBucketReplicationRole(entry, cb) {
        this.log.debug('getting bucket replication',
                       { entry: entry.getLogInfo() });
        this.S3source.getBucketReplication(
            { Bucket: entry.getBucket() }, (err, data) => {
                if (err) {
                    this.log.error('error response getting replication ' +
                                   'configuration from S3',
                                   { entry: entry.getLogInfo(),
                                     origin: this.sourceConfig.s3,
                                     error: err,
                                     httpStatus: err.statusCode });
                    return cb(err);
                }
                return cb(null, data.ReplicationConfiguration.Role);
            });
    }

    _getData(entry, cb) {
        this.log.debug('getting data', { entry: entry.getLogInfo() });
        const req = this.S3source.getObject({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            VersionId: entry.getEncodedVersionId() });
        const incomingMsg = req.createReadStream();
        req.on('error', err => {
            this.log.error('error response getting data from S3',
                           { entry: entry.getLogInfo(),
                             origin: this.sourceConfig.s3,
                             error: err, httpStatus: err.statusCode });
            incomingMsg.emit('error', err);
        });
        return cb(null, incomingMsg);
    }

    _putData(entry, sourceStream, cb) {
        this.log.debug('putting data', { entry: entry.getLogInfo() });
        const cbOnce = jsutil.once(cb);
        sourceStream.on('error', err => {
            this.log.error('error from source S3 server',
                           { entry: entry.getLogInfo(), error: err });
            return cbOnce(err);
        });
        this.backbeatDest.putData({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            ContentLength: entry.getContentLength(),
            ContentMD5: entry.getContentMD5(),
            Body: sourceStream,
        }, (err, data) => {
            if (err) {
                this.log.error('error response from destination S3 server',
                               { entry: entry.getLogInfo(),
                                 origin: this.destConfig.s3,
                                 error: err });
                return cbOnce(err);
            }
            return cbOnce(null, data.Locations);
        });
    }

    _putMetaData(where, entry, cb) {
        this.log.debug('putting metadata',
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
                this.log.error('error response from S3',
                               { entry: entry.getLogInfo(),
                                 origin: this.destConfig.s3,
                                 error: err });
                return cbOnce(err);
            }
            return cbOnce(null, data);
        });
    }

    _processEntry(kafkaEntry, done) {
        const sourceEntry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        const destEntry = sourceEntry.toReplicaEntry();

        if (sourceEntry.error) {
            this.log.error('error processing source entry',
                           { error: sourceEntry.error });
            return process.nextTick(() => done(errors.InternalError));
        }
        this.log.debug('processing entry',
                       { entry: sourceEntry.getLogInfo() });

        const _doneProcessingEntry = err => {
            if (err) {
                this.log.error('an error occurred while processing entry',
                               { entry: sourceEntry.getLogInfo() });
                return done(err);
            }
            this.log.info('entry replicated successfully',
                          { entry: sourceEntry.getLogInfo() });
            return done(null);
        };

        const _writeReplicationStatus = err => {
            if (err) {
                this.log.warn('replication failed for object',
                              { entry: sourceEntry.getLogInfo(),
                                error: err });
                this._putMetaData('source', sourceEntry.toFailedEntry(),
                                  _doneProcessingEntry);
                // TODO: queue entry back in kafka for later retry
            } else {
                this.log.debug('replication succeeded for object, updating ' +
                               'source replication status to COMPLETED',
                               { entry: sourceEntry.getLogInfo() });
                this._putMetaData('source', sourceEntry.toCompletedEntry(),
                                  _doneProcessingEntry);
            }
        };

        if (sourceEntry.isDeleteMarker()) {
            return async.waterfall([
                next => {
                    this._getBucketReplicationRole(sourceEntry, next);
                },
                // put metadata in target bucket
                (role, next) => {
                    // TODO check that bucket role matches role in metadata
                    this._putMetaData('target', destEntry, next);
                },
            ], _writeReplicationStatus);
        }
        return async.waterfall([
            // get data stream from source bucket
            next => {
                this._getBucketReplicationRole(sourceEntry, next);
            },
            (role, next) => {
                // TODO check that bucket role matches role in metadata
                this._getData(sourceEntry, next);
            },
            // put data in target bucket
            (stream, next) => {
                this._putData(destEntry, stream, next);
            },
            // update location, replication status and put metadata in
            // target bucket
            (locations, next) => {
                destEntry.setLocations(locations);
                this._putMetaData('target', destEntry, next);
            },
        ], _writeReplicationStatus);
    }

    start() {
        const consumer = new BackbeatConsumer({
            topic: this.repConfig.topic,
            groupId: this.repConfig.groupId,
            concurrency: 1, // replication has to process entries in
                            // order, so one at a time
            queueProcessor: this._processEntry.bind(this),
            log: this.logConfig,
        });
        consumer.on('error', () => {});
        consumer.subscribe();

        this.log.info('queue processor is ready to consume ' +
                      'replication entries');
    }
}

module.exports = QueueProcessor;
