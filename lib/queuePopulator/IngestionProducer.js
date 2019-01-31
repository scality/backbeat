const async = require('async');
const AWS = require('aws-sdk');
const http = require('http');
const https = require('https');
const jsonStream = require('JSONStream');
const stream = require('stream');
const Logger = require('werelogs').Logger;
const { constants, errors } = require('arsenal');

const { decode } = require('arsenal').versioning.VersionID;
const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;

const BackbeatClient = require('../clients/BackbeatClient');
const { attachReqUids } = require('../clients/utils');
const RaftLogEntry = require('../models/RaftLogEntry');

class ListRecordStream extends stream.Transform {
    constructor(logger) {
        super({ objectMode: true });
        this.logger = logger;
    }

    _transform(itemObj, encoding, callback) {
        itemObj.entries.forEach(entry => {
            // eslint-disable-next-line no-param-reassign
            entry.type = entry.type || 'put';
        });
        this.push(itemObj);
        callback();
    }
}

class IngestionProducer {
    /**
     * Create an IngestionProducer class that helps create a snapshot of
     * pre-existing RING backend
     *
     * @constructor
     * @param {object} sourceConfig - source config (also called bucketdConfig)
     * @param {object} qpConfig - queuePopulator config object with value for
     *                            sslEnabled
     * @param {object} s3Config - S3 config object with value for host and port
     *                            of BackbeatClient endpoint
     */
    constructor(sourceConfig, qpConfig, s3Config) {
        this.log = new Logger('Backbeat:IngestionProducer');
        this.qpConfig = qpConfig;
        this.s3source = s3Config;
        this._targetZenkoBucket = sourceConfig.name;
        this.requestLogger = this.log.newRequestLogger();
        this.createEntry = new RaftLogEntry();

        this._ringReader = null;
        this._s3Client = null;
        this._setupClients(sourceConfig);
    }

    /**
     * Helper method to create a new HTTP(S) agent
     * @param {string} protocol - "https" || "http"
     * @return {http.Agent|https.Agent} new http or https Agent
     */
    _createHTTPAgent(protocol) {
        const params = { keepAlive: true };
        if (protocol === 'https') {
            return new https.Agent(params);
        }
        return new http.Agent(params);
    }

    /**
     * Setup internal clients: `this._ringReader`, `this._s3Client`
     * @param {object} sourceConfig - source config (also called bucketdConfig)
     * @return {undefined}
     */
    _setupClients(sourceConfig) {
        const { https, host, port } = sourceConfig;
        const protocol = https ? 'https' : 'http';
        const endpoint = `${protocol}://${host}:${port}`;
        const s3sourceCredentials = new AWS.Credentials({
            accessKeyId: sourceConfig.auth.accessKey,
            secretAccessKey: sourceConfig.auth.secretKey,
        });

        this._ringReader = new BackbeatClient({
            endpoint,
            credentials: s3sourceCredentials,
            sslEnabled: protocol === 'https',
            httpOptions: {
                agent: this._createHTTPAgent(protocol),
                timeout: 0,
            },
            maxRetries: 0,
        });
        const s3endpoint = process.env.CI === 'true' ?
                          `${protocol}://${host}:8000` :
                          endpoint;
        this._s3Client = new AWS.S3({
            endpoint: s3endpoint,
            credentials: s3sourceCredentials,
            sslEnabled: protocol === 'https',
            s3ForcePathStyle: true,
            signatureVersion: 'v4',
            httpOptions: {
                agent: this._createHTTPAgent(protocol),
                timeout: 0,
            },
            maxRetries: 0,
        });
    }

    /**
     * Find the raft session that the bucket exists on
     * @param {string} bucketName - name of sourcebucket that needs logs
     * @param {function} done - callback function
     * @return {number} the raftId that has logs for the bucket
     */
    getRaftId(bucketName, done) {
        const req = this._ringReader.getRaftId({
            Bucket: bucketName,
        });

        attachReqUids(req, this.requestLogger);
        req.send((err, data) => {
            if (err) {
                this.log.error(`could not find bucket ${bucketName} in any` +
                ' raft session', {
                    method: 'IngestionProducer.getRaftId',
                    bucketName,
                    error: err,
                });
                return done(err);
            } else if (data && data[0]) {
                return done(null, data[0]);
            }
            this.log.error(`empty response for raftid of ${bucketName}`,
            { method: 'getRaftId', bucketName });
            return done(errors.InternalError);
        });
    }

    /**
     * generate a listing of all current objects that exists on the source
     * bucket, including the bucket MD to create the correct entries
     * @param {string} bucketName - name of source bucket
     * @param {function} done - callback(error, resLog)
     * @return {undefined}
     */
    snapshot(bucketName, done) {
        async.waterfall([
            next => this._getObjectVersionsList(bucketName, next),
            (versionList, next) =>
                this._getBucketObjectsMetadata(bucketName, versionList, next),
        ], done);
    }

    getRaftLog(raftId, begin, limit, targetLeader, done) {
        const recordStream = new ListRecordStream(this.log);
        recordStream.on('error', err => {
            if (err.statusCode === 404) {
                // no such raft session, log and ignore
                this.log.warn('raft session does not exist',
                    { raftId: this.raftId, method:
                    'IngestionProducer.getRaftLog' });
                return done(null, { info: { start: null,
                    end: null } });
            }
            if (err.statusCode === 416) {
                // requested range not satisfiable
                this.log.debug('no new log records to ' +
                    'process', {
                        raftId: this.raftId,
                        method: 'IngestionProducer.getRaftLog',
                    });
                return done(null, { info: { start: null,
                    end: null } });
            }
            this.log.error('error receiving raft log',
            { error: err.message });
            return done(errors.InternalError);
        });
        const req = this._ringReader.getRaftLog({
            LogId: raftId.toString(),
            Begin: begin,
            Limit: limit,
            TargetLeader: targetLeader,
        });
        attachReqUids(req, this.requestLogger);
        const readStream = req.createReadStream();
        const jsonResponse = readStream.pipe(jsonStream.parse('log.*'));
        jsonResponse.pipe(recordStream);
        readStream.on('error', err => recordStream.emit('error', err));
        jsonResponse
            .on('header', header => {
                recordStream.removeAllListeners('error');
                return done(null, {
                    info: header.info,
                    log: recordStream,
                });
            })
            .on('error', err => recordStream.emit('error', err));
        return undefined;
    }

    /**
     * Get the list of buckets using the usersBucket
     * Each bucket is stored as a key in the usersBucket
     *
     * @param {number} raftId - raft session id value
     * @param {function} done - callback function
     * @return {Object} list of keys that correspond to list of buckets
     */
    _getBuckets(raftId, done) {
        const req = this._ringReader.getRaftBuckets({
            LogId: raftId,
        });

        attachReqUids(req, this.requestLogger);
        req.send((err, data) => {
            if (err) {
                this.log.error('error getting list of buckets', {
                    method: 'IngestionProducer._getBuckets', err });
                return done(err);
            }
            const bucketList = Object.keys(data).map(index => data[index]);
            return done(null, bucketList);
        });
    }

    /**
     * Get the list of object versions for a bucket
     *
     * @param {string} bucket - bucket name
     * @param {function} done - callback function
     * @return {object} list of objects for each bucket, including a duplicate
     *   entry for IsLatest versions
     */
    _getObjectVersionsList(bucket, done) {
        if (bucket === constants.usersBucket ||
            bucket === constants.metastoreBucket) {
            return done();
        }
        // TODO: VersionIdMarker && KeyMarker
        const params = {
            Bucket: bucket,
        };
        // TODO: For testing, I can set MaxKeys
        const req = this._s3Client.listObjectVersions(params);
        attachReqUids(req, this.requestLogger);
        return req.send((err, data) => {
            if (err) {
                this.log.error('error getting list of object versions', {
                    method: 'IngestionProducer._getObjectVersionsList',
                    error: err,
                    bucket,
                });
                return done(err);
            }
            const {
                IsTruncated,
                // NextKeyMarker,
                // NextVersionIdMarker,
                Versions,
                DeleteMarkers
            } = data;

            if (IsTruncated) {
                // need NextKeyMarker
                // need NextVersionIdMarker
            }

            const versionList = [...Versions, ...DeleteMarkers];
            // find all IsLatest versions since we need these as separate
            // metadata entries
            const latestVersions = versionList.filter(v => v.IsLatest)
                .map(v => Object.assign({}, v, { isLatest: true }));

            return done(null, versionList.concat(latestVersions));
        });
    }

    /**
     * Get metadata for all objects, and send the info to kafka
     *
     * @param {string} bucket - bucket name
     * @param {array} versionList - list of object versions (including delete
     *   markers)
     * @param {function} done - callback function
     * @return {undefined}
     */
    _getBucketObjectsMetadata(bucket, versionList, done) {
        if (versionList.length === 0) {
            return done();
        }

        return async.mapLimit(versionList, 10, (version, cb) => {
            const { Key, VersionId, isLatest } = version;

            let objectKey;
            if (isLatest) {
                objectKey = Key;
            } else {
                objectKey = `${Key}${VID_SEP}${decode(VersionId)}`;
            }

            const req = this._ringReader.getObjectMetadata({
                Bucket: bucket,
                Key: objectKey,
            });
            attachReqUids(req, this.requestLogger);
            req.send((err, data) => {
                if (err) {
                    this.log.error('error getting metadata for object', {
                        method: 'IngestionProducer._getBucketObjectsMetadata',
                        error: err
                    });
                    return cb(err);
                }
                return cb(null, {
                    res: data,
                    objectKey,
                    bucketName: bucket,
                });
            });
        }, (err, objectMDs) => {
            if (err) {
                return done(err);
            }
            return this._createAndPushEntry(objectMDs, done);
        });
    }

    _createAndPushEntry(objectMds, done) {
        if (objectMds.length > 0) {
            return async.mapLimit(objectMds, 10, (objectMd, cb) => {
                const objectMdEntry = this.createEntry.createPutEntry(objectMd,
                        this._targetZenkoBucket);
                return cb(null, objectMdEntry);
            }, (err, entries) => {
                if (err) {
                    this.log.error('error sending objectMd to kafka', {
                        method: 'IngestionProducer._createAndPushEntry',
                        error: err,
                    });
                }
                return done(err, entries);
            });
        }
        return done(null, []);
    }
}

module.exports = IngestionProducer;
