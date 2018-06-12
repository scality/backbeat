const async = require('async');
const http = require('http');
// const bucketclient = require('bucketclient');
const BackbeatClient = require('../clients/BackbeatClient');
const Logger = require('werelogs').Logger;
// const MetadataWrapper = require('arsenal').storage.metadata.MetadataWrapper;

const config = require('../../conf/Config');
const qpConfig = config.queuePopulator;
console.log('qp config from config.queuePopulator', qpConfig);
const s3Config = config.s3;
const auth = config.auth;
const attachReqUids =
    require('../../extensions/replication/utils/attachReqUids');
const constants = require('../../constants');
const RaftLogEntry = require('../../extensions/utils/RaftLogEntry');
const { StaticFileAccountCredentials } =
    require('../credentials/AccountCredentials');

class IngestionProducer {
    /**
     * Create an IngestionProducer class that helps create a snapshot of
     * pre-existing RING backend
     *
     * @constructor
     * @param {object} sourceConfig - source config with value for
     *                                bucketdBootstrap
     */
    constructor(sourceConfig) {
        this.log = new Logger('Backbeat:IngestionProducer');
        this.ringParams = {
            bucketdBootstrap: [`${sourceConfig.host}:${sourceConfig.port}`],
            bucketdLog: undefined,
        };
        this.qpConfig = qpConfig;
        console.log('qpConfig in constructor', qpConfig);
        this.auth = auth;
        this.s3source = s3Config;
        this.bucketPrefix = sourceConfig.prefix ? sourceConfig.prefix :
            sourceConfig.name;
        // this.ringReader = new MetadataWrapper('scality', this.ringParams,
        //     bucketclient, this.log);
        this.requestLogger = this.log.newRequestLogger();
        this.createEntry = new RaftLogEntry();
        this.resLog = [];
        this.ringReader = null;
        this.s3sourceCredentials = null;
        this.sourceHTTPAgent = new http.Agent({ keepAlive: true });
        // TODO: currently only supporting account types
        this.s3sourceCredentials =
            new StaticFileAccountCredentials(this.auth, this.log);
        console.log('this s3source', this.s3source);
        console.log('this qpConfig', this.qpConfig.transport);
        this.ringReader = new BackbeatClient({
            endpoint: `http://` +
                `${this.s3source.host}:${this.s3source.port}`,
            credentials: this.s3sourceCredentials,
            sslEnabled: this.qpConfig.transport === 'https',
            httpOptions: { agent: this.sourceHTTPAgent, timeout: 0 },
            maxRetries: 0,
        });
    }

    snapshot(raftId, done) {
        console.log('we have called snapshot');
        async.waterfall([
            next => this._getBuckets(raftId, next),
            (bucketList, next) => this._getBucketMd(bucketList, next),
            (bucketList, next) => this._getBucketObjects(bucketList, next),
            (bucketList, next) =>
                this._getBucketObjectsMetadata(bucketList, next),
        ], err => done(err, this.resLog));
    }

    _parseBucketName(bucketKey) {
        return bucketKey.split(constants.splitter)[1];
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
        console.log('this.ringReader.getRaftBuckets', this.ringReader.getRaftBuckets);
        const req = this.ringReader.getRaftBuckets({
            LogId: '1'
        });

        attachReqUids(req, this.requestLogger);
        req.send((err, data) => {
            if (err) {
                console.log('err', err);
                this.log.error('error getting list of buckets', {
                    method: 'IngestionProducer:getBuckets', err });
                return done(err);
            }
            console.log('THIS IS THE RES', JSON.parse(data));
            return done(null, JSON.parse(data));
        });
    }

    /**
     * Get the metadata for each bucket, format and send the info to kafka
     * so that it can be ingested into MongoDB
     *
     * @param {object} bucketList - list of buckets
     * @param {function} done - callback function
     * @return {object} list of buckets
     */
    _getBucketMd(bucketList, done) {
        if (bucketList.length < 1) {
            return done(null, null);
        }
        return async.eachLimit(bucketList, 10, (bucket, cb) => {
            const skipBucketMap = bucket === constants.usersBucket
                || bucket === constants.metastore;
            if (skipBucketMap) {
                return cb();
            }
            return async.waterfall([
                next => this.ringReader.getBucket(bucket,
                    this.requestLogger, (err, res) => {
                        if (err) {
                            this.log.error('error getting bucket', {
                                bucket,
                                method: 'IngestionProducer:getBucketMd',
                                err,
                            });
                            return next(err);
                        }
                        const bucketMdObj =
                            this.createEntry.createPutBucketMdEntry(res,
                                this.bucketPrefix);
                        this.resLog.push(bucketMdObj);
                        return next(null, res);
                    }),
                (res, next) => {
                    const bucketObj =
                        this.createEntry.createPutBucketEntry(bucket,
                            res, this.bucketPrefix);
                    this.resLog.push(bucketObj);
                    return next();
                },
            ], cb);
        }, err => done(err, bucketList));
    }

    /**
     * get the list of objects for each bucket
     *
     * @param {object} bucketList - list of buckets
     * @param {function} done - callback function
     * @return {object} list of buckets and list of objects for each bucket
     */
    _getBucketObjects(bucketList, done) {
        if (!bucketList) {
            return done(null, null);
        }
        return async.mapLimit(bucketList, 10, (bucketInfo, cb) => {
            if (bucketInfo === constants.usersBucket ||
            bucketInfo === constants.metastoreBucket) {
                return cb();
            }
            // const bucketName = this._parseBucketName(bucketInfo.key);
            return this.ringReader.listObject(bucketInfo, {},
            this.requestLogger, (err, res) => {
                if (err) {
                    this.log.error('error listing object from bucket', {
                        method: 'IngestionProducer:getBucketObjects',
                        bucketName: bucketInfo,
                        err,
                    });
                    return done(err);
                }
                return cb(null, { bucket: bucketInfo, objects: res.Contents });
            });
        }, (err, buckets) => done(err, buckets));
    }

    /**
     * get metadata for all objects, and send the info to kafka
     *
     * @param {object} bucketObjectList - list of buckets and list of objects
     * for each bucket
     * @param {function} done - callback function
     * @return {undefined}
     */
    _getBucketObjectsMetadata(bucketObjectList, done) {
        if (!bucketObjectList) {
            return done(null, null);
        }
        return async.mapSeries(bucketObjectList, (bucket, cb) => {
            if (!bucket) {
                return cb();
            }
            const bucketName = bucket.bucket;
            return async.mapLimit(bucket.objects, 10, (object, cb) => {
                const objectKey = object.key;
                return this.ringReader.getObjectMD(bucketName, objectKey, {},
                this.requestLogger, (err, res) => {
                    if (err) {
                        this.log.error('error getting object metadata', {
                            method:
                                'IngestionProducer:getBucketObjectsMetadata',
                            bucketName,
                            objectKey,
                            err,
                        });
                        return cb(err);
                    }
                    return cb(null, { res, objectKey, bucketName });
                });
            }, (err, objectMDs) => {
                if (err) {
                    return cb(err);
                }
                return this._createAndPushEntry(objectMDs, cb);
            });
        }, err => done(err));
    }

    _createAndPushEntry(objectMds, done) {
        if (objectMds.length > 0) {
            return async.eachLimit(objectMds, 10, (objectMd, cb) => {
                const objectMdEntry =
                    this.createEntry.createPutEntry(objectMd,
                        this.bucketPrefix);
                this.resLog.push(objectMdEntry);
                return cb();
            }, err => {
                if (err) {
                    this.log.error('error sending objectMd to kafka', {
                        err,
                    });
                }
                return done(err);
            });
        }
        return done();
    }
}

module.exports = IngestionProducer;
