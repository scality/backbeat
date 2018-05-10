const async = require('async');
const bucketclient = require('bucketclient');
const Logger = require('werelogs').Logger;
const MetadataWrapper = require('arsenal').storage.metadata.MetadataWrapper;

const constants = require('../../constants');
const RaftLogEntry = require('../../extensions/utils/RaftLogEntry');

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
            bucketdBootstrap: [sourceConfig.host],
            bucketdLog: undefined,
        };
        this.bucketPrefix = sourceConfig.prefix ? sourceConfig.prefix :
            sourceConfig.name;
        this.ringReader = new MetadataWrapper('scality', this.ringParams,
            bucketclient, this.log);
        this.requestLogger = this.log.newRequestLogger();
        this.createEntry = new RaftLogEntry();
        this.resLog = [];
    }

    snapshot(raftId, done) {
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
        return this.ringReader.getRaftBuckets(raftId, this.requestLogger,
        (err, res) => {
            if (err) {
                this.log.error('error getting list of buckets', {
                    method: 'IngestionProducer:getBuckets', err });
                return done(err);
            }
            return done(null, JSON.parse(res));
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
                next => {
                    const bucketObj =
                        this.createEntry.createPutBucketEntry(bucket,
                            this.bucketPrefix);
                    this.resLog.push(bucketObj);
                    return next();
                },
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
                        return next();
                    }),
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
