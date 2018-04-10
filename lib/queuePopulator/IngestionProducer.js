const async = require('async');
const bucketclient = require('bucketclient');
const Logger = require('werelogs').Logger;
const MetadataWrapper = require('arsenal').storage.metadata.MetadataWrapper;

const constants = require('../../constants');
const RaftLogEntry = require('../../extensions/utils/RaftLogEntry');

// FIXME: this is a hardcoded value
//
// const ringParams = {
//     bucketdBootstrap: [config.ingestion.sources.source1.host],
//     bucketdLog: undefined,
// };

class IngestionProducer {
    /**
     * Create an IngestionProducer class that helps create a snapshot of
     * pre-existing RING backend
     *
     * @constructor
     * @param {object} ringParams - params to use metadatawrapper
     */
    constructor(ringParams) {
        this.log = new Logger('Backbeat:IngestionProducer');
        this.ringReader = new MetadataWrapper('scality', ringParams,
            bucketclient, this.log);
        this.requestLogger = this.log.newRequestLogger();
        this.createEntry = new RaftLogEntry();
        this.resLog = [];
    }

    snapshot(raftId, done) {
        async.waterfall([
            next => this._getBuckets(raftId, (err, res) => {
                console.log('get list of buckets');
                console.log('bucket list err, res', err, res);
                console.log('typeof res', typeof res);
                return next(err, res);
            }),
            (bucketList, next) => this._getBucketMd(bucketList, (err, res) => {
                console.log('getting bucket metadata');
                console.log('bucket metadata err res', err, res);
                return next(err, res);
            }),
            (bucketList, next) => this._getBucketObjects(bucketList, (err, res) => {
                console.log('getting list objects');
                console.log('object list err res', err, res);
                return next(err, res);
            }),
            (bucketList, next) =>
                this._getBucketObjectsMetadata(bucketList, (err, res) => {
                    console.log('getting object metadata');
                    console.log('object metadata err res', err, res);
                    return next(err, res);
                }),
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
            console.log('THIS IS THE RESULT FROM GETTING BUCKETS', raftId);
            return done(null, res);
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
        console.log('getting bucketMd');
        return async.eachLimit(bucketList, 10, (bucket, cb) => {
            if (bucket === constants.usersBucket || bucket === constants.metastore) {
                console.log('matched stored names', bucket);
                return cb();
            }
            console.log('async waterfall');
            return async.waterfall([
                next => {
                    console.log('creating entry');
                    const bucketObj =
                        this.createEntry.createPutBucketEntry(bucket);
                    this.resLog.push(bucketObj);
                    return next();
                },
                next => {
                    console.log('attempting to get bucket from ring reader', bucket);
                    // const bucketName = this._parseBucketName(bucket.key);
                    return this.ringReader.getBucket(bucket,
                    this.requestLogger, (err, res) => {
                        console.log('GETTING BUCKET');
                        if (err) {
                            this.log.error('error getting bucket', {
                                bucket,
                                method: 'IngestionProducer:getBucketMd',
                                err,
                            });
                            return next(err);
                        }
                        const bucketMdObj =
                            this.createEntry.createPutBucketMdEntry(res);
                        this.resLog.push(bucketMdObj);
                        return next();
                    });
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
        console.log('BUCKETLIST', bucketList);
        return async.map(bucketList, (bucketInfo, cb) => {
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
        console.log('BUCKETOBJECTLIST', bucketObjectList);
        return async.mapLimit(bucketObjectList, 1, (bucket, cb) => {
            if (!bucket) {
                return cb();
            }
            console.log(bucket);
            const bucketName = bucket.bucket;
            console.log('bucket.bucket', bucket.bucket);
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
                    this.createEntry.createPutEntry(objectMd);
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
