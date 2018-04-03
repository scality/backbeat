const async = require('async');
const bucketclient = require('bucketclient');
const Logger = require('werelogs').Logger;
const MetadataWrapper = require('arsenal').storage.metadata.MetadataWrapper;

const constants = require('../../constants');
const RaftLogEntry = require('../../extensions/utils/RaftLogEntry');

// FIXME: this is a hardcoded value
const ringParams = {
    bucketdBootstrap: ['54.202.35.127'],
    bucketdLog: undefined,
};

class IngestionProducer {
    /**
     * Create a queue populator object to populate various kafka
     * queues from the metadata log
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {String} zkConfig.connectionString - zookeeper
     *   connection string as "host:port[/chroot]"
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - kafka hosts list
     * as "host:port[,host:port...]"
     * @param {Object} qpConfig - queue populator configuration
     * @param {String} qpConfig.zookeeperPath - sub-path to use for
     *   storing populator state in zookeeper
     * @param {String} qpConfig.logSource - type of source
     *   log: "bucketd" (raft log) or "dmd" (bucketfile)
     * @param {Object} [qpConfig.bucketd] - bucketd source
     *   configuration (mandatory if logSource is "bucket")
     * @param {Object} [qpConfig.dmd] - dmd source
     *   configuration (mandatory if logSource is "dmd")
     * @param {Object} mConfig - metrics configuration object
     * @param {string} mConfig.topic - metrics topic
     * @param {Object} rConfig - redis configuration object
     * @param {Object} extConfigs - configuration of extensions: keys
     *   are extension names and values are extension's config object.
     */
    constructor() {
        this.log = new Logger('Backbeat:IngestionProducer');
        this.ringReader = new MetadataWrapper('scality', ringParams,
            bucketclient, this.log);
        this.requestLogger = this.log.newRequestLogger();
        this.createEntry = new RaftLogEntry();
        this.resLog = [];
    }

    snapshot(done) {
        async.waterfall([
            next => this._getBuckets(next),
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
     * @param {function} done - callback function
     * @return {Object} list of keys that correspond to list of buckets
     */
    _getBuckets(done) {
        return this.ringReader.listObject(constants.usersBucket, {},
        this.requestLogger, (err, res) => {
            if (err) {
                this.log.error('error getting list of buckets', {
                    method: 'IngestionProducer:getBuckets', err });
                return done(err);
            }
            return done(null, res.Contents);
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
        return async.eachLimit(bucketList, 10, (bucket, cb) => {
            async.waterfall([
                next => {
                    const bucketObj =
                        this.createEntry.createPutBucketEntry(bucket);
                    this.resLog.push(bucketObj);
                    return next();
                },
                next => {
                    const bucketName = this._parseBucketName(bucket.key);
                    return this.ringReader.getBucket(bucketName,
                    this.requestLogger, (err, res) => {
                        if (err) {
                            this.log.error('error getting bucket', {
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
        return async.map(bucketList, (bucketInfo, cb) => {
            const bucketName = this._parseBucketName(bucketInfo.key);
            this.ringReader.listObject(bucketName, {}, this.requestLogger,
            (err, res) => {
                if (err) {
                    this.log.error('error listing object from bucket', {
                        method: 'IngestionProducer:getBucketObjects',
                        bucketName,
                        err,
                    });
                    return done(err);
                }
                return cb(null, { bucket: bucketName, objects: res.Contents });
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
        return async.mapLimit(bucketObjectList, 1, (bucket, cb) => {
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
