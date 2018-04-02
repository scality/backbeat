const async = require('async');
const bucketclient = require('bucketclient');
const Logger = require('werelogs').Logger;
const MetadataWrapper = require('arsenal').storage.metadata.MetadataWrapper;

const BackbeatProducer = require('../BackbeatProducer');
const QueuePopulator = require('./QueuePopulator');
const constants = require('../../constants');
const RaftLogEntry = require('../../extensions/utils/RaftLogEntry');

// FIXME: this is a hardcoded value
const ringParams = {
    bucketdBootstrap: ['54.202.35.127'],
    bucketdLog: undefined,
};

class IngestionProducer extends QueuePopulator {
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
    constructor(zkConfig, kafkaConfig, qpConfig, mConfig, rConfig,
                extConfigs) {
        super(zkConfig, kafkaConfig, qpConfig, mConfig, rConfig, extConfigs);

        this.log = new Logger('Backbeat:IngestionProducer');
        this.ringReader = new MetadataWrapper('scality', ringParams,
            bucketclient, this.log);
        this.requestLogger = this.log.newRequestLogger();
        this.producer = new BackbeatProducer({
            // zookeeper: { connectionString: 'localhost:2181/backbeat' },
            topic: 'backbeat-generic',
            kafka: { hosts: '127.0.0.1:9092' },
        });
        this.createEntry = new RaftLogEntry();
    }

    _parseBucketName(bucketKey) {
        return bucketKey.split(constants.splitter)[1];
    }

    getBuckets(done) {
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

    getBucketMd(bucketList, done) {
        this.log.info('getting metadata for buckets and ingestion to mongo', {
            numberOfBuckets: bucketList.length,
        });
        return async.eachLimit(bucketList, 10, (bucket, cb) => {
            async.waterfall([
                next => {
                    const bucketObj =
                        this.createEntry.createPutBucketEntry(bucket);
                    return this.producer.send([{ key: bucket.key,
                        message: bucketObj }], err => {
                        if (err) {
                            this.log.error('error sending bucket entry', {
                                method: 'IngestionProducer:getBucketMd',
                                err,
                            });
                            return next(err);
                        }
                        return next();
                    });
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
                        return this.producer.send([{ key: bucket.key,
                            message: bucketMdObj }], next);
                    });
                },
            ], cb);
        }, err => done(err, bucketList));
    }

    getBucketObjects(bucketList, done) {
        this.log.info('getting list of objects from each bucket and ingestion' +
            ' to mongo');
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

    getBucketObjectsMetadata(bucketObjectList, done) {
        this.log.info('getting metadata for each object from bucket');
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
                return this.createAndSendEntry(objectMDs, cb);
            });
        }, err => done(err));
    }

    createAndSendEntry(objectMds, done) {
        if (objectMds.length > 0) {
            this.log.info('sending objects to kafka');
            return async.eachLimit(objectMds, 10, (objectMd, cb) => {
                const objectMdEntry =
                    this.createEntry.createPutEntry(objectMd);
                return this.producer.send(
                    [{ key: objectMd.objectKey, message: objectMdEntry }], cb);
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
