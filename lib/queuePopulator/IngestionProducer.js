const async = require('async');
const bucketclient = require('bucketclient');
const Logger = require('werelogs').Logger;
const MetadataWrapper = require('arsenal').storage.metadata.MetadataWrapper;

const BackbeatProducer = require('../BackbeatProducer');
const QueuePopulator = require('./QueuePopulator');
const zookeeper = require('../clients/zookeeper');
const ProvisionDispatcher = require('../provisioning/ProvisionDispatcher');
const RaftLogReader = require('./RaftLogReader');
const BucketFileLogReader = require('./BucketFileLogReader');
const MetricsProducer = require('../MetricsProducer');
const MetricsConsumer = require('../MetricsConsumer');
const constants = require('../../constants');

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
    }

    _parseBucketName(bucketKey) {
        return bucketKey.split(constants.splitter)[1];
    }

    getRaftSessionBuckets(done) {
        console.log('getting raft sessions');
        return this.ringReader.listObject(constants.usersBucket, {},
        this.requestLogger, (err, res) => {
            console.log('listing keys in usersBucket');
            if (err) {
                console.log('error', err);
                return done(err);
            }
            console.log('no error');
            return done(null, res.Contents);
        });
    }

    getBucketMd(bucketList, done) {
        console.log('getting bucket md');
        return async.eachLimig(bucketList, 10, (bucket, cb) => {
            console.log(bucket);
            const bucketName = this._parseBucketName(bucket.key);
            return this.ringReader.getBucket(bucketName, this.requestLogger,
            (err, res) => {
                console.log('we got a bucket');
                console.log(res);
                // send the entry after formatting properly
                return cb(err);
            });
        }, err => {
            return done(err, bucketList);
        });
    }

    getRaftSessionBucketObjects(bucketList, done) {
        console.log('getting all the objects from each bucket');
        return async.map(bucketList, (bucketInfo, cb) => {
            const bucketName = this._parseBucketName(bucketInfo.key);
            this.ringReader.listObject(bucketName, {}, this.requestLogger, (err, res) => {
                if (err) {
                    console.log('error');
                    return done(err);
                }
                console.log('no error!');
                console.log(bucketName);
                return cb(null, { bucket: bucketName, objects: res.Contesnts });
            });
        }, (err, buckets) => {
            return done(null, buckets);
        });
    }

    getBucketObjectsMetadata(bucketObjectList, done) {
        console.log('BUCKET OBJECT LIST');
        console.log(bucketObjectList);
        return async.mapLimit(bucketObjectList, 1, (bucket, cb) => {
            console.log('extracting list of objects for each bucket');
            const bucketName = bucket.bucket;
            return async.mapLimit(bucket.objects, 10, (object, cb) => {
                console.log('mapping');
                const objectKey = object.key;
                return this.getObjectMetadata(bucketName, objectKey, (err, res) => {
                    console.log('we got data for', objectKey);
                    return cb(null, { res, objectKey, bucketName });
                });
            }, (err, objectMDs) => {
                console.log('sending object metadata');
                return this.createAndSendEntry(objectMDs, cb);
            });
        }, err => {
            return done(err);
        });
    }

    getObjectMetadata(bucket, objectKye, done) {
        console.log('trying to grab objectMetadata');
        return this.ringReader.getObjectMD(bucket, objectKey, {}, this.requestLogger, (err, res) => {
            console.log('getting Object Metadata!');
            if (err) {
                console.log('error !', err);
                return done(err);
            }
            console.log('no error');
            return done(null, res);
        });
    }

    createAndSendEntry(objectMDs, done) {
        if (objectMDs.length > 0) {
            console.log('sending stuff');
            return async.eachLimit(objectMDs, 10, (objectMD, cb) => {
                this.producer.send(
                    [this.createEntry.createPutEntry(objectMD.bucketName,
                        objectMD.objectKey, objectMD.res)], () => {});
                return cb();
            }, err => {
                return done(err);
            });
        }
        return undefined;
    }
}

module.exports = IngestionProducer;
