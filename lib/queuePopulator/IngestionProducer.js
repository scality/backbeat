const bucketclient = require('bucketclient');
const async = require('async');
const MetadataWrapper = require('arsenal').storage.metadata.MetadataWrapper;
const QueuePopulator = require('./QueuePopulator');
const BackbeatProducer = require('../BackbeatProducer');
const RaftLogEntry = require('../../extensions/replication/utils/RaftLogEntry');
const constants = require('../../constants');

const ringParams = {
    bucketdBootstrap: ['54.202.35.127'],
    bucketdLog: undefined,
};

class IngestionProducer extends QueuePopulator {
    constructor(zkConfig, qpConfig, extConfigs) {
        super(zkConfig, qpConfig, extConfigs);
        this.ringReader = new MetadataWrapper('scality', ringParams, bucketclient, this.log);
        this.requestLogger = this.log.newRequestLogger();
        this.producer = new BackbeatProducer({
            zookeeper: { connectionString: 'localhost:2181/backbeat' },
            topic: 'backbeat-replication',
        });
    }

    _parseBucketName(bucketKey) {
        return bucketKey.split(constants.splitter)[1];
    }

    getRaftSessionBuckets(done) {
        console.log('GETTING RAFT SESSIONS');
        return this.ringReader.listObject(constants.usersBucket, {},
        this.requestLogger, (err, res) => {
            console.log('list objects');
            if (err) {
                console.log('Error');
                console.log('Error');
                console.log(err);
                return done(err);
            }
            // using the res
            // for each 'object key', which is actually name of bucket, call
            // listObject on the bucket - then for list of objects from bucket,
            // get objectMD
            console.log('no error');
            return done(null, res.Contents);
        });
    }

    getRaftSessionBucketObjects(bucketList, done) {
        console.log('getting all the objects from each bucket');
        // make a dictionary
        // use map
        return async.map(bucketList, (bucketInfo, cb) => {
            // listObject gives me markers?
            // at most 1000 objects, process 10 at a time (parallelLimit async call)
            // for each object, produce JSON; combine all 10 and send to Kafka entry
            // move on and continue processing
            // reach end of the list, check  if there is a marker, list objects with marker
            // write another function that uses the list object, gets objects + marker
            // any method that is not exposed to outside of the class, use with leading '_'
            const bucketName = this._parseBucketName(bucketInfo.key);
            this.ringReader.listObject(bucketName, {}, this.requestLogger, (err, res) => {
                if (err) {
                    return done(err);
                }
                console.log('no error!');
                console.log(bucketName);
                return cb(null, { bucket: bucketName, objects: res.Contents });
            });
        }, (err, buckets) => {
            // console.log('here are the buckets with list of object keys!');
            // console.log(buckets);
            return done(null, buckets);
        });
    }

    getObjectMetadata(bucket, objectKey, done) {
        console.log('trying to grab objectMetadata');
        return this.ringReader.getObjectMD(bucket, objectKey, {}, this.requestLogger, (err, res) => {
            console.log('getting Object Metadata~!');
            if (err) {
                console.log('there is an error');
                console.log(err);
                return done(err);
            }
            console.log('no error, we are returning now');
            // console.log(JSON.parse(res));
            return done(null, res);
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
                    console.log('we got data for ', objectKey);
                    return cb(null, { res, objectKey, bucketName });
                });
            }, (err, objectMDs) => {
                console.log('wat is this');
                return this.createAndSendEntry(objectMDs, cb);
            });
        }, err => {
            return done(err);
        });
    }

    createAndSendEntry(objectMDs, done) {
        const queueEntry = new RaftLogEntry();
        if (objectMDs.length > 0) {
            console.log('sending stuff');
            return async.eachLimit(objectMDs, 10, (objectMD, cb) => {
                this.producer.send([queueEntry.createPutEntry(objectMD.bucketName, objectMD.objectKey, objectMD.res)], () => {});
                return cb();
            }, err => {
                return done(err);
            });
        } else {
            return done();
        }
    }
}

module.exports = IngestionProducer;
