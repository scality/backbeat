const assert = require('assert');
const async = require('async');
const zookeeper = require('../../lib/clients/zookeeper');

const AWS = require('aws-sdk');
const S3 = AWS.S3;

const QueuePopulator = require('../../lib/queuePopulator/QueuePopulator');

const testConfig = require('../config.json');
const testBucket = 'queue-populator-test-bucket';

const s3config = {
    endpoint: `${testConfig.s3.transport}://` +
        `${testConfig.s3.host}:${testConfig.s3.port}`,
    s3ForcePathStyle: true,
    credentials: new AWS.Credentials(testConfig.s3.accessKey,
                                     testConfig.s3.secretKey),
};

/**
 * Test whether a given zookeeper node path exists
 * @param {Boolean} shouldExist - Whether the node should exist or not
 * @param {Object} zkClient - The Zookeeper client
 * @param {String} path - The Zookeeper data path to check for the node
 * @param {Function} cb - The callback to call.
 * @return {undefined}
 */
function doesZkNodeExist(shouldExist, zkClient, path, cb) {
    return zkClient.exists(path, (err, stat) => {
        if (err) {
            return cb(err);
        }
        if (shouldExist) {
            assert(stat, 'lifecycle data path was not pre-created');
        } else {
            assert(stat === null, 'lifecycle data path was pre-created');
        }
        return cb();
    });
}

/**
 * Test whether a zookeeper bucket node with given bucket name exists
 * @param {Boolean} shouldExist - Whether the node should exist or not
 * @param {Object} zkClient - The Zookeeper client
 * @param {String} bucketName - The name of the bucket to check in
 * Zookeeper
 * @param {Function} cb - The callback to call.
 * @return {undefined}
 */
function doesBucketNodeExist(shouldExist, zkClient, bucketName, cb) {
    const { zookeeperPath } = testConfig.extensions.lifecycle;
    const lifecycleZkPath = `${zookeeperPath}/data/buckets`;
    zkClient.getChildren(lifecycleZkPath, (err, children) => {
        if (err) {
            return cb(err);
        }
        const match = children.some(child => {
            const name = child.split(':')[2];
            return name === bucketName;
        });
        if (shouldExist) {
            assert(match, 'lifecycle bucket node was not created');
        } else {
            assert(!match, 'lifecycle bucket node should not exist');
        }
        return cb();
    });
}

describe('queuePopulator', () => {
    let queuePopulator;
    let s3;
    let zkClient;

    beforeEach(function setup(done) {
        this.timeout(30000); // may take some time to keep up with the
                             // log entries

        s3 = new S3(s3config);
        async.waterfall([
            next => {
                s3.createBucket({
                    Bucket: testBucket,
                }, next);
            },
            (data, next) => {
                s3.putBucketVersioning(
                    { Bucket: testBucket,
                        VersioningConfiguration: {
                            Status: 'Enabled',
                        },
                    }, next);
            },
            (data, next) => {
                s3.putBucketReplication(
                    { Bucket: testBucket,
                        ReplicationConfiguration: {
                            Role: 'arn:aws:iam::123456789012:role/backbeat,' +
                              'arn:aws:iam::123456789012:role/backbeat',
                            Rules: [{
                                Destination: {
                                    Bucket: 'arn:aws:s3:::dummy-dest-bucket',
                                },
                                Prefix: '',
                                Status: 'Enabled',
                            }],
                        },
                    }, next);
            },
            (data, next) => {
                queuePopulator = new QueuePopulator(
                    testConfig.zookeeper,
                    testConfig.kafka,
                    testConfig.queuePopulator,
                    undefined,
                    undefined,
                    undefined,
                    testConfig.extensions);
                queuePopulator.open(next);
            },
            next => {
                queuePopulator.processLogEntries({ maxRead: 10 }, next);
            },
            (data, next) => {
                const { connectionString } = testConfig.zookeeper;
                zkClient = zookeeper.createClient(connectionString, {
                    autoCreateNamespace: false,
                });
                zkClient.connect();
                zkClient.once('error', next);
                zkClient.once('ready', () => {
                    // just in case there would be more 'error' events emitted
                    zkClient.removeAllListeners('error');
                    return next();
                });
            },
        ], err => {
            assert.ifError(err);
            done();
        });
    });

    afterEach(done =>
        s3.listObjectVersions({ Bucket: testBucket }, (err, data) => {
            // Bucket was deleted in a test.
            if (err && err.code === 'NoSuchBucket') {
                return done();
            }
            if (err) {
                return done(err);
            }
            return async.eachLimit(data.Versions, 10, (version, next) =>
                s3.deleteObject({
                    Bucket: testBucket,
                    Key: version.Key,
                    VersionId: version.VersionId,
                }, next),
            err => {
                if (err) {
                    return done(err);
                }
                return async.eachLimit(data.DeleteMarkers, 10,
                    (deleteMarker, next) =>
                        s3.deleteObject({
                            Bucket: testBucket,
                            Key: deleteMarker.Key,
                            VersionId: deleteMarker.VersionId,
                        }, next),
                done);
            });
        }));

    it('processLogEntries with nothing to do', done => {
        queuePopulator.processLogEntries(
            { maxRead: 10 }, (err, counters) => {
                assert.ifError(err);
                assert.deepStrictEqual(counters[0].queuedEntries, {});
                done();
            });
    });
    it('processLogEntries with an object to replicate', done => {
        async.waterfall([
            next => {
                s3.putObject({ Bucket: testBucket,
                    Key: 'keyToReplicate',
                    Body: 'howdy',
                    Tagging: 'mytag=mytagvalue' }, next);
            },
            (data, next) => {
                queuePopulator.processLogEntries({ maxRead: 10 }, next);
            },
            (counters, next) => {
                // 2 reads expected: master key and and versioned key
                // 1 queued: versioned key only
                assert.strictEqual(counters[0].readEntries, 2);
                assert.deepStrictEqual(counters[0].queuedEntries,
                                       { 'backbeat-test-replication': 1 });
                next();
            },
        ], err => {
            assert.ifError(err);
            done();
        });
    });
    it('processLogEntries with an object put and delete to replicate',
    done => {
        async.waterfall([
            next => {
                s3.putObject({ Bucket: testBucket,
                    Key: 'keyToReplicate',
                    Body: 'howdy',
                    Tagging: 'mytag=mytagvalue' }, next);
            },
            (data, next) => {
                queuePopulator.processLogEntries({ maxRead: 10 }, next);
            },
            (counters, next) => {
                s3.deleteObject({ Bucket: testBucket,
                    Key: 'keyToReplicate' }, next);
            },
            (data, next) => {
                queuePopulator.processLogEntries({ maxRead: 10 }, next);
            },
            (counters, next) => {
                // 2 reads expected: master key update + new delete marker
                // 1 queued: versioned key (delete marker)
                assert.strictEqual(counters[0].readEntries, 2);
                assert.deepStrictEqual(counters[0].queuedEntries,
                                       { 'backbeat-test-replication': 1 });
                next();
            },
        ], err => {
            assert.ifError(err);
            done();
        });
    });
    it('processLogEntries with 100 objects to replicate in 20 batches',
    function test100objects(done) {
        this.timeout(10000);
        async.waterfall([
            next => {
                let nbDone = 0;
                function cbPut(err) {
                    assert.ifError(err);
                    ++nbDone;
                    if (nbDone === 100) {
                        next();
                    }
                }
                for (let i = 0; i < 100; ++i) {
                    s3.putObject({
                        Bucket: testBucket,
                        Key: `keyToReplicate_${i}`,
                        Body: 'howdy',
                        Tagging: 'mytag=mytagvalue',
                    }, cbPut);
                }
            },
            next => {
                queuePopulator.processLogEntries({ maxRead: 10 }, next);
            },
            (counters, next) => {
                // 2 reads expected: master key and and versioned key
                // 1 queued: versioned key only
                assert.strictEqual(counters[0].readEntries, 200);
                assert.deepStrictEqual(counters[0].queuedEntries,
                                       { 'backbeat-test-replication': 100 });
                next();
            },
        ], err => {
            assert.ifError(err);
            done();
        });
    });

    describe('lifecycle extension', () => {
        const { zookeeperPath } = testConfig.extensions.lifecycle;
        const lifecycleZkPath = `${zookeeperPath}/data/buckets`;

        it('should have pre-created the lifecycle data path', done =>
            doesZkNodeExist(true, zkClient, lifecycleZkPath, done));

        it('should not have created the lifecycle bucket data path', done =>
            doesBucketNodeExist(false, zkClient, testBucket, done));

        describe('buckets zookeeper node path', () => {
            beforeEach(done =>
                s3.putBucketLifecycle({
                    Bucket: testBucket,
                    LifecycleConfiguration: {
                        Rules: [{
                            Expiration: { Date: '2016-01-01T00:00:00.000Z' },
                            ID: 'Delete 2014 logs in 2016.',
                            Prefix: 'logs/2014/',
                            Status: 'Enabled',
                        }],
                    },
                }, done));

            it('should have created the lifecycle bucket data path', done =>
                queuePopulator.processLogEntries({ maxRead: 10 },
                    (err, counters) => {
                        if (err) {
                            return done(err);
                        }
                        assert.strictEqual(counters[0].readEntries, 1);
                        return doesBucketNodeExist(true, zkClient, testBucket,
                            done);
                    }));

            it('should delete lifecycle bucket data path if bucket is deleted',
            done => async.series([
                next =>
                    queuePopulator.processLogEntries({ maxRead: 10 }, next),
                next =>
                    doesBucketNodeExist(true, zkClient, testBucket, next),
                next =>
                    s3.deleteBucket({ Bucket: testBucket }, next),
                next =>
                    queuePopulator.processLogEntries({ maxRead: 10 },
                        next),
                next =>
                    doesBucketNodeExist(false, zkClient, testBucket, next),
            ], done));

            it('should delete lifecycle bucket data path if lifecycle config ' +
            'is deleted', done => async.series([
                next =>
                    queuePopulator.processLogEntries({ maxRead: 10 }, next),
                next =>
                    doesBucketNodeExist(true, zkClient, testBucket, next),
                next =>
                    s3.deleteBucketLifecycle({ Bucket: testBucket }, next),
                next =>
                    queuePopulator.processLogEntries({ maxRead: 10 }, next),
                next =>
                    doesBucketNodeExist(false, zkClient, testBucket, next),
            ], done));
        });
    });
});
