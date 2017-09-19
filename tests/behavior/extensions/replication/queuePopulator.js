const assert = require('assert');
const async = require('async');

const AWS = require('aws-sdk');
const S3 = AWS.S3;

const QueuePopulator = require('../../../../extensions/replication' +
                               '/queuePopulator/QueuePopulator');

const testConfig = require('../../../config.json');
const testBucket = 'queue-populator-test-bucket';

const s3config = {
    endpoint: `${testConfig.s3.transport}://` +
        `${testConfig.s3.host}:${testConfig.s3.port}`,
    s3ForcePathStyle: true,
    credentials: new AWS.Credentials(testConfig.s3.accessKey,
                                     testConfig.s3.secretKey),
};

describe('queuePopulator', () => {
    let queuePopulator;
    let s3;

    before(function setup(done) {
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
                                    StorageClass: 'STANDARD',
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
                    testConfig.replication.source,
                    testConfig.replication);
                queuePopulator.open(next);
            },
            next => {
                queuePopulator.processAllLogEntries({ maxRead: 10 }, next);
            },
        ], err => {
            assert.ifError(err);
            done();
        });
    });
    after(done => {
        async.waterfall([
            next => {
                next();
            },
        ], done);
    });

    it('processAllLogEntries with nothing to do', done => {
        queuePopulator.processAllLogEntries(
            { maxRead: 10 }, (err, counters) => {
                assert.ifError(err);
                assert.strictEqual(counters[0].queuedEntries, 0);
                done();
            });
    });
    it('processAllLogEntries with an object to replicate', done => {
        async.waterfall([
            next => {
                s3.putObject({ Bucket: testBucket,
                    Key: 'keyToReplicate',
                    Body: 'howdy',
                    Tagging: 'mytag=mytagvalue' }, next);
            },
            (data, next) => {
                queuePopulator.processAllLogEntries({ maxRead: 10 }, next);
            },
            (counters, next) => {
                // 2 reads expected: master key and and versioned key
                // 1 queued: versioned key only
                assert.strictEqual(counters[0].readEntries, 2);
                assert.strictEqual(counters[0].queuedEntries, 1);
                next();
            },
        ], err => {
            assert.ifError(err);
            done();
        });
    });
    it('processAllLogEntries with an object deletion to replicate', done => {
        async.waterfall([
            next => {
                s3.deleteObject({ Bucket: testBucket,
                    Key: 'keyToReplicate' }, next);
            },
            (data, next) => {
                queuePopulator.processAllLogEntries({ maxRead: 10 }, next);
            },
            (counters, next) => {
                // 2 reads expected: master key update + new delete marker
                // 1 queued: versioned key (delete marker)
                assert.strictEqual(counters[0].readEntries, 2);
                assert.strictEqual(counters[0].queuedEntries, 1);
                next();
            },
        ], err => {
            assert.ifError(err);
            done();
        });
    });
    it('processAllLogEntries with 100 objects to replicate in 20 batches',
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
                queuePopulator.processAllLogEntries({ maxRead: 10 }, next);
            },
            (counters, next) => {
                // 2 reads expected: master key and and versioned key
                // 1 queued: versioned key only
                assert.strictEqual(counters[0].readEntries, 200);
                assert.strictEqual(counters[0].queuedEntries, 100);
                next();
            },
        ], err => {
            assert.ifError(err);
            done();
        });
    });
});
