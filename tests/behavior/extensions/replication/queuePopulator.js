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
    let latestLastProcessedSeq;

    before(done => {
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
                          Role: 'arn:aws:iam::123456789012:role/backbeat',
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
                queuePopulator = new QueuePopulator(testConfig.zookeeper,
                                                    testConfig.source,
                                                    testConfig.replication,
                                                    testConfig.log);
                queuePopulator.open(next);
            },
            next => {
                queuePopulator.processAllLogEntries({ maxRead: 10 }, next);
            },
            (counters, next) => {
                // we need to save the current last processed sequence
                // number because the storage backend may have an
                // existing non-empty log
                latestLastProcessedSeq = counters.lastProcessedSeq;
                next();
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
                // we need to fetch what is the current last processed
                // sequence number because the storage backend may
                // have a non-empty log already
                latestLastProcessedSeq = counters.lastProcessedSeq;
                assert.ifError(err);
                assert.strictEqual(counters.queuedEntries, 0);
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
                assert.deepStrictEqual(counters, {
                    readRecords: 2,
                    readEntries: 2,
                    queuedEntries: 1,
                    lastProcessedSeq: latestLastProcessedSeq + 2 });
                latestLastProcessedSeq = counters.lastProcessedSeq;
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
                assert.deepStrictEqual(counters, {
                    readRecords: 2,
                    readEntries: 2,
                    queuedEntries: 1,
                    lastProcessedSeq: latestLastProcessedSeq + 2 });
                latestLastProcessedSeq = counters.lastProcessedSeq;
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
                assert.deepStrictEqual(counters, {
                    readRecords: 200,
                    readEntries: 200,
                    queuedEntries: 100,
                    lastProcessedSeq: latestLastProcessedSeq + 200 });
                latestLastProcessedSeq = counters.lastProcessedSeq;
                next();
            },
        ], err => {
            assert.ifError(err);
            done();
        });
    });
});
