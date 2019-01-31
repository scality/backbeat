const assert = require('assert');
const async = require('async');
const AWS = require('aws-sdk');
const http = require('http');
const QueuePopulator = require('../../../lib/queuePopulator/QueuePopulator');
const IngestionProducer =
    require('../../../lib/queuePopulator/IngestionProducer');
const testConfig = require('../../config.json');
const { MetadataMock, objectList } = require('../../utils/MockMetadataServer');

function emptyAndDeleteVersionedBucket(s3Client, Bucket, cb) {
    // won't need to worry about 1k+ objects pagination
    async.series([
        next => s3Client.listObjectVersions({ Bucket }, (err, data) => {
                    assert.ifError(err);

                    const list = [
                        ...data.Versions.map(v => ({
                            Key: v.Key,
                            VersionId: v.VersionId,
                        })),
                        ...data.DeleteMarkers.map(dm => ({
                            Key: dm.Key,
                            VersionId: dm.VersionId,
                        })),
                    ];

                    if (list.length === 0) {
                        return next();
                    }

                    return s3Client.deleteObjects({
                        Bucket,
                        Delete: { Objects: list },
                    }, next);
                }),
        next => s3Client.deleteBucket({ Bucket }, next),
    ], cb);
}

describe('ingestion producer tests with mock', () => {
    let httpServer;
    let metadataMock;
    const accessKey = 'accessKey1';
    const secretKey = 'verySecretKey1';
    const bucket = 'bucket1';

    before(done => {
        metadataMock = new MetadataMock();
        httpServer = http.createServer(
            (req, res) => metadataMock.onRequest(req, res)).listen(7999);
        this.iProducer = new IngestionProducer({
            name: 'testbucket',
            bucket: 'src-bucket',
            host: 'localhost',
            port: 7999,
            https: false,
            type: 'scality_s3',
            auth: { accessKey, secretKey },
        }, testConfig.queuePopulator, testConfig.s3);

        this.queuePopulator = new QueuePopulator(
            testConfig.zookeeper,
            testConfig.kafka,
            testConfig.queuePopulator,
            testConfig.extensions);

        this.s3Client = new AWS.S3({
            endpoint: `http://${testConfig.s3.host}:${testConfig.s3.port}`,
            s3ForcePathStyle: true,
            credentials: new AWS.Credentials('accessKey1', 'verySecretKey1'),
        });
        return async.series([
            next => this.s3Client.createBucket({ Bucket: bucket }, next),
            next => this.s3Client.putBucketVersioning({
                Bucket: bucket,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => async.eachLimit(objectList.Contents, 10, (obj, cb) => {
                this.s3Client.putObject({
                    Bucket: bucket,
                    Key: obj.key,
                }, cb);
            }, next),
        ], done);
    });

    after(done => {
        httpServer.close();

        emptyAndDeleteVersionedBucket(this.s3Client, bucket, done);
    });

    it('should be able to grab list of buckets for each raft session', done => {
        this.iProducer._getBuckets('1', (err, res) => {
            assert.ifError(err);
            assert(res);
            assert.strictEqual(res.length, 2);
            return done();
        });
    });

    it('should be able to grab list of object versions for given bucket',
    done => {
        this.iProducer._getObjectVersionsList(bucket, (err, res) => {
            assert.ifError(err);
            assert(res);
            // We have a duplicate IsLatest entry in the returned list.
            // Additional entry is for creating a metadata entry where key name
            // does not include the version id
            assert.strictEqual(res.length, 2);

            const isLatestEntry = res.filter(o => o.isLatest);
            assert(isLatestEntry);
            return done();
        });
    });

    it('should be able to grab metadata for list of objects', done => {
        this.iProducer._getObjectVersionsList(bucket, (err, list) => {
            assert.ifError(err);

            this.iProducer._getBucketObjectsMetadata(bucket, list,
            err => {
                assert.ifError(err);
                return done();
            });
        });
    });

    it('can generate a valid snapshot', done => {
        this.iProducer.snapshot(bucket, (err, res) => {
            assert.ifError(err);

            assert.strictEqual(res.length, 2);
            res.forEach(entry => {
                assert.strictEqual(entry.type, 'put');
                assert(entry.bucket);
                assert(entry.key);
                assert(entry.value);
            });
            assert(res[0].key !== res[1].key);

            return done();
        });
    });

    it('should be able to get raft logs', done => {
        this.iProducer.getRaftLog('1', null, null, null, (err, res) => {
            assert.ifError(err);
            assert(res.log);
            assert(res.info);
            res.log.on('data', data => {
                assert(data.db);
                assert.strictEqual(typeof data.method, 'number');
                assert.strictEqual(typeof data.entries, 'object');
                assert.strictEqual(data.entries.length, 1);
            });
            res.log.on('end', done);
        });
    });

    it('should find the correct raftId for the requested bucket', done => {
        this.iProducer.getRaftId('bucketfindraftid', (err, res) => {
            // based on MetadataMock, raft 1 will have 'bucketfindraftid'
            assert.ifError(err);
            assert.strictEqual(typeof res, 'string');
            assert.strictEqual(res, '5');
            return done();
        });
    });
});
