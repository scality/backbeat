const assert = require('assert');
// const Server = require('arsenal').network.http.server;
const http = require('http');
const QueuePopulator = require('../../../lib/queuePopulator/QueuePopulator');
const IngestionProducer =
    require('../../../lib/queuePopulator/IngestionProducer');
const testConfig = require('../../config.json');
const MetadataMock = require('../../utils/MockMetadataServer');

describe('ingestion producer tests with mock', () => {
    let httpServer;
    let metadataMock;

    beforeEach(done => {
        metadataMock = new MetadataMock();
        httpServer = http.createServer(
            (req, res) => metadataMock.onRequest(req, res)).listen(7999);
        testConfig.s3.port = 7999;
        this.iProducer = new IngestionProducer({
            host: 'localhost',
            port: 7999,
            prefix: 'testbackend',
            auth: {
                type: 'account',
                account: 'bart',
                vault: {
                    host: '127.0.0.1',
                    port: 8500,
                    adminPort: 8600,
                },
            },
        }, testConfig.queuePopulator, testConfig.s3);
        this.queuePopulator = new QueuePopulator(
            testConfig.zookeeper,
            testConfig.kafka,
            testConfig.queuePopulator,
            testConfig.extensions);
        done();
    });

    afterEach(done => {
        httpServer.close();
        done();
    });

    it('should be able to grab list of buckets for each raft session', done => {
        this.iProducer._getBuckets('1', (err, res) => {
            assert.ifError(err);
            assert(res);
            assert.strictEqual(res.length, 2);
            return done();
        });
    });

    it('should be able to grab list of objects for each bucket', done => {
        this.iProducer._getBucketObjects(['bucket1'], (err, res) => {
            assert.ifError(err);
            assert(res);
            assert(res[0]);
            assert(res[0].bucket);
            assert(res[0].objects);
            return done();
        });
    });

    it('should be able to grab metadata for specified bucket', done => {
        this.iProducer._getBucketMd(['bucket1'], (err, res) => {
            assert.ifError(err);
            assert(res);
            assert.deepStrictEqual(res, ['bucket1']);
            return done();
        });
    });

    it('should be able to grab metadata for list of objects', done => {
        this.iProducer._getBucketObjectsMetadata([{
            bucket: 'bucket1',
            objects: [{
                key: 'testobject1',
                value: 'testval',
            }],
        }], err => {
            assert.ifError(err);
            return done();
        });
    });

    it('can generate a valid snapshot', done => {
        this.iProducer.snapshot('bucket1', (err, res) => {
            // we expect 3 logs from the MockMetadataServer: 1 bucket with 2 log
            // entries per bucket, and 1 object in each bucket with 1 log entry
            assert.strictEqual(res.length, 3);
            res.forEach(entry => {
                assert(entry.type);
                assert(entry.bucket);
                assert(entry.key);
                assert(entry.value || entry.value === null);
            });
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
