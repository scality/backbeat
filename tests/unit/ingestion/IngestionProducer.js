const assert = require('assert');
// const Server = require('arsenal').network.http.server;
const http = require('http');
const QueuePopulator = require('../../../lib/queuePopulator/QueuePopulator');
const IngestionProducer =
    require('../../../lib/queuePopulator/IngestionProducer');
const testConfig = require('../../config.json');
const MetadataMock = require('../../utils/MockMetadataServer');

describe('ingestion producer unit tests with mock', () => {
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
        this.iProducer._getBuckets(1, (err, res) => {
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
        this.iProducer.snapshot(1, (err, res) => {
            // we expect 6 logs from the MockMetadataServer: 2 buckets with 2
            // logs per bucket, and 1 object in each bucket with 1 log entry
            assert.strictEqual(res.length, 6);
            res.forEach(entry => {
                assert(entry.type);
                assert(entry.bucket);
                assert(entry.key);
                assert(entry.value || entry.value === null);
            });
            return done();
        });
    });
});
