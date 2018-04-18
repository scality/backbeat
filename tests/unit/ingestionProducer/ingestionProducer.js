const assert = require('assert');
// const Server = require('arsenal').network.http.server;
const http = require('http');
const werelogs = require('werelogs');
const Logger = werelogs.Logger;
const QueuePopulator = require('../../../lib/queuePopulator/QueuePopulator');
const IngestionProducer =
    require('../../../lib/queuePopulator/IngestionProducer');
const testConfig = require('../../config.json');
const MetadataMock = require('../../../utils/MockMetadataServer');

const logger = new Logger('IngestionProducer:test:metadataMock');

describe.only('ingestion producer unit tests with mock', () => {
    let httpServer;
    let metadataMock;

    before(done => {
        console.log('before the ingesion producer test');
        metadataMock = new MetadataMock();
        // httpServer = new Server(7777, logger);
        // metadataMock.start();
        httpServer = http.createServer(
            (req, res) => metadataMock.onRequest(req, res)).listen(7778);
        this.iProducer = new IngestionProducer({
            host: 'localhost:7778',
            port: 7778,
        });
        this.queuePopulator = new QueuePopulator(
            testConfig.zookeeper,
            testConfig.kafka,
            testConfig.queuePopulator,
            testConfig.extensions);
        done();
    });

    after(done => {
        httpServer.close();
        done();
    });

    it('should be able to grab list of buckets for each raft session', done => {
        return this.iProducer._getBuckets(1, (err, res) => {
            assert.ifError(err);
            assert(res);
            assert.strictEqual(res.length, 1);
            return done();
        });
    });

    it('can generate a valid snapshot', done => {
        return this.iProducer.snapshot(1, (err, res) => {
            // we expect 3 logs - 2 logs for the bucket, and 1 log for the obj.
            console.log(res);
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
});
