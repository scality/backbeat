const assert = require('assert');
const http = require('http');
const { MetadataMock } = require('../utils/MetadataMock');
const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;

const QueuePopulator = require('../../../lib/queuePopulator/QueuePopulator');
const IngestionProducer =
    require('../../../lib/queuePopulator/IngestionProducer');
const { setupS3Mock, emptyAndDeleteVersionedBucket } = require('./S3Mock');
const testConfig = require('../../config.json');

const sourceConfig = testConfig.extensions.ingestion.sources[0];

function _extractVersionedBaseKey(key) {
    return key.split(VID_SEP)[0];
}

describe('ingestion producer tests with mock', () => {
    let httpServer;
    let metadataMock;
    const bucket = sourceConfig.bucket;

    before(done => {
        metadataMock = new MetadataMock();
        httpServer = http.createServer(
            (req, res) => metadataMock.onRequest(req, res)).listen(7998);

        const adjustedConfig = Object.assign({}, sourceConfig, {
            auth: { accessKey: 'accessKey1', secretKey: 'verySecretKey1' },
        });
        this.iProducer = new IngestionProducer(adjustedConfig,
            testConfig.queuePopulator, testConfig.s3);

        this.queuePopulator = new QueuePopulator(
            testConfig.zookeeper,
            testConfig.kafka,
            testConfig.queuePopulator,
            null,
            null,
            testConfig.extensions);
        setupS3Mock(sourceConfig, done);
    });

    after(done => {
        httpServer.close();

        emptyAndDeleteVersionedBucket(sourceConfig, done);
    });

    // skipping because functionality currently not needed
    it.skip('should be able to grab list of buckets for each raft session',
        done => {
        this.iProducer._getBuckets('1', (err, res) => {
            assert.ifError(err);
            assert(res);
            assert.strictEqual(res.length, 2);
            return done();
        });
    });

    it('should be able to grab list of object versions for given bucket',
    done => {
        this.iProducer._getObjectVersionsList(bucket, {}, (err, res) => {
            assert.ifError(err);
            assert(res);
            assert(Array.isArray(res.versionList));
            assert.strictEqual(typeof res.IsTruncated, 'boolean');

            assert.strictEqual(res.IsTruncated, false);
            assert.strictEqual(res.versionList.length, 1);

            const isLatestEntry = res.versionList.filter(o => o.isLatest);
            assert(isLatestEntry);
            return done();
        });
    });

    it('should be able to grab metadata for list of objects', done => {
        this.iProducer._getObjectVersionsList(bucket, {}, (err, res) => {
            assert.ifError(err);

            const { versionList } = res;
            this.iProducer._getBucketObjectsMetadata(bucket, versionList,
            (err, list) => {
                assert.ifError(err);

                assert(Array.isArray(list));
                // one versioned entry, one master entry
                assert.strictEqual(list.length, 2);
                const [entry1, entry2] = list;
                assert.strictEqual(entry1.type, entry2.type);
                assert.strictEqual(entry1.bucket, entry2.bucket);
                assert.deepStrictEqual(entry1.value, entry2.value);
                assert(entry1.key !== entry2.key);
                assert.strictEqual(
                    _extractVersionedBaseKey(entry1.key),
                    _extractVersionedBaseKey(entry2.key));

                return done();
            });
        });
    });

    it('should be able to grab bucket cseq', done => {
        this.iProducer._getBucketCseq('bucket1', (err, cseq) => {
            assert.ifError(err);
            assert.strictEqual(cseq, 7);
            return done();
        });
    });

    it('can generate a valid snapshot', done => {
        this.iProducer.snapshot(bucket, {}, (err, res) => {
            assert.ifError(err);

            assert(res);
            assert(res.logRes);
            assert(res.initState);

            assert.strictEqual(res.logRes.length, 2);
            res.logRes.forEach(entry => {
                assert.strictEqual(entry.type, 'put');
                assert(entry.bucket);
                assert(entry.key);
                assert(entry.value);
            });
            assert.strictEqual(res.initState.isStatusComplete, true);
            assert.strictEqual(res.initState.versionMarker, undefined);
            assert.strictEqual(res.initState.keyMarker, undefined);

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
                assert(Array.isArray(data.entries));
                // for our test mock, we may have up to 2 entries
                assert(data.entries.length > 0 && data.entries.length <= 2);
            });
            res.log.on('end', done);
        });
    });

    it('should find the correct raftId for the requested bucket', done => {
        this.iProducer.getRaftId('bucket1', (err, res) => {
            // based on MetadataMock, raft 1 will have 'bucketfindraftid'
            assert.ifError(err);
            assert.strictEqual(typeof res, 'string');
            assert.strictEqual(res, '1');
            return done();
        });
    });
});
