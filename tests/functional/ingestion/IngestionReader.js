const assert = require('assert');
const async = require('async');
const http = require('http');

const IngestionReader = require('../../../lib/queuePopulator/IngestionReader');

const fakeLogger = require('../../utils/fakeLogger');
const testConfig = require('../../config.json');
const { MetadataMock } = require('arsenal').testing.MetadataMock;
const testPort = testConfig.extensions.ingestion.sources[0].port;
const mockLogOffset = 2;

/**
 * The QueuePopulatorExtension class sends entries to kafka, but for testing
 * purposes we may not have kafka setup for quick testing. This class will
 * mock some of the functions that are used by the logReader classes, and will
 * assert that the input from the logReaders is as expected.
 */
class TestIngestionQP {
    constructor(params) {
        this.expectedEntry = params.expectedEntry;
    }

    setBatch(batch) {
        this._batch = batch;
    }

    filter(entry) {
        assert.strictEqual(`${entry.bucket}/${entry.key}`,
            `${this.expectedEntry.bucket}/${this.expectedEntry.key}`);
        assert.deepStrictEqual(entry, this.expectedEntry);
    }
}

const expectedEntry = {
    type: 'put',
    bucket: 'zenkobucket',
    key: 'afternoon',
    value: '{"owner-display-name":"test_1518720219","owner-id":' +
    '"94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8",' +
    '"content-length":0,"content-md5":"d41d8cd98f00b204e9800998ecf8427e",' +
    '"x-amz-version-id":"null","x-amz-server-version-id":"",' +
    '"x-amz-storage-class":"STANDARD","x-amz-server-side-encryption":"",' +
    '"x-amz-server-side-encryption-aws-kms-key-id":"",' +
    '"x-amz-server-side-encryption-customer-algorithm":"",' +
    '"x-amz-website-redirect-location":"","acl":{"Canned":"private",' +
    '"FULL_CONTROL":[],"WRITE_ACP":[],"READ":[],"READ_ACP":[]},"key":"",' +
    '"location":null,"isDeleteMarker":false,"tags":{},"replicationInfo":' +
    '{"status":"","backends":[],"content":[],"destination":"","storageClass":' +
    '"","role":"","storageType":"","dataStoreVersionId":""},"dataStoreName":' +
    '"us-east-1","last-modified":"2018-02-16T21:56:52.690Z",' +
    '"md-model-version":3}',
};

const extIngestionQP = new TestIngestionQP({ expectedEntry });

class MockZkClient {
    constructor(params) {
        this.logOffset = params.logOffset;
    }

    getData(data, done) {
        return done(null, this.logOffset);
    }
}

describe('ingestion reader tests with mock', () => {
    let httpServer;
    let batchState;

    beforeEach(done => {
        batchState = {
            logRes: null,
            logStats: {
                nbLogRecordsRead: 0,
                nbLogEntriesRead: 0,
            },
            entriesToPublish: {},
            publishedEntries: {},
            maxRead: 10000,
            startTime: Date.now(),
            timeoutMs: 1000,
            logger: fakeLogger,
        };
        const metadataMock = new MetadataMock();
        httpServer = http.createServer(
            (req, res) => metadataMock.onRequest(req, res)).listen(testPort);
        testConfig.s3.port = testPort;
        this.ingestionReader = new IngestionReader({
            zkClient: new MockZkClient({
                logOffset: mockLogOffset,
            }),
            kafkaConfig: testConfig.kafka,
            bucketdConfig: testConfig.extensions.ingestion.sources[0],
            qpConfig: testConfig.queuePopulator,
            logger: fakeLogger,
            extensions: [extIngestionQP],
            s3Config: testConfig.s3,
            bucket: testConfig.extensions.ingestion.sources[0].bucket,
        });
        this.ingestionReader.setup(done);
    });

    afterEach(done => {
        httpServer.close();
        done();
    });

    it('_processReadRecords should retrieve logRes stream', done => {
        assert.strictEqual(batchState.logRes, null);
        return this.ingestionReader._processReadRecords({}, batchState, err => {
            assert.ifError(err);
            assert.deepStrictEqual(batchState.logRes.info,
                { start: 1, cseq: 7, prune: 1 });
            const receivedLogs = [];
            batchState.logRes.log.on('data', data => {
                receivedLogs.push(data);
            });
            batchState.logRes.log.on('end', () => {
                assert.strictEqual(receivedLogs.length, 7);
                return done();
            });
        });
    });

    // Assertion on parsedlogs here is done in the extIngestionQP mock
    it('_processPrepareEntries should send entries in the correct format and ' +
    'update `nbLogEntriesRead` + `nbLogRecordsRead`', done => {
        async.waterfall([
            next =>
                this.ingestionReader._processReadRecords({}, batchState, next),
            next =>
            this.ingestionReader._processPrepareEntries(batchState, next),
        ], () => {
            assert.deepStrictEqual(batchState.logStats, {
                nbLogRecordsRead: 7, nbLogEntriesRead: 7,
            });
            return done();
        });
    });

    it('should get logOffset', done => {
        const logOffset = this.ingestionReader.getLogOffset();
        // value initialized when creating MockZkClient
        assert.equal(logOffset, mockLogOffset);
        done();
    });
});
