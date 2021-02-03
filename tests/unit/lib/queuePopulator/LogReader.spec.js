const assert = require('assert');

const ZookeeperMock = require('zookeeper-mock');

const { versioning } = require('arsenal');
const { DbPrefixes } = versioning.VersioningConstants;

const { Logger } = require('werelogs');

const LogReader = require('../../../../lib/queuePopulator/LogReader');

class MockLogConsumer {
    readRecords(params, cb) {
        process.nextTick(() => {
            cb(null, {
                info: {
                    cseq: 12345,
                },
            });
        });
    }
}

describe('LogReader', () => {
    let zkMock;
    let logReader;
    let filteredEntry;

    beforeEach(() => {
        zkMock = new ZookeeperMock();
        logReader = new LogReader({
            logId: 'test-log-reader',
            zkClient: zkMock.createClient('localhost:2181'),
            logConsumer: new MockLogConsumer(),
            extensions: [{
                filter: entry => { filteredEntry = entry; },
            }],
            logger: new Logger('test:LogReader'),
        });
    });

    [{
        desc: 'v0 master key',
        key: 'v0-master-key',
        processedKey: 'v0-master-key',
    }, {
        desc: 'v0 version key',
        key: 'v0-version-key\u0000version-id',
        processedKey: 'v0-version-key\u0000version-id',
    }, {
        desc: 'v1 master key',
        key: `${DbPrefixes.Master}v1-master-key`,
        processedKey: 'v1-master-key',
    }, {
        desc: 'v1 version key',
        key: `${DbPrefixes.Version}v1-version-key\u0000version-id`,
        processedKey: 'v1-version-key\u0000version-id',
    }].forEach(testCase => {
        it(`LogReader::_processLogEntry() should process entry with a ${testCase.desc}`, () => {
            logReader._processLogEntry(null, { db: 'db' }, {
                type: 'put',
                key: testCase.key,
                value: '{}',
            });
            assert.deepStrictEqual(filteredEntry, {
                type: 'put',
                bucket: 'db',
                key: testCase.processedKey,
                value: '{}',
            });
        });
    });

    it('should start from latest log cseq plus one if no zookeeper log offset', done => {
        logReader.setup(err => {
            assert.ifError(err);
            assert.strictEqual(logReader.logOffset, 12346);
            done();
        });
    });
});
