const assert = require('assert');

const ZookeeperMock = require('zookeeper-mock');

const { versioning } = require('arsenal');

const { Logger } = require('werelogs');

const LogReader = require('../../../../lib/queuePopulator/LogReader');

class MockLogConsumer {
    readRecords(params, cb) {
        process.nextTick(() => {
            cb(null, {});
        });
    }
}

describe('LogReader', () => {
    let zkMock;
    let logReader;

    beforeEach(() => {
        zkMock = new ZookeeperMock();
        logReader = new LogReader({
            logId: 'test-log-reader',
            zkClient: zkMock.createClient('localhost:2181'),
            logConsumer: new MockLogConsumer(),
            logger: new Logger('test:LogReader'),
        });
    });

    // Currently the initial offset is set to 1 with mongodb backend,
    // it looks odd considering mongodb uses random IDs as log cursors
    // and could be cleaned up, but this test coming from 7.4 branch
    // with a raft log reader still makes sense to improve coverage in
    // the current code, so keeping it.
    it('should start from offset 1 if no zookeeper log offset', done => {
        logReader.setup(err => {
            assert.ifError(err);
            assert.strictEqual(logReader.logOffset, 1);
            done();
        });
    });
});
