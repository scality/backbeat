const assert = require('assert');

const { Logger } = require('werelogs');
const sinon = require('sinon');

const zookeeper = require('node-zookeeper-client');
const QueuePopulator = require('../../lib/queuePopulator/QueuePopulator');
const LogReader = require('../../lib/queuePopulator/LogReader');

class MockLogReader extends LogReader {
    constructor(queuePopulator, id) {
        super({
            logger: new Logger('MockLogReader'),
        });
        this.queuePopulator = queuePopulator;
        this.id = id;
        this.processLogEntriesCallCount = 0;
    }

    setup(cb) {
        process.nextTick(cb);
    }

    processLogEntries(params, done) {
        this.log.info('processLogEntries', { id: this.id });
        this.processLogEntriesCallCount += 1;
        // check that the provisioning update triggered below stopped
        // the original provisioned raft session from being processed
        assert(this.processLogEntriesCallCount <= 2);
        if (this.processLogEntriesCallCount === 2) {
            // at the 2nd invocation, trigger a provisioning update
            this.queuePopulator.logReadersUpdate = [
                new MockLogReader(this.queuePopulator, 'raft_2'),
            ];
        }
        const processedAll = (this.id === 'raft_2');
        process.nextTick(() => done(null, processedAll));
    }
}

describe('QueuePopulator', () => {
    let qp;
    beforeEach(() => {
        qp = new QueuePopulator({}, {}, {
            logSource: 'bucketd',
        }, null, null, null, {});
    });

    it('should stop processing old raft sessions after provisioning update', done => {
        const logReader1 = new MockLogReader(qp, 'raft_1');
        qp.logReadersUpdate = [logReader1];
        qp.processAllLogEntries({}, err => {
            assert.ifError(err);
            assert.strictEqual(logReader1.processLogEntriesCallCount, 2);
            done();
        });
    });

    describe('handle liveness', () => {
        let mockRes;
        let mockLog;
        beforeEach(() => {
            mockRes = sinon.spy();
            mockLog = sinon.spy();
            mockLog.debug = sinon.spy();
            mockRes.writeHead = sinon.spy();
            mockRes.end = sinon.spy();
        });

        afterEach(() => {
            sinon.restore();
        });

        it('success', () => {
            qp.zkClient = {
                getState: () => zookeeper.State.SYNC_CONNECTED,
            };
            const response = qp.handleLiveness(mockRes, mockLog);
            assert.strictEqual(response, undefined);
            sinon.assert.calledOnceWithExactly(
                mockLog.debug,
                sinon.match.any, // we don't care about the debug label
                { zookeeper: zookeeper.State.SYNC_CONNECTED.code }
            );
            sinon.assert.calledOnceWithExactly(mockRes.writeHead, 200);
            sinon.assert.calledOnce(mockRes.end);
        });

        it('success and logs producers', () => {
            const prodStatus = {
                topicA: true,
                topicB: true,
            };
            const logInfo = {
                name: 'random name',
            };
            const mockLogReader = sinon.spy();
            mockLogReader.getProducerStatus = sinon.fake(() => prodStatus);
            mockLogReader.getLogInfo = sinon.fake(() => logInfo);
            qp.logReaders = [
                mockLogReader,
            ];
            qp.zkClient = {
                getState: () => zookeeper.State.SYNC_CONNECTED,
            };
            const response = qp.handleLiveness(mockRes, mockLog);
            assert.strictEqual(response, undefined);
            sinon.assert.calledOnceWithExactly(
                mockLog.debug,
                sinon.match.any, // we don't care about the debug label
                {
                    'zookeeper': zookeeper.State.SYNC_CONNECTED.code,
                    'producer-topicA': true,
                    'producer-topicB': true,
                }
            );
            sinon.assert.calledOnceWithExactly(mockRes.writeHead, 200);
            sinon.assert.calledOnce(mockRes.end);
        });

        it('returns verbose details on errors', () => {
            const mockLogReader = sinon.spy();
            const prodStatus = {
                topicA: true,
                topicB: false,
            };
            const logInfo = {
                name: 'random name 2',
            };
            mockLogReader.getProducerStatus = sinon.fake(() => prodStatus);
            mockLogReader.getLogInfo = sinon.fake(() => logInfo);
            qp.logReaders = [
                mockLogReader,
            ];
            qp.zkClient = {
                getState: () => zookeeper.State.SYNC_CONNECTED,
            };
            const response = qp.handleLiveness(mockRes, mockLog);
            assert.deepStrictEqual(
                JSON.parse(response),
                [
                    {
                        compoonent: 'log reader',
                        status: 'not ready',
                        topic: 'topicB'
                    }
                ]
            );
            sinon.assert.calledOnceWithExactly(
                mockLog.debug,
                sinon.match.any, // we don't care about the debug label
                {
                    'zookeeper': zookeeper.State.SYNC_CONNECTED.code,
                    'producer-topicA': true,
                    'producer-topicB': false,
                }
            );
            sinon.assert.notCalled(mockRes.writeHead);
            sinon.assert.notCalled(mockRes.end);
        });
    });
});
