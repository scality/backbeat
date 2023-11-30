const assert = require('assert');
const promClient = require('prom-client');

const { Logger } = require('werelogs');
const sinon = require('sinon');

const zookeeper = require('node-zookeeper-client');
const QueuePopulator = require('../../lib/queuePopulator/QueuePopulator');
const LogReader = require('../../lib/queuePopulator/LogReader');
const constants = require('../../lib/constants');

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
        let closeCalled = false;
        logReader1.close = cb => {
            assert(!closeCalled);
            closeCalled = true;
            process.nextTick(cb);
        };
        qp.logReadersUpdate = [logReader1];
        qp.processAllLogEntries({}, err => {
            assert.ifError(err);
            assert.strictEqual(logReader1.processLogEntriesCallCount, 2);
            assert(!closeCalled);
            qp.processAllLogEntries({}, err => {
                assert.ifError(err);
                assert(closeCalled);
                done();
            });
        });
    });

    describe('handle liveness', () => {
        let mockRes;
        let mockLog;
        beforeEach(() => {
            mockRes = sinon.spy();
            mockLog = sinon.spy();
            mockLog.debug = sinon.spy();
            mockLog.error = sinon.spy();
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

            sinon.assert.calledTwice(mockLog.debug);
            // First call assertion
            sinon.assert.calledWith(
                mockLog.debug.getCall(0), // getCall(0) retrieves the first call
                'verbose liveness',
                { zookeeper: zookeeper.State.SYNC_CONNECTED.code },
            );
            // Second call assertion
            sinon.assert.calledWith(
                mockLog.debug.getCall(1), // getCall(1) retrieves the second call
                'replying with success',
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
            sinon.assert.calledTwice(mockLog.debug);
            // First call assertion
            sinon.assert.calledWith(
                mockLog.debug.getCall(0), // getCall(0) retrieves the first call
                'verbose liveness',
                {
                    'zookeeper': zookeeper.State.SYNC_CONNECTED.code,
                    'producer-topicA': true,
                    'producer-topicB': true,
                },
            );
            // Second call assertion
            sinon.assert.calledWith(
                mockLog.debug.getCall(1), // getCall(1) retrieves the second call
                'replying with success',
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
            assert.strictEqual(response, undefined);
            sinon.assert.calledOnceWithExactly(
                mockLog.debug,
                'verbose liveness',
                {
                    'zookeeper': zookeeper.State.SYNC_CONNECTED.code,
                    'producer-topicA': true,
                    'producer-topicB': false,
                },
            );
            sinon.assert.calledOnceWithExactly(mockRes.writeHead, 500);
            const expectedRes = [
                {
                    component: 'log reader',
                    status: constants.statusNotReady,
                    topic: 'topicB',
                },
            ];
            sinon.assert.calledOnceWithExactly(
                mockLog.error,
                'sending back error response',
                {
                    httpCode: 500,
                    error: expectedRes,
                },
            );
            sinon.assert.calledOnceWithExactly(mockRes.end, JSON.stringify(expectedRes));
        });
    });

    describe('handle metrics', () => {
        let response;
        let logger;

        beforeEach(() => {
            response = {
                writeHead: sinon.stub(),
                end: sinon.stub(),
            };
            logger = {
                debug: sinon.stub(),
                error: sinon.stub(),
            };
            sinon.stub(promClient.register, 'metrics').resolves('metrics_data');
        });

        it('should handle metrics correctly', async () => {
            const r = await qp.handleMetrics(response, logger);
            assert.strictEqual(r, undefined);

            sinon.assert.calledOnce(response.writeHead);
            sinon.assert.calledOnceWithExactly(response.writeHead, 200, {
                'Content-Type': promClient.register.contentType,
            });

            sinon.assert.calledOnce(response.end);
            sinon.assert.calledWithExactly(response.end, 'metrics_data');

            sinon.assert.calledOnce(logger.debug);
            sinon.assert.calledWithExactly(logger.debug, 'metrics requested');
            return;
        });
    });
});
