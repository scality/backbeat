const assert = require('assert');
const sinon = require('sinon');

const zookeeper = require('node-zookeeper-client');
const QueuePopulator = require('../../lib/queuePopulator/QueuePopulator');
const constants = require('../../lib/constants');

describe('QueuePopulator', () => {
    let qp;
    beforeEach(() => {
        qp = new QueuePopulator({}, {}, {
            logSource: 'bucketd',
        }, null, null, null, {});
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
                        component: 'log reader',
                        status: constants.statusNotReady,
                        topic: 'topicB',
                    },
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
