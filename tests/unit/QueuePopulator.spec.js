const assert = require('assert');
const sinon = require('sinon');

const zookeeper = require('node-zookeeper-client');
const QueuePopulator = require('../../lib/queuePopulator/QueuePopulator');
const constants = require('../../lib/constants');
const { errors } = require('arsenal');

describe('QueuePopulator', () => {
    let qp;
    beforeEach(() => {
        qp = new QueuePopulator({}, {}, {
            logSource: 'bucketd',
        }, null, null, null, null, {});
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
            mockLogReader.batchProcessTimedOut = sinon.fake(() => false);
            mockLogReader.isLogConsumerReady = sinon.fake(() => true);
            qp.logReaders = [
                mockLogReader,
            ];
            qp.zkClient = {
                getState: () => zookeeper.State.SYNC_CONNECTED,
            };
            const response = qp.handleLiveness(mockRes, mockLog);
            assert.strictEqual(response, undefined);
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
            mockLogReader.batchProcessTimedOut = sinon.fake(() => false);
            mockLogReader.isLogConsumerReady = sinon.fake(() => true);
            qp.logReaders = [
                mockLogReader,
            ];
            qp.zkClient = {
                getState: () => zookeeper.State.SYNC_CONNECTED,
            };
            qp.handleLiveness(mockRes, mockLog);
            sinon.assert.calledOnceWithExactly(mockRes.writeHead, 500);
            sinon.assert.calledOnceWithExactly(
                mockRes.end,
                JSON.stringify([
                    {
                        component: 'log reader',
                        status: constants.statusNotReady,
                        topic: 'topicB',
                    },
                ])
            );
        });

        it('returns proper details when batch process timed out', () => {
            const mockLogReader = sinon.spy();
            mockLogReader.getProducerStatus = sinon.fake(() => ({
                topicA: true,
            }));
            mockLogReader.getLogInfo = sinon.fake(() => {});
            mockLogReader.batchProcessTimedOut = sinon.fake(() => true);
            mockLogReader.isLogConsumerReady = sinon.fake(() => true);
            qp.logReaders = [
                mockLogReader,
            ];
            qp.zkClient = {
                getState: () => zookeeper.State.SYNC_CONNECTED,
            };
            qp.handleLiveness(mockRes, mockLog);
            sinon.assert.calledOnceWithExactly(mockRes.writeHead, 500);
            sinon.assert.calledOnceWithExactly(
                mockRes.end,
                JSON.stringify([
                    {
                        component: 'log reader',
                        status: constants.statusTimedOut,
                    },
                ])
            );
        });

        it('returns proper details when log consumer is not ready', () => {
            const mockLogReader = sinon.spy();
            mockLogReader.getProducerStatus = sinon.fake(() => ({
                topicA: true,
            }));
            mockLogReader.getLogInfo = sinon.fake(() => {});
            mockLogReader.batchProcessTimedOut = sinon.fake(() => false);
            mockLogReader.isLogConsumerReady = sinon.fake(() => false);
            qp.logReaders = [
                mockLogReader,
            ];
            qp.zkClient = {
                getState: () => zookeeper.State.SYNC_CONNECTED,
            };
            qp.handleLiveness(mockRes, mockLog);
            sinon.assert.calledOnceWithExactly(mockRes.writeHead, 500);
            sinon.assert.calledOnceWithExactly(
                mockRes.end,
                JSON.stringify([
                    {
                        component: 'log consumer',
                        status: constants.statusNotReady,
                    },
                ])
            );
        });
    });

    describe('_processLogEntries', () => {
        it('should process log records once when no more logs are available', done => {
            qp.qpConfig.exhaustLogSource = true;
            qp.logReaders = [{
                processLogEntries: sinon.stub().yields(null, false),
            }];
            qp._processLogEntries({}, err => {
                assert.ifError(err);
                assert(qp.logReaders[0].processLogEntries.calledOnce);
                return done();
            });
        });

        it('should process log records until no more logs are available', done => {
            qp.qpConfig.exhaustLogSource = true;
            qp.logReaders = [{
                processLogEntries: sinon.stub()
                    .onCall(0).yields(null, true)
                    .onCall(1).yields(null, false),
            }];
            qp._processLogEntries({}, err => {
                assert.ifError(err);
                assert(qp.logReaders[0].processLogEntries.calledTwice);
                return done();
            });
        });

        it('should only process log records once if exhaustLogSource is set to false', done => {
            qp.qpConfig.exhaustLogSource = false;
            qp.logReaders = [{
                processLogEntries: sinon.stub()
                    .onCall(0).yields(null, true)
                    .onCall(1).yields(null, false),
            }];
            qp._processLogEntries({}, err => {
                assert.ifError(err);
                assert(qp.logReaders[0].processLogEntries.calledOnce);
                return done();
            });
        });

        it('should only process log records once if the logReaders need to be updated', done => {
            qp.qpConfig.exhaustLogSource = true;
            qp.logReaders = [{
                processLogEntries: sinon.stub()
                    .onCall(0).yields(null, true)
                    .onCall(1).yields(null, false),
            }];
            qp.logReadersUpdate = true;
            qp._processLogEntries({}, err => {
                assert.ifError(err);
                assert(qp.logReaders[0].processLogEntries.calledOnce);
                return done();
            });
        });

        it('should forward logReader errors', done => {
            qp.qpConfig.exhaustLogSource = true;
            qp.logReaders = [{
                processLogEntries: sinon.stub().yields(errors.InternalError, false),
            }];
            qp._processLogEntries({}, err => {
                assert.deepEqual(err, errors.InternalError);
                assert(qp.logReaders[0].processLogEntries.calledOnce);
                return done();
            });
        });
    });

    describe('close', () => {
        let mockLogReader1;
        let mockLogReader2;
        let mockCircuitBreaker;
        let mockMProducer;
        let mockMConsumer;

        beforeEach(() => {
            mockLogReader1 = {
                close: sinon.stub().yields(),
            };
            mockLogReader2 = {
                close: sinon.stub().yields(),
            };
            mockCircuitBreaker = {
                stop: sinon.stub(),
            };
            mockMProducer = {
                close: sinon.stub().yields(),
            };
            mockMConsumer = {
                close: sinon.stub().yields(),
            };

            qp.logReaders = [mockLogReader1, mockLogReader2];
            qp._circuitBreaker = mockCircuitBreaker;
            qp._mProducer = mockMProducer;
            qp._mConsumer = mockMConsumer;
            qp.raftIdDispatcher = undefined;
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should call close on all logReaders', done => {
            qp.close(err => {
                assert.ifError(err);
                sinon.assert.calledOnce(mockLogReader1.close);
                sinon.assert.calledOnce(mockLogReader2.close);
                sinon.assert.calledOnce(mockCircuitBreaker.stop);
                sinon.assert.calledOnce(mockMProducer.close);
                sinon.assert.calledOnce(mockMConsumer.close);
                return done();
            });
        });

        it('should call close on single logReader', done => {
            qp.logReaders = [mockLogReader1];
            qp.close(err => {
                assert.ifError(err);
                sinon.assert.calledOnce(mockLogReader1.close);
                sinon.assert.notCalled(mockLogReader2.close);
                sinon.assert.calledOnce(mockCircuitBreaker.stop);
                return done();
            });
        });

        it('should handle empty logReaders array', done => {
            qp.logReaders = [];
            qp.close(err => {
                assert.ifError(err);
                sinon.assert.notCalled(mockLogReader1.close);
                sinon.assert.notCalled(mockLogReader2.close);
                sinon.assert.calledOnce(mockCircuitBreaker.stop);
                return done();
            });
        });

        it('should handle logReader close error', done => {
            const closeError = new Error('Close failed');
            mockLogReader1.close = sinon.stub().yields(closeError);
            
            qp.close(err => {
                assert.strictEqual(err, closeError);
                sinon.assert.calledOnce(mockLogReader1.close);
                sinon.assert.notCalled(mockLogReader2.close);
                sinon.assert.notCalled(mockCircuitBreaker.stop);
                return done();
            });
        });

        it('should handle metrics producer close error', done => {
            const closeError = new Error('Metrics producer close failed');
            mockMProducer.close = sinon.stub().yields(closeError);
            
            qp.close(err => {
                assert.strictEqual(err, closeError);
                sinon.assert.calledOnce(mockLogReader1.close);
                sinon.assert.calledOnce(mockLogReader2.close);
                sinon.assert.calledOnce(mockCircuitBreaker.stop);
                sinon.assert.calledOnce(mockMProducer.close);
                sinon.assert.notCalled(mockMConsumer.close);
                return done();
            });
        });

        it('should handle metrics consumer close error', done => {
            const closeError = new Error('Metrics consumer close failed');
            mockMConsumer.close = sinon.stub().yields(closeError);
            
            qp.close(err => {
                assert.strictEqual(err, closeError);
                sinon.assert.calledOnce(mockLogReader1.close);
                sinon.assert.calledOnce(mockLogReader2.close);
                sinon.assert.calledOnce(mockCircuitBreaker.stop);
                sinon.assert.calledOnce(mockMProducer.close);
                sinon.assert.calledOnce(mockMConsumer.close);
                return done();
            });
        });

        it('should work when no metrics producer exists', done => {
            qp._mProducer = null;
            
            qp.close(err => {
                assert.ifError(err);
                sinon.assert.calledOnce(mockLogReader1.close);
                sinon.assert.calledOnce(mockLogReader2.close);
                sinon.assert.calledOnce(mockCircuitBreaker.stop);
                sinon.assert.notCalled(mockMProducer.close);
                sinon.assert.calledOnce(mockMConsumer.close);
                return done();
            });
        });

        it('should work when no metrics consumer exists', done => {
            qp._mConsumer = null;
            
            qp.close(err => {
                assert.ifError(err);
                sinon.assert.calledOnce(mockLogReader1.close);
                sinon.assert.calledOnce(mockLogReader2.close);
                sinon.assert.calledOnce(mockCircuitBreaker.stop);
                sinon.assert.calledOnce(mockMProducer.close);
                sinon.assert.notCalled(mockMConsumer.close);
                return done();
            });
        });
    });
});
