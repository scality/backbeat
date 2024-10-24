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

    describe('_setupMetricsClients', () => {
        let queuePopulatorWithReplication;
        let zkConfig;
        let kafkaConfig;
        let qpConfig;
        let httpsConfig;
        let mConfig;
        let rConfig;
        let vConfig;
        let extConfigs;

        beforeEach(() => {
            zkConfig = { connectionString: 'localhost:2181' };
            kafkaConfig = { hosts: 'localhost:9092' };
            qpConfig = { logSource: 'bucketd', zookeeperPath: '/test' };
            httpsConfig = {};
            mConfig = { topic: 'metrics' };
            rConfig = {};
            vConfig = {};
            extConfigs = { replication: {} };

            queuePopulatorWithReplication = new QueuePopulator(
                zkConfig, kafkaConfig, qpConfig, httpsConfig, mConfig, rConfig, vConfig, extConfigs
            );
        });

        it('should set up metrics clients when replication extension is configured', done => {
            const setupProducerStub = sinon.stub(MetricsProducer.prototype, 'setupProducer').callsFake(cb => cb());
            const startConsumerStub = sinon.stub(MetricsConsumer.prototype, 'start').callsFake(() => {});

            queuePopulatorWithReplication._setupMetricsClients(err => {
                assert.ifError(err);
                assert(queuePopulatorWithReplication._mProducer instanceof MetricsProducer);
                assert(queuePopulatorWithReplication._mConsumer instanceof MetricsConsumer);
                assert(setupProducerStub.calledOnce);
                assert(startConsumerStub.calledOnce);

                setupProducerStub.restore();
                startConsumerStub.restore();
                done();
            });
        });

        it('should not set up metrics clients when replication extension is not configured', done => {
            qp._setupMetricsClients(err => {
                assert.ifError(err);
                assert.strictEqual(qp._mProducer, null);
                assert.strictEqual(qp._mConsumer, null);
                done();
            });
        });
    });
});
