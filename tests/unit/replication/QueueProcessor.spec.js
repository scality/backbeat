const assert = require('assert');
const sinon = require('sinon');
const constants = require('../../../lib/constants');
const promClient = require('prom-client');

const QueueProcessor =
    require('../../../extensions/replication/queueProcessor/QueueProcessor');

describe('Queue Processor', () => {
    let qp;
    beforeEach(() => {
        qp = new QueueProcessor(
            'backbeat-func-test-dummy-topic',
            { hosts: 'localhost:9092' },
            {
                auth: {
                    type: 'role',
                    vault: {
                        host: 'localhost:9093',
                        port: 7777,
                    },
                },
                s3: {
                    host: 'localhost:9094',
                    port: 7777,
                },
                transport: 'http',
            },
            {
                auth: {
                    type: 'role',
                },
                bootstrapList: [
                    { site: 'sf', servers: ['localhost:9094'] },
                ],
                transport: 'http',
            },
            {
                topic: 'backbeat-func-test-dummy-topic',
                replicationStatusTopic: 'backbeat-func-test-repstatus',
                queueProcessor: {
                    retryTimeoutS: 5,
                    groupId: 'backbeat-func-test-group-id',
                    mpuPartsConcurrency: 10,
                },
            },
            { }, { }, 'site', undefined
        );
    });

    afterEach(() => {
        sinon.restore();
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

        it('with ready components', () => {
            const mockConsumer = {
                isReady: sinon.stub().returns(true),
            };
            const mockProducer = {
                isReady: sinon.stub().returns(true),
            };

            qp.replicationStatusProducer = mockProducer;
            qp._consumer = mockConsumer;
            const response = qp.handleLiveness(mockRes, mockLog);
            assert.strictEqual(response, undefined);
            sinon.assert.calledOnceWithExactly(mockRes.writeHead, 200);
            sinon.assert.calledOnce(mockRes.end);
        });

        it('with undefined components', () => {
            const response = qp.handleLiveness(mockRes, mockLog);
            assert.strictEqual(response, undefined);

            sinon.assert.calledOnceWithExactly(mockRes.writeHead, 500);
            const expectedRes = JSON.stringify([
                {
                    component: 'Replication Status Producer',
                    status: constants.statusUndefined,
                    site: 'site',
                }, {
                    component: 'Consumer',
                    status: constants.statusUndefined,
                    site: 'site',
                },
            ]);
            sinon.assert.calledOnceWithExactly(mockRes.end, expectedRes);
            // only need to test this once as it matches the response anyway
            sinon.assert.calledOnceWithExactly(
                mockLog.debug,
                sinon.match.any, // we don't care about the debug label
                {
                    replicationStatusProducer: constants.statusUndefined,
                    consumer: constants.statusUndefined,
                }
            );
        });

        it('with not ready components', () => {
            const mockConsumer = {
                isReady: sinon.stub().returns(false),
            };
            const mockProducer = {
                isReady: sinon.stub().returns(false),
            };

            qp.replicationStatusProducer = mockProducer;
            qp._consumer = mockConsumer;
            const response = qp.handleLiveness(mockRes, mockLog);
            assert.strictEqual(response, undefined);
            sinon.assert.calledOnceWithExactly(mockRes.writeHead, 500);
            const expectedRes = [
                {
                    component: 'Replication Status Producer',
                    status: constants.statusNotReady,
                    site: 'site',
                }, {
                    component: 'Consumer',
                    status: constants.statusNotReady,
                    site: 'site',
                },
            ];
            sinon.assert.calledOnceWithExactly(mockRes.end, JSON.stringify(expectedRes));

            sinon.assert.calledOnceWithExactly(
                mockLog.error,
                'sending back error response',
                {
                    httpCode: 500,
                    error: expectedRes,
                },
            );
        });
    });

    describe('handle metrics', () => {
        let res;
        let log;
        let fakeMetrics;

        beforeEach(() => {
            res = { writeHead: sinon.spy(), end: sinon.spy() };
            log = { debug: sinon.spy() };

            fakeMetrics = 'test_metrics';
            sinon.stub(promClient.register, 'metrics').resolves(fakeMetrics);

            const mockConsumer = {
                consumerStats: { lag: { partition1: 5, partition2: 10 } },
            };

            qp._consumer = mockConsumer;
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should handle metrics correctly', async () => {
            const r = await qp.handleMetrics(res, log);
            assert.strictEqual(r, undefined);

            sinon.assert.calledOnce(log.debug);
            sinon.assert.calledWithExactly(log.debug, 'metrics requested');
            sinon.assert.calledOnceWithExactly(promClient.register.metrics);
            sinon.assert.calledOnceWithExactly(res.writeHead, 200, { 'Content-Type': promClient.register.contentType });
            sinon.assert.calledOnceWithExactly(res.end, fakeMetrics);
        });
    });
});
