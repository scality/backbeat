const assert = require('assert');
const sinon = require('sinon');
const probeUtils = require('../../../lib/util/probeUtils');

const QueueProcessor =
    require('../../../extensions/replication/queueProcessor/QueueProcessor');

describe('Queue Processor', () => {
    let qp;
    beforeEach(() => {
        qp = new QueueProcessor(
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
            assert.deepStrictEqual(
                JSON.parse(response),
                [
                    {
                        component: 'Replication Status Producer',
                        status: probeUtils.statusUndefined,
                        site: 'site',
                    }, {
                        component: 'Consumer',
                        status: probeUtils.statusUndefined,
                        site: 'site',
                    },
                ]
            );
            // only need to test this once as it matches the response anyway
            sinon.assert.calledOnceWithExactly(
                mockLog.debug,
                sinon.match.any, // we don't care about the debug label
                {
                    replicationStatusProducer: probeUtils.statusUndefined,
                    consumer: probeUtils.statusUndefined,
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
            assert.deepStrictEqual(
                JSON.parse(response),
                [
                    {
                        component: 'Replication Status Producer',
                        status: probeUtils.statusNotReady,
                        site: 'site',
                    }, {
                        component: 'Consumer',
                        status: probeUtils.statusNotReady,
                        site: 'site',
                    },
                ]
            );
        });
    });
});
