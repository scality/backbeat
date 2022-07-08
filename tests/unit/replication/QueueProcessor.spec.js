const assert = require('assert');
const sinon = require('sinon');
const constants = require('../../../lib/constants');

const QueueProcessor =
    require('../../../extensions/replication/queueProcessor/QueueProcessor');

describe('Queue Processor', () => {
    let qp;
    beforeEach(() => {
        qp = new QueueProcessor(
            'backbeat-func-test-dummy-topic',
            { connectionString: '127.0.0.1:2181/backbeat',
              autoCreateNamespace: false },
            null,
            { hosts: 'localhost:9092' },
            { auth: { type: 'role',
                vault: { host: 'localhost:9093',
                    port: 7777 } },
                s3: { host: 'localhost:9094',
                    port: 7777 },
                transport: 'http',
            },
            { auth: { type: 'role' },
                bootstrapList: [{
                    site: 'site', servers: ['localhost:9095'],
                }, {
                    site: 'toazure', type: 'azure',
                }],
                transport: 'http' },
            { topic: 'backbeat-func-test-dummy-topic',
              replicationStatusTopic: 'backbeat-func-test-repstatus',
              queueProcessor: {
                  retry: {
                      scality: { timeoutS: 5 },
                      azure: { timeoutS: 5 },
                  },
                  groupId: 'backbeat-func-test-group-id',
                  mpuPartsConcurrency: 10,
              },
            },
            { host: '127.0.0.1',
              port: 6379 },
            { topic: 'metrics-test-topic' },
            {},
            {},
            'site',
        );
    });

    describe('handle liveness', () => {
        let mockLog;
        beforeEach(() => {
            mockLog = sinon.spy();
            mockLog.debug = sinon.spy();
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
            const response = qp.handleLiveness(mockLog);
            assert.deepStrictEqual(response, []);
        });

        it('with undefined components', () => {
            const response = qp.handleLiveness(mockLog);
            assert.deepStrictEqual(
                response,
                [
                    {
                        component: 'Replication Status Producer',
                        status: constants.statusUndefined,
                        site: 'site',
                    }, {
                        component: 'Consumer',
                        status: constants.statusUndefined,
                        site: 'site',
                    },
                ]
            );
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
            const response = qp.handleLiveness(mockLog);
            assert.deepStrictEqual(
                response,
                [
                    {
                        component: 'Replication Status Producer',
                        status: constants.statusNotReady,
                        site: 'site',
                    }, {
                        component: 'Consumer',
                        status: constants.statusNotReady,
                        site: 'site',
                    },
                ]
            );
        });
    });
});
