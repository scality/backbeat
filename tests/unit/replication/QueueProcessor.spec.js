const assert = require('assert');
const sinon = require('sinon');
const constants = require('../../../lib/constants');

const QueueProcessor =
    require('../../../extensions/replication/queueProcessor/QueueProcessor');

function getQueueProcessorConfig() {
    return [
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
        {
            transport: 'http',
            auth: { type: 'role' },
            replicationEndpoint: {
                site: 'site-crr',
                servers: ['site-crr:8000'],
            },
        },
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
        null,
        { topic: 'metrics-test-topic' },
        {},
        {},
        'site-crr',
    ];
}

describe('Queue Processor', () => {
    let qp;
    beforeEach(() => {
        qp = new QueueProcessor(...getQueueProcessorConfig());
    });

    afterEach(() => {
        sinon.restore();
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
                        site: 'site-crr',
                    }, {
                        component: 'Consumer',
                        status: constants.statusUndefined,
                        site: 'site-crr',
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
                        site: 'site-crr',
                    }, {
                        component: 'Consumer',
                        status: constants.statusNotReady,
                        site: 'site-crr',
                    },
                ]
            );
        });
    });

    describe('constructor', () => {
        it('should use s3c site\'s host as a destination host', () => {
            const config = getQueueProcessorConfig();
            config[5].replicationEndpoint = {
                site: 'site-crr',
                servers: ['site-crr:8000'],
            };
            config[11] = 'site-crr';
            const qp = new QueueProcessor(...config);
            assert.deepStrictEqual(qp.destHosts.pickHost(), {
                host: 'site-crr',
                port: 8000,
            });
        });
        it('should setup echo mode when configured for site', () => {
            const config = getQueueProcessorConfig();
            config[5].replicationEndpoint = {
                site: 'site-crr',
                servers: ['site-crr:8000'],
                echo: true,
            };
            config[11] = 'site-crr';
            const setupEchoStub = sinon.stub(QueueProcessor.prototype, '_setupEcho').returns();
            const qp = new QueueProcessor(...config);
            assert.deepStrictEqual(qp.destHosts.pickHost(), {
                host: 'site-crr',
                port: 8000,
            });
            assert(setupEchoStub.calledOnce);
        });
        it('should not set destination host when using a non scality type site', () => {
            const config = getQueueProcessorConfig();
            config[5].replicationEndpoint = {
                site: 'site_aws',
                type: 's3_aws',
            };
            config[11] = 'site_aws';
            const qp = new QueueProcessor(...config);
            assert.strictEqual(qp.destHosts, null);
        });

        describe('default port based on transport', () => {
            it('should use port 443 for HTTPS transport with explicit port 443', () => {
                const config = getQueueProcessorConfig();
                config[5].transport = 'https';
                config[5].replicationEndpoint = {
                    site: 'site-crr',
                    servers: ['s3.example.com:443'],
                };
                config[11] = 'site-crr';
                const qp = new QueueProcessor(...config);
                assert.deepStrictEqual(qp.destHosts.pickHost(), {
                    host: 's3.example.com',
                    port: 443,
                });
            });

            it('should use port 443 as default for HTTPS transport when server has no port', () => {
                const config = getQueueProcessorConfig();
                config[5].transport = 'https';
                config[5].replicationEndpoint = {
                    site: 'site-crr',
                    servers: ['s3.example.com'],
                };
                config[11] = 'site-crr';
                const qp = new QueueProcessor(...config);
                assert.deepStrictEqual(qp.destHosts.pickHost(), {
                    host: 's3.example.com',
                    port: 443,
                });
            });

            it('should use port 80 for HTTP transport with explicit port 80', () => {
                const config = getQueueProcessorConfig();
                config[5].transport = 'http';
                config[5].replicationEndpoint = {
                    site: 'site-crr',
                    servers: ['s3.example.com:80'],
                };
                config[11] = 'site-crr';
                const qp = new QueueProcessor(...config);
                assert.deepStrictEqual(qp.destHosts.pickHost(), {
                    host: 's3.example.com',
                    port: 80,
                });
            });

            it('should use port 80 as default for HTTP transport when server has no port', () => {
                const config = getQueueProcessorConfig();
                config[5].transport = 'http';
                config[5].replicationEndpoint = {
                    site: 'site-crr',
                    servers: ['s3.example.com'],
                };
                config[11] = 'site-crr';
                const qp = new QueueProcessor(...config);
                assert.deepStrictEqual(qp.destHosts.pickHost(), {
                    host: 's3.example.com',
                    port: 80,
                });
            });

            it('should use explicit non-standard port for HTTPS transport', () => {
                const config = getQueueProcessorConfig();
                config[5].transport = 'https';
                config[5].replicationEndpoint = {
                    site: 'site-crr',
                    servers: ['s3.example.com:8443'],
                };
                config[11] = 'site-crr';
                const qp = new QueueProcessor(...config);
                assert.deepStrictEqual(qp.destHosts.pickHost(), {
                    host: 's3.example.com',
                    port: 8443,
                });
            });

            it('should use explicit non-standard port for HTTP transport', () => {
                const config = getQueueProcessorConfig();
                config[5].transport = 'http';
                config[5].replicationEndpoint = {
                    site: 'site-crr',
                    servers: ['s3.example.com:8080'],
                };
                config[11] = 'site-crr';
                const qp = new QueueProcessor(...config);
                assert.deepStrictEqual(qp.destHosts.pickHost(), {
                    host: 's3.example.com',
                    port: 8080,
                });
            });
        });
    });
});
