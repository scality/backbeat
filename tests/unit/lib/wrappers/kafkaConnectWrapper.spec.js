const assert = require('assert');
const sinon = require('sinon');
const errors = require('arsenal').errors;
const werelogs = require('werelogs');
const http = require('http');
const events = require('events');

const logger = new werelogs.Logger('connect-wrapper-logger');

const KafkaConnectWrapper =
    require('../../../../lib/wrappers/KafkaConnectWrapper');

describe('KafkaConnectWrapper', () => {
    let wrapper;

    beforeEach(() => {
        wrapper = new KafkaConnectWrapper({
            kafkaConnectHost: 'localhost',
            kafkaConnectPort: 8083,
            logger,
        });
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('constructor', () => {
        it('should fail when params invalid', done => {
            assert.throws(() => new KafkaConnectWrapper({}));
            assert.throws(() => new KafkaConnectWrapper({
                kafkaConnectHost: 'localhost'
            }));
            assert.throws(() => new KafkaConnectWrapper({
                kafkaConnectPort: 8083
            }));
            assert.throws(() => new KafkaConnectWrapper({
                logger,
            }));
            const tempWrapper = new KafkaConnectWrapper({
                kafkaConnectHost: 'localhost',
                kafkaConnectPort: 8083,
                logger,
            });
            assert(tempWrapper instanceof KafkaConnectWrapper);
            return done();
        });
    });

    describe('getConnectors', () => {
        it('should return connectors', async () => {
            const makeRequestStub = sinon.stub(wrapper, 'makeRequest').resolves(
                ['mongo-source-1', 'mongo-source-2']);
            await wrapper.getConnectors()
            .then(connectors => {
                assert.deepEqual(connectors, ['mongo-source-1', 'mongo-source-2']);
                assert(makeRequestStub.calledOnceWith({
                    method: 'GET',
                    path: '/connectors',
                }));
            })
            .catch(err => assert.ifError(err));
        });

        it('should throw error when request fails', async () => {
            sinon.stub(wrapper, 'makeRequest').rejects(errors.InternalError);
            await wrapper.getConnectors()
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });

    describe('getConnectorConfig', () => {
        it('should return connector config', async () => {
            const makeRequestStub = sinon.stub(wrapper, 'makeRequest').resolves({
                'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
                'database': 'metadata',
                'topic.namespace.map': '{"*": "oplog"}',
            });
            await wrapper.getConnectorConfig('mongo-source-1')
            .then(connectorConfig => {
                assert.deepEqual(connectorConfig, {
                    'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
                    'database': 'metadata',
                    'topic.namespace.map': '{"*": "oplog"}',
                });
                assert(makeRequestStub.calledOnceWith({
                    method: 'GET',
                    path: '/connectors/mongo-source-1/config',
                }));
            })
            .catch(err => assert.ifError(err));
        });

        it('should throw error when request fails', async () => {
            sinon.stub(wrapper, 'makeRequest').rejects(errors.InternalError);
            await wrapper.getConnectorConfig('mongo-source-1')
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });

    describe('updateConnectorConfig', () => {
        it('should update connector config', async () => {
            const getConnectorStub = sinon.stub(wrapper, 'getConnectors').resolves(
                ['mongo-source-1']);
            const makeRequestStub = sinon.stub(wrapper, 'makeRequest').resolves({
                example: 'config'
            });
            await wrapper.updateConnectorConfig('mongo-source-1', {})
            .then(newConf => {
                assert.deepEqual(newConf, {
                    example: 'config'
                });
                assert(getConnectorStub.calledOnce);
                assert(makeRequestStub.calledOnceWith({
                    method: 'PUT',
                    path: '/connectors/mongo-source-1/config',
                    headers: {
                        'Content-Type': 'application/json',
                    }
                }));
            })
            .catch(err => assert.ifError(err));
        });

        it('should not update connector config when connector invalid', async () => {
            const makeRequestStub = sinon.stub(wrapper, 'makeRequest').resolves(true);
            const getConnectorStub = sinon.stub(wrapper, 'getConnectors').resolves([]);
            await wrapper.updateConnectorConfig('mongo-source-1', {})
            .then(newConf => {
                assert.strictEqual(newConf, undefined);
                assert(makeRequestStub.notCalled);
                assert(getConnectorStub.calledOnce);
            })
            .catch(err => assert.strictEqual(err.name, errors.InternalError.name));
        });

        it('should throw error when getConnectors fails', async () => {
            sinon.stub(wrapper, 'getConnectors').rejects(errors.InternalError);
            await wrapper.updateConnectorConfig('mongo-source-1', {})
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('should throw error when put fails', async () => {
            sinon.stub(wrapper, 'makeRequest').rejects(errors.InternalError);
            sinon.stub(wrapper, 'getConnectors').resolves(['mongo-source-1']);
            await wrapper.updateConnectorConfig('mongo-source-1', {})
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });

    describe('updateConnectorPipeline', () => {
        it('should update connector pipeline', async () => {
            const getConnectorConfigStub = sinon.stub(wrapper, 'getConnectorConfig').resolves({
                'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
                'database': 'metadata',
                'topic.namespace.map': '{"*": "oplog"}',
                'pipeline': '[]',
            });
            const updateConnectorConfigStub = sinon.stub(wrapper, 'updateConnectorConfig').resolves({
                example: 'config'
            });
            await wrapper.updateConnectorPipeline('mongo-source-1', JSON.stringify([{ $match: { $in: ['bucket1'] } }]))
            .then(newConf => {
                assert.deepEqual(newConf, {
                    example: 'config'
                });
                assert(getConnectorConfigStub.calledOnce);
                assert(updateConnectorConfigStub.calledOnceWith('mongo-source-1', {
                    'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
                    'database': 'metadata',
                    'topic.namespace.map': '{"*": "oplog"}',
                    'pipeline': '[{"$match":{"$in":["bucket1"]}}]',
                }));
            })
            .catch(err => assert.ifError(err));
        });

        it('should not update connector pipeline', async () => {
            const getConnectorConfigStub = sinon.stub(wrapper, 'getConnectorConfig').resolves({
                'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
                'database': 'metadata',
                'topic.namespace.map': '{"*": "oplog"}',
                'pipeline': '[]',
            });
            const updateConnectorConfigStub = sinon.stub(wrapper, 'updateConnectorConfig').resolves({
                example: 'config'
            });
            await wrapper.updateConnectorPipeline('mongo-source-2', [{ $match: { $in: ['bucket1'] } }])
            .then(newConf => {
                assert.deepEqual(newConf, {
                    example: 'config'
                });
                assert(getConnectorConfigStub.calledOnce);
                assert(updateConnectorConfigStub.calledOnce);
            })
            .catch(err => assert.ifError(err));
        });

        it('should throw error when getConnectorConfig', async () => {
            sinon.stub(wrapper, 'getConnectorConfig').rejects(errors.InternalError);
            await wrapper.updateConnectorPipeline('mongo-source-1', {})
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('should throw error when updateConnectorConfig', async () => {
            sinon.stub(wrapper, 'getConnectorConfig').resolves({
                'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
                'database': 'metadata',
                'topic.namespace.map': '{"*": "oplog"}',
                'pipeline': '[]',
            });
            sinon.stub(wrapper, 'updateConnectorConfig').rejects(errors.InternalError);
            await wrapper.updateConnectorPipeline('mongo-source-1', {})
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });

    describe('createConnector', () => {
        it('should create a connector', async () => {
            const makeRequestStub = sinon.stub(wrapper, 'makeRequest').resolves({
                example: 'data'
            });
            const config = {
                name: 'mongo-source',
                config: {
                    key: 'value',
                }
            };
            await wrapper.createConnector(config)
            .then(connectorData => {
                assert.deepEqual(connectorData, {
                    example: 'data'
                });
                assert(makeRequestStub.calledOnceWith({
                    method: 'POST',
                    path: '/connectors',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                }, config));
            })
            .catch(err => assert.ifError(err));
        });

        it('should throw error when request fails', async () => {
            sinon.stub(wrapper, 'makeRequest').rejects(errors.InternalError);
            await wrapper.createConnector({})
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });

    describe('getConnector', () => {
        it('should return a connector', async () => {
            const makeRequestStub = sinon.stub(wrapper, 'makeRequest').resolves({
                name: 'mongo-source',
                config: {},
            });
            await wrapper.getConnector('mongo-source')
            .then(connectorData => {
                assert(makeRequestStub.calledOnceWith({
                    method: 'GET',
                    path: '/connectors/mongo-source',
                }));
                assert.deepEqual(connectorData, {
                    name: 'mongo-source',
                    config: {},
                });
            })
            .catch(err => assert.ifError(err));
        });

        it('should throw error when request fails', async () => {
            sinon.stub(wrapper, 'makeRequest').rejects(errors.InternalError);
            await wrapper.getConnector('mongo-source')
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });

    describe('getConnectorStatus', () => {
        it('should return the status of a connector', async () => {
            const makeRequestStub = sinon.stub(wrapper, 'makeRequest').resolves({
                name: 'mongo-source',
                connector: {},
                tasks: {},
                type: 'source',
            });
            await wrapper.getConnectorStatus('mongo-source')
            .then(connectorStatus => {
                assert(makeRequestStub.calledOnceWith({
                    method: 'GET',
                    path: '/connectors/mongo-source/status',
                }));
                assert.deepEqual(connectorStatus, {
                    name: 'mongo-source',
                    connector: {},
                    tasks: {},
                    type: 'source',
                });
            })
            .catch(err => assert.ifError(err));
        });

        it('should throw error when request fails', async () => {
            sinon.stub(wrapper, 'makeRequest').rejects(errors.InternalError);
            await wrapper.getConnectorStatus('mongo-source')
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });

    describe('deleteConnector', () => {
        it('should only restart connector', async () => {
            const makeRequestStub = sinon.stub(wrapper, 'makeRequest').resolves();
            await wrapper.deleteConnector('mongo-source')
            .then(() =>  assert(makeRequestStub.calledOnceWith({
                method: 'DELETE',
                path: '/connectors/mongo-source',
            })))
            .catch(err => assert.ifError(err));
        });

        it('should throw error when request fails', async () => {
            sinon.stub(wrapper, 'makeRequest').rejects(errors.InternalError);
            await wrapper.deleteConnector('mongo-source')
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });

    describe('makeRequest', () => {
        let response;
        let request;
        let requestStub;
        let endStub;
        let writetStub;

        beforeEach(() => {
            response = new events.EventEmitter();
            request = new events.EventEmitter();
            writetStub = sinon.stub();
            endStub = sinon.stub();
            request.write = writetStub;
            request.end = endStub;
            requestStub = sinon.stub(http, 'request').callsArgWith(1, response)
                .returns(request);
        });

        it('Should make request and return body', async () => {
            const promiseResponse = wrapper.makeRequest({
                method: 'GET',
                path: '/connectors',
            });
            const bodyBuffer = Buffer.from(JSON.stringify([]));
            response.emit('data', bodyBuffer);
            response.emit('end');
            await promiseResponse
            .then(resolvedResponse => {
                assert.deepEqual(resolvedResponse, []);
                assert(requestStub.calledOnceWith({
                    host: 'localhost',
                    port: 8083,
                    method: 'GET',
                    path: '/connectors',
                }));
                assert(endStub.calledOnce);
            })
            .catch(err => assert.ifError(err));
        });

        it('Should fail if request fails', async () => {
            const promiseResponse = wrapper.makeRequest({
                method: 'GET',
                path: '/connectors',
            });
            request.emit('error', errors.InternalError);
            await promiseResponse
            .then(resolvedResponse => assert.strict(resolvedResponse, undefined))
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('Should write data into post request', async () => {
            const promiseResponse = wrapper.makeRequest({
                method: 'POST',
                path: '/connectors',
            }, {
                name: 'connector-name',
                config: {},
            });
            const bodyBuffer = Buffer.from(JSON.stringify([]));
            response.emit('data', bodyBuffer);
            response.emit('end');
            await promiseResponse
            .then(resolvedResponse => {
                assert.deepEqual(resolvedResponse, []);
                assert(requestStub.calledOnceWith({
                    host: 'localhost',
                    port: 8083,
                    method: 'POST',
                    path: '/connectors',
                }));
                assert(writetStub.calledOnceWith(JSON.stringify({
                    name: 'connector-name',
                    config: {},
                })));
                assert(endStub.calledOnce);
            })
            .catch(err => assert.ifError(err));
        });

        it('Should group and parse data chunks', async () => {
            const promiseResponse = wrapper.makeRequest({
                method: 'GET',
                path: '/connectors',
            });
            const bodyBuffer = Buffer.from(JSON.stringify({ key: 'value' }));
            const chunk1 = bodyBuffer.slice(0, Math.floor(bodyBuffer.length / 2));
            const chunk2 = bodyBuffer.slice(Math.floor(bodyBuffer.length / 2),
                bodyBuffer.length);
            response.emit('data', chunk1);
            response.emit('data', chunk2);
            response.emit('end');
            await promiseResponse
            .then(resolvedResponse => {
                assert.deepEqual(resolvedResponse, { key: 'value' });
                assert(requestStub.calledOnceWith({
                    host: 'localhost',
                    port: 8083,
                    method: 'GET',
                    path: '/connectors',
                }));
                assert(endStub.calledOnce);
            })
            .catch(err => assert.ifError(err));
        });

        it('Should fail if it can\'t parse response body', async () => {
            const promiseResponse = wrapper.makeRequest({
                method: 'GET',
                path: '/connectors',
            });
            const bodyBuffer = Buffer.from('{ "key":');
            response.emit('data', bodyBuffer);
            response.emit('end');
            await promiseResponse
            .then(resolvedResponse => {
                assert.deepEqual(resolvedResponse, undefined);
                assert(requestStub.calledOnceWith({
                    host: 'localhost',
                    port: 8083,
                    method: 'GET',
                    path: '/connectors',
                }));
                assert(endStub.calledOnce);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('Should fail if response has bad status code', async () => {
            response.statusCode = 500;
            response.message = 'Server Error';
            const promiseResponse = wrapper.makeRequest({
                method: 'GET',
                path: '/connectors',
            });
            const bodyBuffer = Buffer.from(JSON.stringify([]));
            response.emit('data', bodyBuffer);
            response.emit('end');
            await promiseResponse
            .then(resolvedResponse => {
                assert.deepEqual(resolvedResponse, []);
                assert(requestStub.calledOnceWith({
                    host: 'localhost',
                    port: 8083,
                    method: 'GET',
                    path: '/connectors',
                }));
                assert(endStub.calledOnce);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });
});
