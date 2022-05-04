const assert = require('assert');
const sinon = require('sinon');
const errors = require('arsenal').errors;
const werelogs = require('werelogs');

const logger = new werelogs.Logger('connect-wrapper-logger');

const OplogPopulator =
    require('../../../extensions/oplogPopulator/OplogPopulator');
const KafkaConnectWrapper =
    require('../../../extensions/oplogPopulator/KafkaConnectWrapper');

const oplogPopulatorConfig = {
    topic: 'oplog',
    kafkaConnectHost: '127.0.0.1',
    kafkaConnectPort: 8083,
    connectors: [
        {
            name: 'mongo-source'
        }
    ]
};

const mongoConfig = {
    replicaSetHosts:
    'localhost:27017,localhost:27018,localhost:27019',
    writeConcern: 'majority',
    replicaSet: 'rs0',
    readPreference: 'primary',
    database: 'metadata'
};

const connectorConfig = {
    'name': 'mongo-source',
    'database': 'metadata',
    'connection.uri': 'mongodb://localhost:27017,localhost:27018,localhost:27019' +
        '/?w=majority&readPreference=primary&replicaSet=rs0',
    'topic.namespace.map': '{\"*\":\"oplog\"}',
    'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
    'change.stream.full.document': 'updateLookup',
    'pipeline': '[]',
    'collection': '',
};

describe('OplogPopulator', () => {
    let oplogPopulator;

    beforeEach(() => {
        oplogPopulator = new OplogPopulator({
            config: oplogPopulatorConfig,
            mongoConfig,
            logger,
        });
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('constructor', () => {
        it('should fail when params invalid', done => {
            assert.throws(() => new OplogPopulator({}));
            assert.throws(() => new OplogPopulator({
                config: oplogPopulatorConfig,
            }));
            assert.throws(() => new OplogPopulator({
                mongoConfig,
            }));
            assert.throws(() => new OplogPopulator({
                logger,
            }));
            assert.throws(() => new OplogPopulator({
                config: oplogPopulatorConfig,
                mongoConfig,
            }));
            assert.throws(() => new OplogPopulator({
                config: oplogPopulatorConfig,
                logger,
            }));
            assert.throws(() => new OplogPopulator({
                mongoConfig,
                logger,
            }));
            const op = new OplogPopulator({
                config: oplogPopulatorConfig,
                mongoConfig,
                logger,
            });
            assert(op instanceof OplogPopulator);
            return done();
        });

        it('should initialize kafka wrapper', done => {
            const op = new OplogPopulator({
                config: oplogPopulatorConfig,
                mongoConfig,
                logger,
            });
            assert(op._connectWrapper instanceof KafkaConnectWrapper);
            return done();
        });
    });

    describe('setup', () => {
        it('should update a connector', () => {
            const getConnectorsStub = sinon.stub(oplogPopulator._connectWrapper, 'getConnectors')
                .resolves(['mongo-source']);
            const getConf = sinon.stub(oplogPopulator, 'getDefaultConnectorConfiguration').returns(connectorConfig);
            const updateConfStub = sinon.stub(oplogPopulator._connectWrapper, 'updateConnectorConfig').resolves(null);
            const createConnectorStub = sinon.stub(oplogPopulator._connectWrapper, 'createConnector').resolves(null);
            oplogPopulator.setup()
            .then(() => {
                assert(getConnectorsStub.calledOnce);
                assert(getConf.calledOnceWith(oplogPopulatorConfig.connectors[0]));
                assert(updateConfStub.calledOnceWith(connectorConfig));
                assert(createConnectorStub.notCalled);
            })
            .catch(err => assert.ifError(err));
        });

        it('should create a connector', () => {
            const getConnectorsStub = sinon.stub(oplogPopulator._connectWrapper, 'getConnectors').resolves([]);
            const getConf = sinon.stub(oplogPopulator, 'getDefaultConnectorConfiguration').returns(connectorConfig);
            const updateConfStub = sinon.stub(oplogPopulator._connectWrapper, 'updateConnectorConfig').resolves(null);
            const createConnectorStub = sinon.stub(oplogPopulator._connectWrapper, 'createConnector').resolves(null);
            oplogPopulator.setup()
            .then(() => {
                assert(getConnectorsStub.calledOnce);
                assert(getConf.calledOnceWith(oplogPopulatorConfig.connectors[0]));
                assert(updateConfStub.notCalled);
                assert(createConnectorStub.calledWith({
                    name: 'mongo-source',
                    config: connectorConfig,
                }));
            })
            .catch(err => assert.ifError(err));
        });

        it('should throw error if it fails to get connectors', () => {
            const getConnectorsStub = sinon.stub(oplogPopulator._connectWrapper, 'getConnectors')
                .rejects(errors.InternalError);
            const getConf = sinon.stub(oplogPopulator, 'getDefaultConnectorConfiguration').returns(null);
            const updateConfStub = sinon.stub(oplogPopulator._connectWrapper, 'updateConnectorConfig').resolves(null);
            const createConnectorStub = sinon.stub(oplogPopulator._connectWrapper, 'createConnector').resolves(null);
            oplogPopulator.setup()
            .then(() => {
                assert(getConnectorsStub.calledOnce);
                assert(getConf.notCalled);
                assert(updateConfStub.notCalled);
                assert(createConnectorStub.notCalled);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('should throw error if it fails to update connector', () => {
            const getConnectorsStub = sinon.stub(oplogPopulator._connectWrapper, 'getConnectors')
                .resolves(['mongo-source']);
            const getConf = sinon.stub(oplogPopulator, 'getDefaultConnectorConfiguration').returns(connectorConfig);
            const updateConfStub = sinon.stub(oplogPopulator._connectWrapper, 'updateConnectorConfig')
                .rejects(errors.InternalError);
            const createConnectorStub = sinon.stub(oplogPopulator._connectWrapper, 'createConnector').resolves(null);
            oplogPopulator.setup()
            .then(() => {
                assert(getConnectorsStub.calledOnce);
                assert(getConf.calledOnce);
                assert(updateConfStub.calledOnce);
                assert(createConnectorStub.notCalled);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('should throw error if it fails to create connector', () => {
            const getConnectorsStub = sinon.stub(oplogPopulator._connectWrapper, 'getConnectors')
                .resolves(['mongo-source']);
            const getConf = sinon.stub(oplogPopulator, 'getDefaultConnectorConfiguration').returns(connectorConfig);
            const updateConfStub = sinon.stub(oplogPopulator._connectWrapper, 'updateConnectorConfig').resolves(null);
            const createConnectorStub = sinon.stub(oplogPopulator._connectWrapper, 'createConnector')
                .rejects(errors.InternalError);
            oplogPopulator.setup()
            .then(() => {
                assert(getConnectorsStub.calledOnce);
                assert(getConf.calledOnce);
                assert(updateConfStub.notCalled);
                assert(createConnectorStub.calledOnce);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });

    describe('getDefaultConnectorConfiguration', () => {
        it('should return default configuration (relica set)', done => {
            const config = oplogPopulator.getDefaultConnectorConfiguration(
                oplogPopulatorConfig.connectors[0]);
            assert.deepEqual(config, connectorConfig);
            return done();
        });

        it('should return default configuration (sharded)', done => {
            // making config without replica set
            const shardedMongoConfig = Object.assign({}, mongoConfig);
            shardedMongoConfig.replicaSet = '';
            oplogPopulator._mongoConfig = shardedMongoConfig;
            // getting default config
            const config = oplogPopulator.getDefaultConnectorConfiguration(
                oplogPopulatorConfig.connectors[0]);
            // removing replica set from expected default config
            const shardedConnectorConfig = Object.assign({}, connectorConfig);
            shardedConnectorConfig['connection.uri'] = 'mongodb://localhost:27017,' +
                'localhost:27018,localhost:27019/?w=majority&readPreference=primary';
            assert.deepEqual(config, shardedConnectorConfig);
            return done();
        });
    });
});
