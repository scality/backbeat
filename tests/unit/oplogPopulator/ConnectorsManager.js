const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');

const Connector =
    require('../../../extensions/oplogPopulator/modules/Connector');
const ConnectorsManager =
    require('../../../extensions/oplogPopulator/modules/ConnectorsManager');
const OplogPopulatorMetrics =
    require('../../../extensions/oplogPopulator/OplogPopulatorMetrics');

const logger = new werelogs.Logger('ConnectorsManager');

const connectorConfig = {
    'name': 'source-connector',
    'database': 'metadata',
    'connection.uri': 'mongodb://localhost:27017/?w=majority&readPreference=primary',
    'topic.namespace.map': '{\"*\":\"oplog\"}',
    'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
    'pipeline': '[]',
    'collection': '',
    'startup.mode': 'timestamp',
    'output.format.value': 'json',
    'value.converter.schemas.enable': false,
    'value.converter': 'org.apache.kafka.connect.storage.StringConverter',
    'output.format.key': 'schema',
    'output.schema.key': JSON.stringify({
        type: 'record',
        name: 'keySchema',
        fields: [{
            name: 'ns',
            type: [{
                    name: 'ns',
                    type: 'record',
                    fields: [{
                        name: 'coll',
                        type: ['string', 'null'],
                    }],
                }, 'null'],
        }, {
            name: 'fullDocument',
            type: [{
               type: 'record',
               name: 'fullDocumentRecord',
               fields: [{
                    name: 'value',
                    type: [{
                        type: 'record',
                        name: 'valueRecord',
                        fields: [{
                            name: 'key',
                            type: ['string', 'null'],
                        }],
                    }, 'null'],
               }],
            }, 'null'],
        }],
    }),
    'heartbeat.interval.ms': 10000,
};

describe('ConnectorsManager', () => {
    let connectorsManager;
    let connector1;

    let connectorCreateStub;
    let connectorDeleteStub;
    let connectorUpdateStub;

    beforeEach(() => {
        connector1 = new Connector({
            name: 'source-connector',
            buckets: [],
            config: connectorConfig,
            isRunning: true,
            logger,
            kafkaConnectHost: '127.0.0.1',
            kafkaConnectPort: 8083,
        });
        connectorCreateStub = sinon.stub(connector1._kafkaConnect, 'createConnector')
            .resolves();
        connectorDeleteStub = sinon.stub(connector1._kafkaConnect, 'deleteConnector')
            .resolves();
        connectorUpdateStub = sinon.stub(connector1._kafkaConnect, 'updateConnectorConfig')
            .resolves();
        connectorsManager = new ConnectorsManager({
            nbConnectors: 1,
            database: 'metadata',
            mongoUrl: 'mongodb://localhost:27017/?w=majority&readPreference=primary',
            oplogTopic: 'oplog',
            cronRule: '*/5 * * * * *',
            heartbeatIntervalMs: 10000,
            kafkaConnectHost: '127.0.0.1',
            kafkaConnectPort: 8083,
            metricsHandler: new OplogPopulatorMetrics(logger),
            logger,
        });
    });

    afterEach(() => {
        sinon.reset();
    });

    describe('_getDefaultConnectorConfiguration', () => {
        it('should return default configuration', () => {
            const config = connectorsManager._getDefaultConnectorConfiguration(
                'source-connector');
            assert.deepEqual(config, connectorConfig);
        });
    });

    describe('_generateConnectorName', () => {
        it('Should generate a random name', () => {
            const connectorName = connectorsManager._generateConnectorName();
            assert(connectorName.startsWith('source-connector-'));
        });

        it('Should add prefix to connector name', () => {
            connectorsManager._prefix = 'pfx-';
            const connectorName = connectorsManager._generateConnectorName();
            assert(connectorName.startsWith('pfx-source-connector-'));
        });
    });

    describe('addConnector', () => {
        it('should create a connector', async () => {
            sinon.stub(connectorsManager, '_generateConnectorName')
                .returns('source-connector');
            sinon.stub(connectorsManager, '_getDefaultConnectorConfiguration')
                .returns(connectorConfig);
            const connector = connectorsManager.addConnector();
            assert(connector instanceof Connector);
            assert.strictEqual(connector.name, 'source-connector');
            assert.strictEqual(connector.isRunning, false);
        });
    });

    describe('_extractBucketsFromConfig', () => {
        it('should extract buckets from connector config', () => {
            const config = {
                pipeline: JSON.stringify([{
                    $match: {
                        'ns.coll': {
                            $in: ['example-bucket-1, example-bucket-2'],
                        }
                    }
                }])
            };
            const buckets = connectorsManager._extractBucketsFromConfig(config);
            assert.deepEqual(buckets, ['example-bucket-1, example-bucket-2']);
        });
    });

    describe('_getOldConnectors', () => {
        it('Should update connector config while keeping the extra fields', async () => {
            const config = { ...connectorConfig };
            config['topic.namespace.map'] = 'outdated-topic';
            config['offset.partitiom.name'] = 'partition-name';
            sinon.stub(connectorsManager._kafkaConnect, 'getConnectorConfig')
                .resolves(config);
            const connectors = await connectorsManager._getOldConnectors(['source-connector']);
            assert.strictEqual(connectors.length, 1);
            assert.strictEqual(connectors[0].name, 'source-connector');
            assert.strictEqual(connectors[0].config['offset.partitiom.name'], 'partition-name');
            assert.strictEqual(connectors[0].config['topic.namespace.map'], '{"*":"oplog"}');
            assert.strictEqual(connectors[0].isRunning, true);
        });
    });

    describe('initializeConnectors', () => {
        it('Should initialize old connector', async () => {
            connectorsManager._nbConnectors = 1;
            sinon.stub(connectorsManager._kafkaConnect, 'getConnectors')
                .resolves(['source-connector']);
            sinon.stub(connectorsManager, '_getOldConnectors')
                .resolves([connector1]);
            const connectors = await connectorsManager.initializeConnectors();
            assert.deepEqual(connectors, [connector1]);
            assert.deepEqual(connectorsManager._connectors, [connector1]);
            assert.deepEqual(connectorsManager._oldConnectors, [connector1]);
        });

        it('Should add more connectors', async () => {
            connectorsManager._nbConnectors = 1;
            sinon.stub(connectorsManager._kafkaConnect, 'getConnectors')
                .resolves([]);
            sinon.stub(connectorsManager, 'addConnector')
                .returns(connector1);
            const connectors = await connectorsManager.initializeConnectors();
            assert.deepEqual(connectors, [connector1]);
            assert.deepEqual(connectorsManager._connectors, [connector1]);
            assert.deepEqual(connectorsManager._oldConnectors, []);
        });
    });

    describe('_spawnOrDestroyConnector', () => {
        it('should destroy running connector when no buckets are configured', async () => {
            connector1._isRunning = true;
            connector1._state.bucketsGotModified = false;
            connector1._buckets = new Set();
            const updated = await connectorsManager._spawnOrDestroyConnector(connector1);
            assert.strictEqual(updated, true);
            assert(connectorCreateStub.notCalled);
            assert(connectorDeleteStub.calledOnceWith(connector1.name));
        });

        it('should spawn a non running connector when buckets are configured', async () => {
            connector1._isRunning = false;
            connector1._state.bucketsGotModified = false;
            connector1._buckets = new Set(['bucket1']);
            const updated = await connectorsManager._spawnOrDestroyConnector(connector1);
            assert.strictEqual(updated, true);
            assert(connectorCreateStub.calledOnceWith({
                name: connector1.name,
                config: connector1.config
            }));
            assert(connectorDeleteStub.notCalled);
        });

        it('should do nothing when a running connector has buckets', async () => {
            connector1._isRunning = true;
            connector1._state.bucketsGotModified = false;
            connector1._buckets = new Set(['bucket1']);
            const updated = await connectorsManager._spawnOrDestroyConnector(connector1);
            assert.strictEqual(updated, false);
            assert(connectorCreateStub.notCalled);
            assert(connectorDeleteStub.notCalled);
        });

        it('should do nothing when a non running connector still has no buckets', async () => {
            connector1._isRunning = false;
            connector1._state.bucketsGotModified = false;
            connector1._buckets = new Set();
            const updated = await connectorsManager._spawnOrDestroyConnector(connector1);
            assert.strictEqual(updated, false);
            assert(connectorCreateStub.notCalled);
            assert(connectorDeleteStub.notCalled);
        });
    });

    describe('_updateConnectors', () => {
        it('should update a running connector when its buckets changed', async () => {
            connector1._isRunning = true;
            connector1._state.bucketsGotModified = false;
            connector1._buckets = new Set(['bucket1']);
            connectorsManager._connectors = [connector1];
            connector1._buckets = new Set(['bucket1']);
            connector1.addBucket('bucket2', false);
            await connectorsManager._updateConnectors();
            assert(connectorCreateStub.notCalled);
            assert(connectorDeleteStub.notCalled);
            assert(connectorUpdateStub.calledOnceWith(
                connector1.name,
                connector1.config
            ));
        });
        it('should not update a running connector when its buckets didn\'t change', async () => {
            connector1._isRunning = true;
            connector1._state.bucketsGotModified = false;
            connector1._buckets = new Set(['bucket1']);
            connectorsManager._connectors = [connector1];
            await connectorsManager._updateConnectors();
            assert(connectorCreateStub.notCalled);
            assert(connectorDeleteStub.notCalled);
            assert(connectorUpdateStub.notCalled);
        });
        it('should destroy a running connector if no buckets are assigned to it', async () => {
            connector1._isRunning = true;
            connector1._state.bucketsGotModified = false;
            connector1._buckets = new Set([]);
            connectorsManager._connectors = [connector1];
            await connectorsManager._updateConnectors();
            assert(connectorCreateStub.notCalled);
            assert(connectorDeleteStub.calledOnceWith(connector1.name));
            assert(connectorUpdateStub.notCalled);
        });
        it('should spawn a non running connector when buckets are assigned to it', async () => {
            connector1._isRunning = false;
            connector1._state.bucketsGotModified = false;
            connector1._buckets = new Set([]);
            connectorsManager._connectors = [connector1];
            connector1._buckets = new Set(['bucket1']);
            await connectorsManager._updateConnectors();
            assert(connectorCreateStub.calledOnceWith({
                name: connector1.name,
                config: connector1.config
            }));
            assert(connectorDeleteStub.notCalled);
            assert(connectorUpdateStub.notCalled);
        });
        it('should do nothing when a non running connector has not buckets', async () => {
            connector1._isRunning = false;
            connector1._state.bucketsGotModified = false;
            connector1._buckets = new Set([]);
            connectorsManager._connectors = [connector1];
            await connectorsManager._updateConnectors();
            assert(connectorCreateStub.notCalled);
            assert(connectorDeleteStub.notCalled);
            assert(connectorUpdateStub.notCalled);
        });
    });
});

