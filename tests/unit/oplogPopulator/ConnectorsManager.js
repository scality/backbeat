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
};

const connector1 = new Connector({
    name: 'source-connector',
    buckets: [],
    config: connectorConfig,
    logger,
    kafkaConnectHost: '127.0.0.1',
    kafkaConnectPort: 8083,
});

describe('ConnectorsManager', () => {
    let connectorsManager;
    beforeEach(() => {
        connectorsManager = new ConnectorsManager({
            nbConnectors: 1,
            database: 'metadata',
            mongoUrl: 'mongodb://localhost:27017/?w=majority&readPreference=primary',
            oplogTopic: 'oplog',
            cronRule: '*/5 * * * * *',
            kafkaConnectHost: '127.0.0.1',
            kafkaConnectPort: 8083,
            metricsHandler: new OplogPopulatorMetrics(logger),
            logger,
        });
    });

    afterEach(() => {
        sinon.restore();
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
            const connector = await connectorsManager.addConnector(false);
            assert(connector instanceof Connector);
            assert.strictEqual(connector.name, 'source-connector');
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
        it('Should return old connector', async () => {
            sinon.stub(connectorsManager._kafkaConnect, 'getConnectorConfig')
                .resolves(connectorConfig);
            sinon.stub(connectorsManager, '_extractBucketsFromConfig').returns([]);
            const connectors = await connectorsManager._getOldConnectors(['source-connector']);
            assert.strictEqual(connectors.length, 1);
            assert.strictEqual(connectors[0].name, 'source-connector');
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
                .resolves(connector1);
            const connectors = await connectorsManager.initializeConnectors();
            assert.deepEqual(connectors, [connector1]);
            assert.deepEqual(connectorsManager._connectors, [connector1]);
            assert.deepEqual(connectorsManager._oldConnectors, []);
        });
    });
});

