const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');

const Connector =
    require('../../../extensions/oplogPopulator/modules/Connector');
const ConnectorsManager =
    require('../../../extensions/oplogPopulator/modules/ConnectorsManager');

const logger = new werelogs.Logger('ConnectorsManager');

const connectorConfig = {
    'name': 'source-connector',
    'database': 'metadata',
    'connection.uri': 'mongodb://localhost:27017/?w=majority&readPreference=primary',
    'topic.namespace.map': '{\"*\":\"oplog\"}',
    'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
    'change.stream.full.document': 'updateLookup',
    'pipeline': '[]',
    'collection': '',
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
            kafkaConnectHost: '127.0.0.1',
            kafkaConnectPort: 8083,
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

    describe('removeConnectorInvalidBuckets', () => {
        it('Should remove invalid buckets from connector', async () => {
            connector1._buckets = new Set(['valid-bucket-1', 'invalid-bucket-1', 'invalid-bucket-2']);
            const removeStub = sinon.stub(connector1, 'removeBucket')
                .resolves();
            sinon.stub(connector1, 'updatePipeline')
                .resolves();
            await connectorsManager.removeConnectorInvalidBuckets(connector1, [
                'valid-bucket-1',
            ]);
            assert(removeStub.getCall(0).calledWith('invalid-bucket-1', false));
            assert(removeStub.getCall(1).calledWith('invalid-bucket-2', false));
        });
    });

    describe('removeInvalidBuckets', () => {
        it('Should remove invalid buckets from old connectors', async () => {
            connectorsManager._oldConnectors = [connector1];
            const removeBucketsStub = sinon.stub(connectorsManager, 'removeConnectorInvalidBuckets')
                .resolves();
            await connectorsManager.removeInvalidBuckets(['valid-bucket-1']);
            assert(removeBucketsStub.calledOnceWith(connector1, ['valid-bucket-1']));
        });
    });
});

