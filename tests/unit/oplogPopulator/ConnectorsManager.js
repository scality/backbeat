const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');
const schedule = require('node-schedule');

const Connector =
    require('../../../extensions/oplogPopulator/modules/Connector');
const ConnectorsManager =
    require('../../../extensions/oplogPopulator/modules/ConnectorsManager');
const OplogPopulatorMetrics =
    require('../../../extensions/oplogPopulator/OplogPopulatorMetrics');
const RetainBucketsDecorator = require('../../../extensions/oplogPopulator/allocationStrategy/RetainBucketsDecorator');
const LeastFullConnector = require('../../../extensions/oplogPopulator/allocationStrategy/LeastFullConnector');
const constants = require('../../../extensions/oplogPopulator/constants');
const MultipleBucketsPipelineFactory =
    require('../../../extensions/oplogPopulator/pipeline/MultipleBucketsPipelineFactory');

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
    let connectorRestartStub;

    const pipelineFactory = new MultipleBucketsPipelineFactory();

    beforeEach(() => {
        connector1 = new Connector({
            name: 'source-connector',
            buckets: [],
            config: connectorConfig,
            isRunning: true,
            logger,
            kafkaConnectHost: '127.0.0.1',
            kafkaConnectPort: 8083,
            getPipeline: pipelineFactory.getPipeline,
        });
        connectorCreateStub = sinon.stub(connector1._kafkaConnect, 'createConnector')
            .resolves();
        connectorDeleteStub = sinon.stub(connector1._kafkaConnect, 'deleteConnector')
            .resolves();
        connectorUpdateStub = sinon.stub(connector1._kafkaConnect, 'updateConnectorConfig')
            .resolves();
        connectorRestartStub = sinon.stub(connector1._kafkaConnect, 'restartConnector')
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
            allocationStrategy: new RetainBucketsDecorator(
                // Not needed to test all strategies here: we stub their methods
                new LeastFullConnector({
                    logger,
                    pipelineFactory,
                }),
            ),
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
        it('should generate a random name', () => {
            const connectorName = connectorsManager._generateConnectorName();
            assert(connectorName.startsWith('source-connector-'));
        });

        it('should add prefix to connector name', () => {
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

    describe('_processOldConnectors', () => {
        it('should delete old connector when the strategy rejects it', async () => {
            const config = { ...connectorConfig };
            config['topic.namespace.map'] = 'outdated-topic';
            config['offset.partitiom.name'] = 'partition-name';
            sinon.stub(connectorsManager._kafkaConnect, 'getConnectorConfig')
                .resolves(config);
            sinon.stub(connectorsManager._kafkaConnect, 'deleteConnector');
            const connectors = await connectorsManager._processOldConnectors(['source-connector']);
            assert.strictEqual(connectors.length, 0);
        });

        it('should update connector config while keeping the extra fields', async () => {
            const config = { ...connectorConfig };
            config['topic.namespace.map'] = 'outdated-topic';
            config['offset.partitiom.name'] = 'partition-name';
            sinon.stub(connectorsManager._kafkaConnect, 'getConnectorConfig')
                .resolves(config);
            sinon.stub(connectorsManager._allocationStrategy, 'getOldConnectorBucketList').returns(['bucket1']);
            sinon.stub(connectorsManager._kafkaConnect, 'deleteConnector');
            const connectors = await connectorsManager._processOldConnectors(['source-connector']);
            assert.strictEqual(connectors.length, 1);
            assert.strictEqual(connectors[0].name, 'source-connector');
            assert.strictEqual(connectors[0].config['offset.partitiom.name'], 'partition-name');
            assert.strictEqual(connectors[0].config['topic.namespace.map'], '{"*":"oplog"}');
            assert.strictEqual(connectors[0].isRunning, true);
        });

        it('should warn when the number of retrieved bucket in a connector exceeds the limit', async () => {
            const config = { ...connectorConfig };
            config['topic.namespace.map'] = 'outdated-topic';
            config['offset.partitiom.name'] = 'partition-name';
            sinon.stub(connectorsManager._allocationStrategy, 'maximumBucketsPerConnector').value(1);
            sinon.stub(connectorsManager._kafkaConnect, 'getConnectorConfig')
                .resolves(config);
            sinon.stub(connectorsManager._allocationStrategy, 'getOldConnectorBucketList')
                .returns(['bucket1', 'bucket2']);
            const warnStub = sinon.stub(connectorsManager._logger, 'warn');
            const connectors = await connectorsManager.
                _processOldConnectors(['source-connector', 'source-connector-2']);
            assert.strictEqual(connectors.length, 2);
            assert(warnStub.called);
        });
    });

    describe('initializeConnectors', () => {
        it('should initialize old connector', async () => {
            connectorsManager._nbConnectors = 1;
            sinon.stub(connectorsManager._kafkaConnect, 'getConnectors')
                .resolves(['source-connector']);
            sinon.stub(connectorsManager, '_processOldConnectors')
                .resolves([connector1]);
            const connectors = await connectorsManager.initializeConnectors();
            assert.deepEqual(connectors, [connector1]);
            assert.deepEqual(connectorsManager._connectors, [connector1]);
            assert.deepEqual(connectorsManager._oldConnectors, [connector1]);
        });

        it('should add more connectors', async () => {
            connectorsManager._nbConnectors = 1;
            sinon.stub(connectorsManager._kafkaConnect, 'getConnectors')
                .resolves([]);
            sinon.stub(connectorsManager, 'addConnector')
                .callsFake(() => {
                    connectorsManager._connectors.push(connector1);
                    return connector1;
                });
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

        it('should emit event when destroying connector', async () => {
            connector1._isRunning = true;
            connector1._state.bucketsGotModified = false;
            connector1._buckets = new Set();
            const emitStub = sinon.stub(connector1, 'emit');
            await connectorsManager._spawnOrDestroyConnector(connector1);
            assert(emitStub.calledOnceWith(constants.connectorUpdatedEvent, connector1));
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

        it('should do nothing if the strategy does not allow to update', async () => {
            connector1._isRunning = true;
            connector1._state.bucketsGotModified = false;
            connector1._buckets = new Set(['bucket1']);
            sinon.stub(connectorsManager._allocationStrategy, 'canUpdate')
                .resolves(false);
            const updated = await connectorsManager._spawnOrDestroyConnector(connector1);
            assert.strictEqual(updated, false);
            assert(connectorCreateStub.notCalled);
            assert(connectorDeleteStub.notCalled);
        });
    });

    describe('_updateConnectors', () => {
        it('should update a running connector when its buckets changed', async () => {
            sinon.stub(connectorsManager, '_validateConnectorState').resolves();
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
            sinon.stub(connectorsManager, '_validateConnectorState').resolves();
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

    describe('_validateConnectorState', () => {
        it('should restart a connector when tasks are failed', async () => {
            const getStatusStub = sinon.stub(connectorsManager._kafkaConnect, 'getConnectorStatus')
                .resolves({
                    name: 'connector1',
                    connector: {
                        state: 'RUNNING',
                    },
                    tasks:
                    [
                        {
                            id: 0,
                            state: 'RUNNING',
                        },
                        {
                            id: 1,
                            state: 'FAILED',
                            trace: 'org.apache.kafka.common.errors.RecordTooLargeException\n'
                        }
                    ]
                });
            connector1._isRunning = true;
            await connectorsManager._validateConnectorState(connector1);
            assert(getStatusStub.called);
            assert(connectorRestartStub.called);
        });

        it('should restart a connector when the connector instance failed', async () => {
            const getStatusStub = sinon.stub(connectorsManager._kafkaConnect, 'getConnectorStatus')
                .resolves({
                    name: 'connector1',
                    connector: {
                        state: 'FAILED',
                    },
                    tasks: []
                });
            connector1._isRunning = true;
            await connectorsManager._validateConnectorState(connector1);
            assert(getStatusStub.called);
            assert(connectorRestartStub.called);
        });

        it('should do nothing when connector and tasks are running', async () => {
            const getStatusStub = sinon.stub(connectorsManager._kafkaConnect, 'getConnectorStatus')
                .resolves({
                    name: 'connector1',
                    connector: {
                        state: 'RUNNING',
                    },
                    tasks: [
                        {
                            id: 0,
                            state: 'RUNNING',
                        },
                    ]
                });
            connector1._isRunning = true;
            await connectorsManager._validateConnectorState(connector1);
            assert(getStatusStub.called);
            assert(connectorRestartStub.notCalled);
        });

        it('should do nothing when connector is not spawned', async () => {
            const getStatusStub = sinon.stub(connectorsManager._kafkaConnect, 'getConnectorStatus')
                .resolves({});
            connector1._isRunning = false;
            await connectorsManager._validateConnectorState(connector1);
            assert(getStatusStub.notCalled);
            assert(connectorRestartStub.notCalled);
        });
    });

    describe('scheduleConnectorUpdates', () => {
        afterEach(() => {
            sinon.restore();
        });

        it('should schedule connector updates', () => {
            const updateConnectorsStub = sinon.stub(connectorsManager, '_updateConnectors');
            sinon.stub(schedule, 'scheduleJob').callsFake((rule, cb) => {
                cb();
            });
            connectorsManager.scheduleConnectorUpdates();
            assert(updateConnectorsStub.called);
        });
    });
});

