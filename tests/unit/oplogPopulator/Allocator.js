const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');

const Allocator =
    require('../../../extensions/oplogPopulator/modules/Allocator');
const Connector =
    require('../../../extensions/oplogPopulator/modules/Connector');
const OplogPopulatorMetrics =
    require('../../../extensions/oplogPopulator/OplogPopulatorMetrics');
const LeastFullConnector = require('../../../extensions/oplogPopulator/allocationStrategy/LeastFullConnector');
const RetainBucketsDecorator = require('../../../extensions/oplogPopulator/allocationStrategy/RetainBucketsDecorator');

const logger = new werelogs.Logger('Allocator');

const defaultConnectorParams = {
    config: {},
    isRunning: false,
    logger,
    kafkaConnectHost: '127.0.0.1',
    kafkaConnectPort: 8083,
};

const connector1 = new Connector({
    name: 'example-connector-1',
    buckets: ['example-bucket-1'],
    ...defaultConnectorParams,
});

describe('Allocator', () => {
    let allocator;
    beforeEach(() => {
        allocator = new Allocator({
            connectorsManager: {
                connectors: [],
                addConnector: () => connector1,
            },
            metricsHandler: new OplogPopulatorMetrics(logger),
            allocationStrategy: new RetainBucketsDecorator(
                // Not needed to test all strategies here: we stub their methods
                new LeastFullConnector({
                    logger,
                }),
                { logger, }
            ),
            logger,
        });
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('_initConnectorToBucketMap', () => {
        it('should initialize map', () => {
            allocator._connectorsManager.connectors = [connector1];
            allocator._initConnectorToBucketMap();
            const connector = allocator._bucketsToConnectors.get('example-bucket-1');
            assert.strictEqual(connector.name, 'example-connector-1');
            assert.strictEqual(allocator._bucketsToConnectors.size, 1);
        });
    });

    describe('has', () => {
        it('should return true if bucket exist', () => {
            allocator._bucketsToConnectors.set('example-bucket-1', connector1);
            const exists = allocator.has('example-bucket-1');
            assert.strictEqual(exists, true);
        });

        it('should return false if bucket doesn\'t exist', () => {
            const exists = allocator.has('example-bucket-2');
            assert.strictEqual(exists, false);
        });
    });

    describe('listenToBucket', () => {
        it('should handle errors', async () => {
            sinon.stub(connector1, 'addBucket').rejects(new Error('error'));
            assert.rejects(allocator.listenToBucket('example-bucket-2'));
        });

        it('should listen to bucket if it wasn\'t assigned before', async () => {
            allocator._connectorsManager.connectors = [connector1];
            const getConnectorStub = sinon.stub(allocator._allocationStrategy, 'getConnector')
                .returns(connector1);
            const addBucketStub = sinon.stub(connector1, 'addBucket').resolves();
            await allocator.listenToBucket('example-bucket-1');
            assert(getConnectorStub.calledOnceWith([connector1], 'example-bucket-1'));
            assert(addBucketStub.calledOnceWith('example-bucket-1'));
            const assignedConnector = allocator._bucketsToConnectors.get('example-bucket-1');
            assert.deepEqual(assignedConnector, connector1);
        });

        it('should not listen to bucket it was assigned before', async () => {
            allocator._bucketsToConnectors.set('example-bucket-1', connector1);
            const getConnectorStub = sinon.stub(allocator._allocationStrategy, 'getConnector')
                .returns(connector1);
            const addBucketStub = sinon.stub(connector1, 'addBucket').resolves();
            await allocator.listenToBucket('example-bucket-1');
            assert(getConnectorStub.notCalled);
            assert(addBucketStub.notCalled);
        });

        it('should add a connector if the strategy returns null', () => {
            allocator._connectorsManager.connectors = [connector1];
            sinon.stub(allocator._allocationStrategy, 'getConnector').returns(null);
            const addConnectorStub = sinon.stub(allocator._connectorsManager, 'addConnector');
            allocator.listenToBucket('example-bucket-2');
            assert(addConnectorStub.calledOnce);
        });
    });

    describe('stopListeningToBucket', () => {
        it('should handle errors', async () => {
            sinon.stub(connector1, 'removeBucket').rejects(new Error('error'));
            assert.rejects(allocator.stopListeningToBucket('example-bucket-2'));
        });

        it('should emit event if listening to bucket that was assigned a connector', async () => {
            allocator._bucketsToConnectors.set('example-bucket-1', connector1);
            const removeBucketStub = sinon.stub(connector1, 'removeBucket').resolves();
            await allocator.stopListeningToBucket('example-bucket-1');
            assert(removeBucketStub.calledOnceWith('example-bucket-1'));
            const assignedConnector = allocator._bucketsToConnectors.get('example-bucket-1');
            assert.strictEqual(assignedConnector, undefined);
        });

        it('should do nothing if bucket has no connector assigned', async () => {
            const removeBucketStub = sinon.stub(connector1, 'removeBucket').resolves();
            await allocator.stopListeningToBucket('example-bucket-1');
            assert(removeBucketStub.notCalled);
        });
    });
});
