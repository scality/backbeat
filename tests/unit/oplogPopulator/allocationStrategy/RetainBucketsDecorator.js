const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');

const Connector =
    require('../../../../extensions/oplogPopulator/modules/Connector');
const RetainBucketsDecorator =
    require('../../../../extensions/oplogPopulator/allocationStrategy/RetainBucketsDecorator');
const ImmutableConnector = require('../../../../extensions/oplogPopulator/allocationStrategy/ImmutableConnector');
const LeastFullConnector = require('../../../../extensions/oplogPopulator/allocationStrategy/LeastFullConnector');
const MultipleBucketsPipelineFactory =
    require('../../../../extensions/oplogPopulator/pipeline/MultipleBucketsPipelineFactory');

const logger = new werelogs.Logger('LeastFullConnector');

const defaultConnectorParams = {
    config: {},
    isRunning: true,
    logger,
    kafkaConnectHost: '127.0.0.1',
    kafkaConnectPort: 8083,
    getPipeline: new MultipleBucketsPipelineFactory().getPipeline,
};

const connector1 = new Connector({
    name: 'example-connector-1',
    buckets: [],
    ...defaultConnectorParams,
});

const connector2 = new Connector({
    name: 'example-connector-2',
    buckets: ['bucket1', 'bucket2'],
    ...defaultConnectorParams,
});

[
    {
        scenario: 'ImmutableConnector',
        strategy: new ImmutableConnector({ logger }),
    },
    {
        scenario: 'LeastFullConnector',
        strategy: new LeastFullConnector({
            logger,
        }),
    }
].forEach(({ scenario, strategy }) => {
    describe(`RetainBucketsDecorator with ${scenario}`, () => {
        let decorator;

        beforeEach(() => {
            decorator = new RetainBucketsDecorator(strategy, { logger });
        });

        afterEach(() => {
            decorator._retainedBuckets.clear();
            sinon.restore();
        });

        describe('onBucketRemoved', () => {
            it('should add bucket from retained buckets', () => {
                decorator.onBucketRemoved('bucket1', connector1);
                assert.strictEqual(decorator._retainedBuckets.size, 1);
                assert.strictEqual(decorator._retainedBuckets.get('bucket1'), connector1);
            });
        });

        describe('getConnector', () => {
            it('should return the retrained bucket\'s connector if it exists', () => {
                decorator._retainedBuckets.set('bucket1', connector1);
                const result = decorator.getConnector([connector1, connector2], 'bucket1');
                assert.strictEqual(result, connector1);
            });

            it('should call the strategy if the bucket is not retained', () => {
                const result = decorator.getConnector([connector1, connector2], 'bucket1');
                assert.strictEqual(result, strategy.getConnector([connector1, connector2], 'bucket1'));
            });
        });

        describe('onConnectorDestroyed', () => {
            it('should remove retained buckets for connector', () => {
                decorator._retainedBuckets.set('bucket1', connector1);
                decorator._retainedBuckets.set('bucket2', connector2);
                decorator.onConnectorUpdatedOrDestroyed(connector1);
                assert.strictEqual(decorator._retainedBuckets.size, 1);
                assert.strictEqual(decorator._retainedBuckets.get('bucket2'), connector2);
            });

            it('should not remove retained buckets for other connectors', () => {
                decorator._retainedBuckets.set('bucket1', connector1);
                decorator._retainedBuckets.set('bucket2', connector2);
                decorator.onConnectorUpdatedOrDestroyed(connector2);
                assert.strictEqual(decorator._retainedBuckets.size, 1);
                assert.strictEqual(decorator._retainedBuckets.get('bucket1'), connector1);
            });
        });

        describe('canUpdate', () => {
            it('should return the strategy result', async () => {
                const result = await decorator.canUpdate();
                assert.strictEqual(result, strategy.canUpdate());
            });

            it('should remove from retained buckets if the strategy allows', async () => {
                sinon.stub(strategy, 'canUpdate').returns(true);
                await decorator.canUpdate();
                assert.strictEqual(decorator._retainedBuckets.size, 0);
            });
        });

        describe('maximumBucketsPerConnector', () => {
            it('should return the strategy result', () => {
                const result = decorator.maximumBucketsPerConnector;
                assert.strictEqual(result, strategy.maximumBucketsPerConnector);
            });
        });
    });

});
