const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');

const Connector =
    require('../../../../extensions/oplogPopulator/modules/Connector');
const RetainBucketsDecorator =
    require('../../../../extensions/oplogPopulator/allocationStrategy/RetainBucketsDecorator');
const ImmutableConnector = require('../../../../extensions/oplogPopulator/allocationStrategy/ImmutableConnector');
const LeastFullConnector = require('../../../../extensions/oplogPopulator/allocationStrategy/LeastFullConnector');

const logger = new werelogs.Logger('LeastFullConnector');

const defaultConnectorParams = {
    config: {},
    isRunning: true,
    logger,
    kafkaConnectHost: '127.0.0.1',
    kafkaConnectPort: 8083,
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
            maximumBucketsPerConnector: 2,
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
            it('Should return the retrained bucket\'s connector if it exists', () => {
                decorator._retainedBuckets.set('bucket1', connector1);
                const result = decorator.getConnector([connector1, connector2], 'bucket1');
                assert.strictEqual(result, connector1);
            });

            it('Should call the strategy if the bucket is not retained', () => {
                const result = decorator.getConnector([connector1, connector2], 'bucket1');
                assert.strictEqual(result, strategy.getConnector([connector1, connector2], 'bucket1'));
            });
        });

        describe('onConnectorUpdated', () => {
            it('should remove retained buckets for connector', () => {
                decorator._retainedBuckets.set('bucket1', connector1);
                decorator._retainedBuckets.set('bucket2', connector2);
                decorator.onConnectorUpdated(connector1);
                assert.strictEqual(decorator._retainedBuckets.size, 1);
                assert.strictEqual(decorator._retainedBuckets.get('bucket2'), connector2);
            });

            it('should not remove retained buckets for other connectors', () => {
                decorator._retainedBuckets.set('bucket1', connector1);
                decorator._retainedBuckets.set('bucket2', connector2);
                decorator.onConnectorUpdated(connector2);
                assert.strictEqual(decorator._retainedBuckets.size, 1);
                assert.strictEqual(decorator._retainedBuckets.get('bucket1'), connector1);
            });
        });

        describe('canUpdate', () => {
            it('Should return the strategy result', async () => {
                const result = await decorator.canUpdate(connector1);
                assert.strictEqual(result, strategy.canUpdate(connector1));
            });

            it('Should remove from retained buckets if the strategy allows', async () => {
                sinon.stub(strategy, 'canUpdate').returns(true);
                await decorator.canUpdate(connector1);
                assert.strictEqual(decorator._retainedBuckets.size, 0);
            });
        });
    });

});
