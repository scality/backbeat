const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');

const Connector =
    require('../../../../extensions/oplogPopulator/modules/Connector');
const UniqueConnector =
    require('../../../../extensions/oplogPopulator/allocationStrategy/UniqueConnector');
const MultipleBucketsPipelineFactory =
    require('../../../../extensions/oplogPopulator/pipeline/MultipleBucketsPipelineFactory');

const logger = new werelogs.Logger('LeastFullConnector');

const defaultConnectorParams = {
    config: {
        pipeline: JSON.stringify([{
            $match: {
                'ns.coll': {
                    $in: ['example-bucket-1, example-bucket-2'],
                },
            },
        }]),
    },
    isRunning: true,
    logger,
    kafkaConnectHost: '127.0.0.1',
    kafkaConnectPort: 8083,
    getPipeline: new MultipleBucketsPipelineFactory().getPipeline,
};

const connector1 = new Connector({
    name: 'example-connector-2',
    buckets: ['bucket1', 'bucket2'],
    ...defaultConnectorParams,
});

const connector2 = new Connector({
    name: 'example-connector-3',
    buckets: ['bucket3'],
    ...defaultConnectorParams,
});

describe('UniqueConnector', () => {
    let strategy;
    beforeEach(() => {
        strategy = new UniqueConnector({
            logger,
        });
    });

    describe('getConnector', () => {
        afterEach(() => {
            sinon.restore();
        });

        it('should return null if no connector', () => {
            const connector = strategy.getConnector([]);
            assert.strictEqual(connector, null);
        });

        it('should return null if no connector with the wildcard', () => {
            const connector = strategy.getConnector([connector1, connector2]);
            assert.strictEqual(connector, null);
        });

        it('should return connector with wildcard', () => {
            const connectorWithWildCard = new Connector({
                name: 'example-connector-1',
                buckets: ['*'],
                ...defaultConnectorParams,
                config: {
                    pipeline: JSON.stringify([{
                        $match: {
                            'ns.coll': {
                                $not: {
                                    $regex: '^(mpuShadowBucket|__).*',
                                },
                            },
                        },
                    }]),
                },
            });
            const connector = strategy.getConnector([connectorWithWildCard]);
            assert.strictEqual(connector, connectorWithWildCard);
        });
    });

    it('should return false for canUpdate', async () =>
        assert.strictEqual(strategy.canUpdate(), false));

    it('should return 1 for maximumBucketsPerConnector', async () =>
        assert.strictEqual(strategy.maximumBucketsPerConnector, 1));
});
