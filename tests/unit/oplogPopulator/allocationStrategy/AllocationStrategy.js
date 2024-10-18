const assert = require('assert');
const werelogs = require('werelogs');
const { constants } = require('arsenal');
const AllocationStrategy = require('../../../../extensions/oplogPopulator/allocationStrategy/AllocationStrategy');
const WildcardPipelineFactory = require('../../../../extensions/oplogPopulator/pipeline/WildcardPipelineFactory');
const MultipleBucketsPipelineFactory =
    require('../../../../extensions/oplogPopulator/pipeline/MultipleBucketsPipelineFactory');

const logger = new werelogs.Logger('LeastFullConnector');

describe('AllocationStrategy', () => {
    it('should throw NotImplemented when calling getConnector', async () => {
        const allocationStrategy = new AllocationStrategy({ logger });
        assert.throws(() => allocationStrategy.getConnector(), {
            name: 'Error',
            type: 'NotImplemented',
        });
    });

    it('should throw NotImplemented when calling canUpdate', async () => {
        const allocationStrategy = new AllocationStrategy({ logger });
        assert.throws(() => allocationStrategy.canUpdate(), {
            name: 'Error',
            type: 'NotImplemented',
        });
    });

    it('should throw NotImplemented when calling maximumBucketsPerConnector', async () => {
        const allocationStrategy = new AllocationStrategy({ logger });
        assert.throws(() => allocationStrategy.maximumBucketsPerConnector(), {
            name: 'Error',
            type: 'NotImplemented',
        });
    });

    it('should return the list of buckets if the list is valid against the pipeline factory', async () => {
        const allocationStrategy = new AllocationStrategy({
            logger,
            pipelineFactory: new MultipleBucketsPipelineFactory(),
        });
        const config = {
            pipeline: JSON.stringify([{
                $match: {
                    'ns.coll': {
                        $in: ['bucket1', 'bucket2'],
                    },
                },
            }]),
        };
        const result = allocationStrategy.getOldConnectorBucketList(config);
        assert.deepStrictEqual(result, ['bucket1', 'bucket2']);
    });

    it('should return null if the list is not valid against the pipeline factory', async () => {
        const allocationStrategy = new AllocationStrategy({
            logger,
            pipelineFactory: new WildcardPipelineFactory(),
        });
        const config = {
            pipeline: JSON.stringify([{
                $match: {
                    'ns.coll': {
                        $in: ['bucket1', 'bucket2'],
                    },
                },
            }]),
        };
        const result = allocationStrategy.getOldConnectorBucketList(config);
        assert.deepStrictEqual(result, null);
    });

    it('should return the list of buckets if the list is valid against the pipeline factory (wildcard)', async () => {
        const allocationStrategy = new AllocationStrategy({
            logger,
            pipelineFactory: new WildcardPipelineFactory(),
        });
        const config = {
            pipeline: JSON.stringify([
                {
                    $match: {
                        'ns.coll': {
                            $not: {
                                $regex: `^(${constants.mpuBucketPrefix}|__).*`,
                            },
                        },
                    },
                },
            ]),
        };
        const result = allocationStrategy.getOldConnectorBucketList(config);
        assert.deepStrictEqual(result, ['*']);
    });
});
