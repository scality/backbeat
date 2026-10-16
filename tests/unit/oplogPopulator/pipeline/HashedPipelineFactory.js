const assert = require('assert');

const HashedPipelineFactory = require('../../../../extensions/oplogPopulator/pipeline/HashedPipelineFactory');
const MultipleBucketsPipelineFactory =
    require('../../../../extensions/oplogPopulator/pipeline/MultipleBucketsPipelineFactory');

describe('HashedPipelineFactory', () => {
    const factory = new HashedPipelineFactory(0);

    it('should select the bucket collections of one connector', () => {
        assert.deepStrictEqual(factory.getPipelineStage({ nbConnectors: 4, index: 1 }), {
            $match: {
                'ns.coll': { $not: { $regex: '^(mpuShadowBucket|__).*' } },
                'operationType': { $in: ['insert', 'update', 'replace'] },
                '$expr': {
                    $in: [
                        { $mod: [{ $toHashedIndexKey: '$ns.coll' }, 4] },
                        [1, -3],
                    ],
                },
            },
        });
    });

    it('should keep the shared stages after the connector stage', () => {
        const pipeline = JSON.parse(new HashedPipelineFactory(100).getPipeline({ nbConnectors: 1, index: 0 }));
        assert.strictEqual(pipeline.length, 3);
        assert(pipeline[1].$addFields);
        assert(pipeline[2].$set);
    });

    it('should not adopt connectors as bucket lists', () => {
        assert.strictEqual(factory.isValid(['bucket']), false);
    });

    it('should be recognized and deleted by the per-bucket mode', () => {
        const config = { pipeline: factory.getPipeline({ nbConnectors: 4, index: 1 }) };
        assert.strictEqual(new MultipleBucketsPipelineFactory(0).getOldConnectorBucketList(config), null);
    });
});
