const assert = require('assert');
const crypto = require('crypto');

const HashedPipelineFactory = require('../../../../extensions/oplogPopulator/pipeline/HashedPipelineFactory');
const MultipleBucketsPipelineFactory =
    require('../../../../extensions/oplogPopulator/pipeline/MultipleBucketsPipelineFactory');
const { hashedPipelineGeneration } = require('../../../../extensions/oplogPopulator/constants');

// Hash of reference pipelines for each generation. A pipeline change must
// bump hashedPipelineGeneration (so that older populators keep off newer
// connectors) and add its hash here.
const referencePipelineHashes = {
    1: '0abc11d9',
};

describe('HashedPipelineFactory', () => {
    const factory = new HashedPipelineFactory(0);

    it('should select the bucket collections of one connector', () => {
        assert.deepStrictEqual(factory.getPipelineStage({ nbConnectors: 4, index: 1 }), {
            $match: {
                'ns.coll': {
                    $not: { $regex: '^(mpuShadowBucket|__).*' },
                    $ne: 'PENSIEVE',
                },
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

    it('should match the reference pipeline of its generation', () => {
        const reference = [new HashedPipelineFactory(0), new HashedPipelineFactory(100)]
            .map(f => f.getPipeline({ nbConnectors: 4, index: 1 }))
            .join('');
        const hash = crypto.createHash('sha256').update(reference).digest('hex').slice(0, 8);
        assert.strictEqual(hash, referencePipelineHashes[hashedPipelineGeneration],
            'the hashed pipeline changed: bump hashedPipelineGeneration and add its reference hash');
    });

    it('should not adopt connectors as bucket lists', () => {
        assert.strictEqual(factory.isValid(['bucket']), false);
    });

    it('should be recognized and deleted by the per-bucket mode', () => {
        const config = { pipeline: factory.getPipeline({ nbConnectors: 4, index: 1 }) };
        assert.strictEqual(new MultipleBucketsPipelineFactory(0).getOldConnectorBucketList(config), null);
    });
});
