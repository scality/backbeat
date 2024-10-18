const assert = require('assert');
const WildcardPipelineFactory = require('../../../../extensions/oplogPopulator/pipeline/WildcardPipelineFactory');

describe('WildcardPipelineFactory', () => {
    const wildcardPipelineFactory = new WildcardPipelineFactory();

    describe('isValid', () => {
        it('should detect a wildcard', () => {
            const bucketList = ['*'];
            const result = wildcardPipelineFactory.isValid(bucketList);
            assert.strictEqual(result, true);
        });

        it('should reject an empty list of buckets', () => {
            const bucketList = [];
            const result = wildcardPipelineFactory.isValid(bucketList);
            assert.strictEqual(result, false);
        });

        it('should reject an empty object', () => {
            const bucketList = null;
            const result = wildcardPipelineFactory.isValid(bucketList);
            assert.strictEqual(result, false);
        });

        it('should reject a list with buckets', () => {
            const bucketList = ['bucket1'];
            const result = wildcardPipelineFactory.isValid(bucketList);
            assert.strictEqual(result, false);
        });
    });

    describe('getPipeline', () => {
        it('should return the pipeline with wildcard', () => {
            const buckets = ['bucket1', 'bucket2'];
            const result = wildcardPipelineFactory.getPipeline(buckets);
            assert.strictEqual(result, '[{"$match":{"ns.coll":{"$not":{"$regex":"^(mpuShadowBucket|__).*"}}}}]');
        });
    });
});
