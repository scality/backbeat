const assert = require('assert');
const MultipleBucketsPipelineFactory =
    require('../../../../extensions/oplogPopulator/pipeline/MultipleBucketsPipelineFactory');

describe('MultipleBucketsPipelineFactory', () => {
    const multipleBucketsPipelineFactory = new MultipleBucketsPipelineFactory();

    describe('isValid', () => {
        it('should detect a valid list of buckets', () => {
            const bucketList = ['bucket1', 'bucket2'];
            const result = multipleBucketsPipelineFactory.isValid(bucketList);
            assert.strictEqual(result, true);
        });

        it('should reject an empty list of buckets', () => {
            const bucketList = [];
            const result = multipleBucketsPipelineFactory.isValid(bucketList);
            assert.strictEqual(result, false);
        });

        it('should reject an empty object', () => {
            const bucketList = null;
            const result = multipleBucketsPipelineFactory.isValid(bucketList);
            assert.strictEqual(result, false);
        });

        it('should reject a list with wildcard', () => {
            const bucketList = ['*'];
            const result = multipleBucketsPipelineFactory.isValid(bucketList);
            assert.strictEqual(result, false);
        });
    });

    describe('getPipeline', () => {
        it('should return an empty array as string if no bucket', () => {
            const buckets = [];
            const result = multipleBucketsPipelineFactory.getPipeline(buckets);
            assert.strictEqual(result, '[]');
        });

        it('should return an empty array as string if bucket list is null', () => {
            const buckets = null;
            const result = multipleBucketsPipelineFactory.getPipeline(buckets);
            assert.strictEqual(result, '[]');
        });

        it('should return the pipeline with buckets', () => {
            const buckets = ['bucket1', 'bucket2'];
            const result = multipleBucketsPipelineFactory.getPipeline(buckets);
            assert.strictEqual(result, '[{"$match":{"ns.coll":{"$in":["bucket1","bucket2"]}}}]');
        });
    });
});
