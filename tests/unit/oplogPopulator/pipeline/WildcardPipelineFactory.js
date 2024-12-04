const assert = require('assert');
const WildcardPipelineFactory = require('../../../../extensions/oplogPopulator/pipeline/WildcardPipelineFactory');
const { constants } = require('arsenal');

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

    describe('getOldConnectorBucketList', () => {
        it('should return null if the list is not valid against the pipeline factory', async () => {
            const config = {
                pipeline: JSON.stringify([{
                    $match: {
                        'ns.coll': {
                            $in: ['bucket1', 'bucket2'],
                        },
                    },
                }]),
            };
            const result = wildcardPipelineFactory.getOldConnectorBucketList(config);
            assert.deepStrictEqual(result, null);
        });

        it('should return the list of buckets if the list is valid against the pipeline factory', async () => {
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
            const result = wildcardPipelineFactory.getOldConnectorBucketList(config);
            assert.deepStrictEqual(result, ['*']);
        });
    });
});
