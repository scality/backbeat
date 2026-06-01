const assert = require('assert');
const WildcardPipelineFactory = require('../../../../extensions/oplogPopulator/pipeline/WildcardPipelineFactory');
const { constants } = require('arsenal');

describe('WildcardPipelineFactory', () => {
    const thresholdBytes = 100 * 1000000;
    const wildcardPipelineFactory = new WildcardPipelineFactory(thresholdBytes);

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
        it('should return the pipeline with buckets, key synthesis and location stripping', () => {
            const buckets = ['bucket1', 'bucket2'];
            const result = wildcardPipelineFactory.getPipeline(buckets);
            const pipeline = JSON.parse(result);

            assert.strictEqual(pipeline.length, 3);
            assert.deepStrictEqual(pipeline[0], {$match:{'ns.coll':{$not:{$regex:'^(mpuShadowBucket|__).*'}}}});
            assert.deepStrictEqual(pipeline[1], {
                $addFields: {
                    key: {
                        $ifNull: [
                            '$fullDocument.value.key',
                            '$updateDescription.updatedFields.value.key',
                        ],
                    },
                },
            });
            assert.deepStrictEqual(pipeline[2].$set['fullDocument.value.location'], {
                $cond: {
                    if: { $gte: ['$fullDocument.value.content-length', thresholdBytes] },
                    then: '$$REMOVE',
                    else: '$fullDocument.value.location',
                },
            });
            assert.deepStrictEqual(pipeline[2].$set['updateDescription.updatedFields.value.location'], {
                $cond: {
                    if: { $gte: ['$updateDescription.updatedFields.value.content-length', thresholdBytes] },
                    then: '$$REMOVE',
                    else: '$updateDescription.updatedFields.value.location',
                },
            });
        });

        it('should return the pipeline with key synthesis and no location stripping if disabled', () => {
            const wildcardPipelineFactoryNoStripping = new WildcardPipelineFactory(0);

            const buckets = ['bucket1', 'bucket2'];
            const result = wildcardPipelineFactoryNoStripping.getPipeline(buckets);
            const pipeline = JSON.parse(result);

            assert.strictEqual(pipeline.length, 2);
            assert.deepStrictEqual(pipeline[1], {
                $addFields: {
                    key: {
                        $ifNull: [
                            '$fullDocument.value.key',
                            '$updateDescription.updatedFields.value.key',
                        ],
                    },
                },
            });
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
