const assert = require('assert');
const MultipleBucketsPipelineFactory =
    require('../../../../extensions/oplogPopulator/pipeline/MultipleBucketsPipelineFactory');
const { constants } = require('arsenal');

describe('MultipleBucketsPipelineFactory', () => {
    const thresholdBytes = 100 * 1000000;
    const multipleBucketsPipelineFactory = new MultipleBucketsPipelineFactory(thresholdBytes);

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

        it('should return the pipeline with buckets, key synthesis and location stripping', () => {
            const buckets = ['bucket1', 'bucket2'];
            const result = multipleBucketsPipelineFactory.getPipeline(buckets);
            const pipeline = JSON.parse(result);

            assert.strictEqual(pipeline.length, 3);
            assert.deepStrictEqual(pipeline[0], {$match:{'ns.coll':{$in:['bucket1','bucket2']}}});
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

        it('should return the pipeline with key synthesis when location stripping is disabled', () => {
            const factory = new MultipleBucketsPipelineFactory(0);
            const pipeline = JSON.parse(factory.getPipeline(['bucket1']));
            assert.strictEqual(pipeline.length, 2);
            assert.deepStrictEqual(pipeline[0], {$match:{'ns.coll':{$in:['bucket1']}}});
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
        it('should return the list of buckets if the list is valid against the pipeline factory', async () => {
            const config = {
                pipeline: JSON.stringify([{
                    $match: {
                        'ns.coll': {
                            $in: ['bucket1', 'bucket2'],
                        },
                    },
                }]),
            };
            const result = multipleBucketsPipelineFactory.getOldConnectorBucketList(config);
            assert.deepStrictEqual(result, ['bucket1', 'bucket2']);
        });

        it('should return null if the list is not valid against the pipeline factory', async () => {
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
            const result = multipleBucketsPipelineFactory.getOldConnectorBucketList(config);
            assert.deepStrictEqual(result, null);
        });
    });
});
