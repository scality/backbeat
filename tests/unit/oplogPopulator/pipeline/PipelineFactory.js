const assert = require('assert');
const PipelineFactory = require('../../../../extensions/oplogPopulator/pipeline/PipelineFactory');
const { constants } = require('arsenal');

describe('PipelineFactory', () => {
    const pipelineFactory = new PipelineFactory();

    it('should throw NotImplemented when calling isValid', () => {
        assert.throws(() => pipelineFactory.isValid(), {
            name: 'Error',
            type: 'NotImplemented',
        });
    });

    it('should throw NotImplemented when calling getPipeline', () => {
        assert.throws(() => pipelineFactory.getPipeline(), {
            name: 'Error',
            type: 'NotImplemented',
        });
    });

    it('should extract buckets from connector config', () => {
        const config = {
            pipeline: JSON.stringify([{
                $match: {
                    'ns.coll': {
                        $in: ['example-bucket-1, example-bucket-2'],
                    }
                }
            }])
        };
        const buckets = pipelineFactory.extractBucketsFromConfig(config);
        assert.deepEqual(buckets, ['example-bucket-1, example-bucket-2']);
    });

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
        assert.throws(() => pipelineFactory.getOldConnectorBucketList(config), {
            name: 'Error',
            type: 'NotImplemented',
        });
    });

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
        assert.throws(() => pipelineFactory.getOldConnectorBucketList(config), {
            name: 'Error',
            type: 'NotImplemented',
        });
    });

    it('should return the list of buckets if the list is valid against the pipeline factory (wildcard)', async () => {
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
        assert.throws(() => pipelineFactory.getOldConnectorBucketList(config), {
            name: 'Error',
            type: 'NotImplemented',
        });
    });
});
