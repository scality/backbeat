const assert = require('assert');
const PipelineFactory = require('../../../../extensions/oplogPopulator/pipeline/PipelineFactory');

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
});
