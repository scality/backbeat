const assert = require('assert');

const bucketProcessorPolicy = require('../../../extensions/lifecycle/bucketProcessor/policy.json');

describe('LifecycleBucketProcessor policy', () => {
    it('should allow archive info reads for lifecycle metric location resolution', () => {
        const actions = bucketProcessorPolicy.Statement
            .find(statement => statement.Sid === 'LifecycleExpirationBucketProcessor')
            .Action;

        assert(actions.includes('scality:GetObjectArchiveInfo'));
        assert(!actions.includes('s3:GetBucketLocation'));
    });
});
