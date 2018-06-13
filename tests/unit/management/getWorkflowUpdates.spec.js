const assert = require('assert');

const getWorkflowUpdates =
      require('../../../lib/management/getWorkflowUpdates');

const workflow1 = {
    workflowId: 'w1',
    prefix: '/foo',
    destination: {
        locations: ['a', 'b', 'c'],
    },
};

const workflow1Mod = {
    workflowId: 'w1',
    prefix: '/foo',
    destination: {
        locations: ['a', 'b', 'd'],
    },
};

const workflow2 = {
    workflowId: 'w2',
    prefix: '/bar',
    destination: {
        locations: ['d', 'e'],
    },
};

const workflow2Mod = {
    workflowId: 'w2',
    prefix: '/another_prefix',
    destination: {
        locations: ['d', 'e'],
    },
};

const workflow3 = {
    workflowId: 'w3',
    prefix: '/qux',
    destination: {
        locations: ['f', 'g'],
    },
};

const workflow4 = {
    workflowId: 'w4',
    prefix: '/quz',
    destination: {
        locations: ['h', 'i'],
    },
};

describe('getWorkflowUpdates', () => {
    it('should detect a new workflow on new config', () => {
        assert.deepStrictEqual(
            getWorkflowUpdates(
                {
                    bucket1: [workflow1],
                },
                undefined
            ),
            {
                bucket1: {
                    new: [workflow1],
                    modified: [],
                    deleted: [],
                },
            });
    });
    it('should detect a new workflow on new bucket', () => {
        assert.deepStrictEqual(
            getWorkflowUpdates(
                {
                    bucket1: [workflow1],
                },
                {
                }
            ),
            {
                bucket1: {
                    new: [workflow1],
                    modified: [],
                    deleted: [],
                },
            });
    });
    it('should detect a new workflow on existing bucket with workflows',
    () => {
        assert.deepStrictEqual(
            getWorkflowUpdates(
                {
                    bucket1: [workflow1, workflow2],
                },
                {
                    bucket1: { w1: workflow1 },
                }
            ),
            {
                bucket1: {
                    new: [workflow2],
                    modified: [],
                    deleted: [],
                },
            });
    });
    it('should detect a modified workflow', () => {
        assert.deepStrictEqual(
            getWorkflowUpdates(
                {
                    bucket1: [workflow1Mod, workflow2],
                },
                {
                    bucket1: { w1: workflow1, w2: workflow2 },
                }
            ),
            {
                bucket1: {
                    new: [],
                    modified: [{ previous: workflow1,
                                 current: workflow1Mod }],
                    deleted: [],
                },
            });
    });
    it('should ignore an existing but unmodified workflow', () => {
        assert.deepStrictEqual(
            getWorkflowUpdates(
                {
                    bucket1: [workflow1, workflow2],
                },
                {
                    bucket1: { w1: workflow1, w2: workflow2 },
                }
            ),
            {
            });
    });
    it('should detect a deleted workflow on existing bucket', () => {
        assert.deepStrictEqual(
            getWorkflowUpdates(
                {
                    bucket1: [workflow2],
                },
                {
                    bucket1: { w1: workflow1, w2: workflow2 },
                }
            ),
            {
                bucket1: {
                    new: [],
                    modified: [],
                    deleted: [workflow1],
                },
            });
    });
    it('should detect deleted workflows on deleted bucket', () => {
        assert.deepStrictEqual(
            getWorkflowUpdates(
                {
                },
                {
                    bucket1: { w1: workflow1, w2: workflow2 },
                }
            ),
            {
                bucket1: {
                    new: [],
                    modified: [],
                    deleted: [workflow1, workflow2],
                },
            });
    });
    it('should be fine with no configured workflow at all', () => {
        assert.deepStrictEqual(
            getWorkflowUpdates(
                undefined,
                {
                    bucket1: { w1: workflow1, w2: workflow2 },
                }
            ),
            {
                bucket1: {
                    new: [],
                    modified: [],
                    deleted: [workflow1, workflow2],
                },
            });
    });
    it('should detect a set of new, modified and deleted workflows', () => {
        assert.deepStrictEqual(
            getWorkflowUpdates(
                {
                    bucket1: [workflow1Mod],
                    bucket2: [workflow2Mod, workflow4, workflow3],
                    bucket4: [],
                },
                {
                    bucket1: { w1: workflow1 },
                    bucket2: { w1: workflow1, w2: workflow2, w3: workflow3 },
                    bucket3: {},
                }
            ),
            {
                bucket1: {
                    new: [],
                    modified: [{ previous: workflow1,
                                 current: workflow1Mod }],
                    deleted: [],
                },
                bucket2: {
                    new: [workflow4],
                    modified: [{ previous: workflow2,
                                 current: workflow2Mod }],
                    deleted: [workflow1],
                },
            });
    });
});
