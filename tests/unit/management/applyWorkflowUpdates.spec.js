const assert = require('assert');

const werelogs = require('werelogs');

const getWorkflowUpdates =
      require('../../../lib/management/getWorkflowUpdates');
const applyWorkflowUpdates =
      require('../../../lib/management/applyWorkflowUpdates');

const logger = new werelogs.Logger('mdManagement:test');

class DummyWorkflowManager {

    constructor(config, currentState) {
        this.params = {
            serviceName: 'dummyService',
            applyBucketWorkflows: this.applyBucketDummyWorkflows.bind(this),
        };
        this.conf = config;
        this.currentState = currentState;
        this.notifiedWorkflows = undefined;
        this.notifiedWorkflowUpdates = undefined;
    }

    applyBucketDummyWorkflows(bucketName, workflows, workflowUpdates, cb) {
        assert.strictEqual(bucketName, 'test-bucket');
        assert.strictEqual(this.notifiedWorkflows, undefined);
        assert.strictEqual(this.notifiedWorkflowUpdates, undefined);
        this.notifiedWorkflows = workflows;
        this.notifiedWorkflowUpdates = workflowUpdates;
        return process.nextTick(cb);
    }

    apply(cb) {
        const workflowUpdates = getWorkflowUpdates(
            this.conf.workflows.dummyService,
            this.currentState.workflows);
        applyWorkflowUpdates(this.params, this.conf, this.currentState,
                             workflowUpdates, logger, cb);
    }

    checkNotifiedEqual(expectWorkflows, expectWorkflowUpdates) {
        assert.deepStrictEqual(this.notifiedWorkflows, expectWorkflows);
        assert.deepStrictEqual(this.notifiedWorkflowUpdates,
                               expectWorkflowUpdates);
    }
}


const workflow1 = {
    workflowId: 'w1',
    prefix: '/foo',
    destination: {
        locations: ['a', 'b', 'c'],
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

describe('applyWorkflowUpdates', () => {
    it('should apply a set of workflow updates to new config', done => {
        const mgr = new DummyWorkflowManager({
            version: 2,
            workflows: {
                dummyService: {
                    'test-bucket': [workflow1, workflow2],
                },
            },
        }, {
            overlayVersion: 1,
            workflows: {},
        });
        mgr.apply((err, newState) => {
            assert.ifError(err);
            mgr.checkNotifiedEqual([workflow1, workflow2], {
                new: [workflow1, workflow2],
                modified: [],
                deleted: [],
            });
            assert.deepStrictEqual(newState, {
                overlayVersion: 2,
                workflows: { 'test-bucket': {
                    w1: workflow1,
                    w2: workflow2,
                } },
            });
            done();
        });
    });

    it('should apply a set of workflow updates to existing bucket', done => {
        const mgr = new DummyWorkflowManager({
            version: 2,
            workflows: {
                dummyService: {
                    'test-bucket': [workflow2Mod, workflow4, workflow3],
                },
            },
        }, {
            overlayVersion: 1,
            workflows: {
                'test-bucket': {
                    w1: workflow1,
                    w2: workflow2,
                    w3: workflow3,
                },
            },
        });
        mgr.apply((err, newState) => {
            assert.ifError(err);
            mgr.checkNotifiedEqual([workflow2Mod, workflow4, workflow3], {
                new: [workflow4],
                modified: [{ previous: workflow2,
                             current: workflow2Mod }],
                deleted: [workflow1],
            });
            assert.deepStrictEqual(newState, {
                overlayVersion: 2,
                workflows: { 'test-bucket': {
                    w2: workflow2Mod,
                    w3: workflow3,
                    w4: workflow4,
                } },
            });
            done();
        });
    });

    it('should remove bucket from state when last workflow is ' +
    'deleted', done => {
        const mgr = new DummyWorkflowManager({
            version: 2,
            workflows: {
                dummyService: {},
            },
        }, {
            overlayVersion: 1,
            workflows: {
                'test-bucket': {
                    w1: workflow1,
                    w2: workflow2,
                },
            },
        });
        mgr.apply((err, newState) => {
            assert.ifError(err);
            mgr.checkNotifiedEqual([], {
                new: [],
                modified: [],
                deleted: [workflow1, workflow2],
            });
            assert.deepStrictEqual(newState, {
                overlayVersion: 2,
                workflows: {},
            });
            done();
        });
    });

    it('should not notify if overlay version has not changed', done => {
        const mgr = new DummyWorkflowManager({
            version: 2,
            workflows: {
                dummyService: [workflow2Mod, workflow3],
            },
        }, {
            overlayVersion: 2,
            workflows: {
                'test-bucket': {
                    w1: workflow1,
                    w2: workflow2,
                },
            },
        });
        mgr.apply((err, newState) => {
            assert.ifError(err);
            mgr.checkNotifiedEqual(undefined, undefined);
            assert.deepStrictEqual(newState, null);
            done();
        });
    });
});
