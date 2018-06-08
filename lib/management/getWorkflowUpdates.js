const assert = require('assert');

/**
 * Get a representation of workflow updates as a JS object
 *
 * Any new, modified or deleted workflow for each modified bucket will
 * have its workflow appended to a list respectively in the new,
 * modified or deleted key under the bucket key.
 *
 * - for new workflows, the workflow object is appended as-is to the
 *   'new' array
 *
 * - for deleted workflows, the previous workflow object is appended
 *   as-is to the 'deleted' array
 *
 * - for modified workflows, the previous and current workflows are
 *   set in an enclosing object as properties 'previous' and 'current'
 *   which is pushed to the 'modified' array.
 *
 * @example
 * getWorkflowUpdates({ bucket1: [{ workflowId: 'wf1' }] },
 *                    {})
 *    -> { bucket1: { new: [{ workflowId: 'wf1' }],
 *                    modified: [],
 *                    deleted: [] } }
 *
 * @example
 * getWorkflowUpdates({ bucket1: [{ workflowId: 'wf1',
 *                                  someProp: 2 }] },
 *                    { bucket1: { wf1: { workflowId: 'wf1',
 *                                        someProp: 1 } } })
 *    -> { bucket1: { new: [],
 *                    modified: [{ previous: {
 *                                     workflowId: 'wf1',
 *                                     someProp: 1
 *                                 },
 *                                 current: {
 *                                     workflowId: 'wf1',
 *                                     someProp: 2
 *                                 } }],
 *                    deleted: [] } }
 *
 * @param {object} [configuredWorkflows] - object containing the
 *   currently configured workflows, containing arrays of workflow
 *   objects grouped by target bucket. May be null or undefined.
 * @param {object} currentWorkflows - current workflow state extracted
 *   from the service state (under 'workflows' key). May be null or
 *   undefined.
 * @return {object} - a representation of workflow updates (see
 *   examples in description)
 */
function getWorkflowUpdates(configuredWorkflows, currentWorkflows) {
    const workflowUpdates = {};
    if (!configuredWorkflows) {
        // eslint-disable-next-line no-param-reassign
        configuredWorkflows = {};
    }
    if (!currentWorkflows) {
        // eslint-disable-next-line no-param-reassign
        currentWorkflows = {};
    }
    Object.keys(configuredWorkflows).forEach(bucketName => {
        const currentBucketWorkflows = currentWorkflows[bucketName];
        const configuredBucketWorkflows = configuredWorkflows[bucketName];
        if (!currentBucketWorkflows) {
            if (configuredBucketWorkflows.length > 0) {
                workflowUpdates[bucketName] = {
                    new: configuredBucketWorkflows,
                    modified: [],
                    deleted: [],
                };
            }
            return undefined;
        }
        const currentBucketWorkflowsArray =
              Object.keys(currentBucketWorkflows)
              .map(workflowId => currentBucketWorkflows[workflowId]);
        const updates = {
            new: configuredBucketWorkflows.filter(
                wf => !currentBucketWorkflows[wf.workflowId]),
            modified: configuredBucketWorkflows.filter(
                wf => {
                    if (!currentBucketWorkflows[wf.workflowId]) {
                        return false;
                    }
                    try {
                        assert.deepStrictEqual(
                            wf, currentBucketWorkflows[wf.workflowId]);
                        return false;
                    } catch (err) {
                        return true;
                    }
                }).map(wf => ({
                    previous: currentBucketWorkflows[wf.workflowId],
                    current: wf,
                })),
            deleted: currentBucketWorkflowsArray
                .filter(wf => configuredBucketWorkflows.find(
                    cwf => wf.workflowId === cwf.workflowId) === undefined),
        };
        if (updates.new.length > 0 ||
            updates.modified.length > 0 ||
            updates.deleted.length > 0) {
            workflowUpdates[bucketName] = updates;
        }
        return undefined;
    });
    Object.keys(currentWorkflows).forEach(bucketName => {
        if (!configuredWorkflows[bucketName]) {
            const currentBucketWorkflows = currentWorkflows[bucketName];
            const currentBucketWorkflowsArray =
                  Object.keys(currentBucketWorkflows)
                  .map(workflowId => currentBucketWorkflows[workflowId]);
            if (currentBucketWorkflowsArray.length > 0) {
                workflowUpdates[bucketName] = {
                    new: [],
                    modified: [],
                    deleted: currentBucketWorkflowsArray,
                };
            }
        }
    });
    return workflowUpdates;
}

module.exports = getWorkflowUpdates;
