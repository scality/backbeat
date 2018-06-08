const async = require('async');

/* eslint-disable no-param-reassign */
function applyWorkflowUpdates(params, conf, currentState,
                              workflowUpdates, logger, cb) {
    if (currentState.overlayVersion >= conf.version) {
        return process.nextTick(() => cb(null, null));
    }
    if (Object.keys(workflowUpdates).length === 0) {
        return process.nextTick(() => cb(null, null));
    }
    let changed = false;
    const errors = [];
    return async.each(Object.keys(workflowUpdates), (bucketName, next) => {
        // if there's a key in workflowUpdates array (returned by
        // getWorkflowUpdates() helper) it means the bucket workflows
        // have been updated, so apply the new workflows and updates
        // to the service

        const newBucketWorkflows =
              (conf.workflows &&
               conf.workflows[params.serviceName] &&
               conf.workflows[params.serviceName][bucketName]) || [];
        params.applyBucketWorkflows(
            bucketName, newBucketWorkflows,
            workflowUpdates[bucketName], err => {
                if (err) {
                    errors.push({ bucketName, message: err.message });
                } else {
                    const bucketWorkflowsState = {};
                    newBucketWorkflows.forEach(wf => {
                        bucketWorkflowsState[wf.workflowId] = wf;
                    });
                    if (Object.keys(bucketWorkflowsState).length > 0) {
                        currentState.workflows[bucketName] =
                            bucketWorkflowsState;
                    } else {
                        delete currentState.workflows[bucketName];
                    }
                    changed = true;
                }
                next();
            });
    }, () => {
        errors.forEach(error => {
            logger.error('an error occurred applying bucket workflows',
                         { bucketName: error.bucketName,
                           error: error.message });
        });
        // if any change has been applied even though errors may have
        // occurred, update version and return the new state to commit
        // it
        if (changed) {
            currentState.overlayVersion = conf.version;
            return cb(errors[0] || null, currentState);
        }
        return cb(errors[0] || null, null);
    });
}
/* eslint-enable no-param-reassign */

module.exports = applyWorkflowUpdates;

