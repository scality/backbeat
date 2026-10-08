const { PULL_REPLICATION } = require('../../lifecycle/LifecycleMetrics');

/**
 * Whether a copyLocation action pulls data from a remote location to a local
 * one, rather than copying data to a location
 *
 * @param {ActionQueueEntry} actionEntry - copyLocation action entry
 * @return {boolean} true for a pull replication copy
 */
function isPullReplication(actionEntry) {
    return actionEntry.getAttribute('metrics.origin') === PULL_REPLICATION;
}

module.exports = { isPullReplication };
