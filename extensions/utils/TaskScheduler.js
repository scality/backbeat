const async = require('async');

/**
 * Schedule replication tasks for replicated buckets by ensuring that all
 * updates to the same object are serialized. For lifecycle buckets, ensure
 * that the same object is not being transitioned multiple times.
*/
class TaskScheduler {
    constructor(processingFunc) {
        this._processingFunc = processingFunc;
        this._replicationTasks = {};
        this._lifecycleTasks = {};
    }

    /**
     * Set the unqiue key in the relevant task or push to that queue.
     * @param {object} tasks - The running tasks to push to
     * @param {object} ctx - The context for the task
     * @param {string} key - The unique key to track running tasks
     * @param {Function} cb - The callback to push to the queue
     * @return {undefined}
     */
    _pushToQueue(tasks, ctx, key, cb) {
        let queue = tasks[key];
        if (queue === undefined) {
            queue = async.queue(this._processingFunc);
            /* eslint-disable no-param-reassign */
            queue.drain = () => delete tasks[key];
            tasks[key] = queue;
            /* eslint-enable no-param-reassign */
        }
        queue.push(ctx, cb);
    }

    /**
     * Push to the running replication tasks.
     * @param {object} ctx - The context for the task
     * @param {Function} cb - The callback to push to the queue
     * @return {undefined}
     */
    _pushReplicationTask(ctx, cb) {
        const { entry } = ctx;
        const key = entry.getCanonicalKey();
        return this._pushToQueue(this._replicationTasks, ctx, key, cb);
    }

    /**
     * Push to the running lifecycle tasks, or skip if already running.
     * @param {object} ctx - The context for the task
     * @param {Function} cb - The callback to push to the queue
     * @return {undefined}
     */
    _pushLifecycleTask(ctx, cb) {
        const { entry } = ctx;
        const canonicalKey = entry.getCanonicalKey();
        const versionId = entry.getVersionId() || '';
        const contentMD5 = entry.getContentMd5();
        const key = `${canonicalKey}:${versionId}:${contentMD5}`;
        // If this object is already queued, skip it.
        if (this._lifecycleTasks[key]) {
            return cb();
        }
        return this._pushToQueue(this._lifecycleTasks, ctx, key, cb);
    }

    /**
     * Push to either replication or lifecycle tasks depending on entry type.
     * @param {object} ctx - The context for the task
     * @param {Function} cb - The callback to push to the queue
     * @return {undefined}
     */
    push(ctx, cb) {
        const { entry } = ctx;
        if (entry.isReplicationOperation()) {
            return this._pushReplicationTask(ctx, cb);
        }
        if (entry.isLifecycleOperation()) {
            return this._pushLifecycleTask(ctx, cb);
        }
        return cb();
    }
}

module.exports = TaskScheduler;
