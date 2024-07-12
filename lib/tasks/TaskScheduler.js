const async = require('async');

/**
 * Schedule tasks according to:
 *
 * - An optional queue key that guarantees all tasks sharing the same
 *   queue key will be processed in order and serially
 *
 * - An optional dedupe key that will skip new tasks if they share the
 *   same dedupe key than another task either in progress or already
 *   queued
 */
class TaskScheduler {
    /**
     * @constructor
     * @param {function} processingFunc - task processing function:
     *   processingFunc(ctx, cb)
     *   (see: {@link TaskScheduler.push()})
     * @param {function} [getQueueKeyFunc] - function to get the queue
     *   key of a task: getQueueKeyFunc(ctx) -> {string} key
     *   (see: {@link TaskScheduler.push()})
     * @param {function} [getDedupeKeyFunc] - function to get the dedupe
     *   key of a task: getDedupeKeyFunc(ctx) -> {string} key
     *   (see: {@link TaskScheduler.push()})
     */
    constructor(processingFunc, getQueueKeyFunc, getDedupeKeyFunc) {
        this._processingFunc = processingFunc;
        this._getQueueKeyFunc = getQueueKeyFunc;
        this._getDedupeKeyFunc = getDedupeKeyFunc;
        this._taskQueues = {};
        this._dedupeCache = {};
    }

    _getNewTaskQueue(ctx, queueKey) {
        const queue = async.queue(this._processingFunc);
        queue.drain = () => delete this._taskQueues[queueKey];
        this._taskQueues[queueKey] = queue;
        return queue;
    }

    _getTaskQueue(ctx, queueKey) {
        const queue = this._taskQueues[queueKey];
        return queue || this._getNewTaskQueue(ctx, queueKey);
    }

    /**
     * Add a new task to be executed by the scheduler
     *
     * @param {object} ctx - user-defined argument, passed to first
     * argument of processingFunc(), getQueueKeyFunc() and
     * getDedupeKeyFunc()
     * @param {function} done - called when the processing function
     * has called its callback, or when skipped by deduplication
     * @return {undefined}
     */
    push(ctx, done) {
        let dedupeKey;
        let queueKey;
        const onTaskEnd = () => {
            if (dedupeKey !== undefined) {
                delete this._dedupeCache[dedupeKey];
            }
            done();
        };
        if (this._getDedupeKeyFunc) {
            dedupeKey = this._getDedupeKeyFunc(ctx);
            if (typeof dedupeKey !== 'string') {
                dedupeKey = undefined;
            }
            if (dedupeKey !== undefined) {
                if (this._dedupeCache[dedupeKey]) {
                    return process.nextTick(done);
                }
                this._dedupeCache[dedupeKey] = true;
            }
        }
        if (this._getQueueKeyFunc) {
            queueKey = this._getQueueKeyFunc(ctx);
            if (typeof queueKey !== 'string') {
                queueKey = undefined;
            }
            if (queueKey !== undefined) {
                const queue = this._getTaskQueue(ctx, queueKey);
                return queue.push(ctx, onTaskEnd);
            }
        }
        return process.nextTick(() => this._processingFunc(ctx, onTaskEnd));
    }
}

module.exports = TaskScheduler;
