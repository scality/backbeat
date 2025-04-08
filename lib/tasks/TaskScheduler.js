const async = require('async');

/**
 * Function that is called when all tasks have been processed
 * @typedef {Function} DrainFunction
 * @param {undefined}
 * @returns {undefined}
 */

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
        this._drainFunc = null;
        this._taskQueues = {};
        this._dedupeCache = {};
        this._inQueue = 0; // Number of tasks currently waiting in the queue
        this._running = 0; // Number of tasks currently running
    }
    
    _queueProcessingFunc(ctx, done) {
        this._inQueue--;
        this._running++;
        this._processingFunc(ctx, done);
    }

    _getNewTaskQueue(ctx, queueKey) {
        const queue = async.queue(this._queueProcessingFunc.bind(this));
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
        const onTaskEnd = (...args) => {
            if (dedupeKey !== undefined) {
                delete this._dedupeCache[dedupeKey];
            }
            this._running--;
            this._tryDrain();
            done(...args);
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
                this._inQueue++;
                return queue.push(ctx, onTaskEnd);
            }
        }
        this._running++;
        return process.nextTick(() => this._processingFunc(ctx, onTaskEnd));
    }

    /**
     * @returns {Number} The number of tasks waiting to be processed
     */
    length() {
        return this._inQueue;
    }

    /**
     * @returns {Number} The number of tasks currently being processed
     */
    running() {
        return this._running;
    }

    /**
     * @returns {Boolean} True if there are no tasks running or waiting
     */
    idle() {
        return this._running === 0 && this._inQueue === 0;
    }

    /**
     * Set a function to be called after all tasks have been processed
     * @param {DrainFunction} func drain function
     * @returns {undefined}
     */
    setDrain(func) {
        this._drainFunc = func;
    }

    /**
     * Call the drain function if there are no tasks running
     * @returns {undefined}
     */
    _tryDrain() {
        if (this.idle()) {
            this._drainFunc?.();
        }
    }
}

module.exports = TaskScheduler;
