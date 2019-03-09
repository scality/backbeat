const async = require('async');

const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');

/**
 * Schedule replication tasks by ensuring that all updates to the same object
 * are serialized
 */
class ReplicationTaskScheduler {
    constructor(processingFunc) {
        this._processingFunc = processingFunc;
        this._runningTasksByMasterKey = {};
        this._runningTasks = {};
    }

    _getUniqueKey(ctx) {
        const { entry } = ctx;
        let key;
        let version;
        let contentMd5;
        if (entry instanceof ObjectQueueEntry) {
            key = entry.getCanonicalKey();
            version = entry.getVersionId();
            contentMd5 = entry.getContentMd5();
        }
        if (entry instanceof ActionQueueEntry) {
            ({ key, version, contentMd5 } = entry.getAttribute('target'));
        }
        return `${key}:${version || ''}:${contentMd5}`;
    }

    _queueCallback(ctx, done) {
        delete this._runningTasks[this._getUniqueKey(ctx)];
        done();
    }

    _isRunningTask(ctx) {
        return this._runningTasks[this._getUniqueKey(ctx)];
    }

    _getNewQueue(ctx, masterKey) {
        const queue = async.queue(this._processingFunc);
        queue.drain = () => delete this._runningTasksByMasterKey[masterKey];
        this._runningTasksByMasterKey[masterKey] = queue;
        this._runningTasks[this._getUniqueKey(ctx)] = true;
        return queue;
    }

    _getQueue(ctx, masterKey) {
        const runningQueue = this._runningTasksByMasterKey[masterKey];
        return runningQueue || this._getNewQueue(ctx, masterKey);
    }

    push(ctx, masterKey, done) {
        if (this._isRunningTask(ctx)) {
            return done();
        }
        const queue = this._getQueue(ctx, masterKey);
        return queue.push(ctx, () => this._queueCallback(ctx, done));
    }
}

module.exports = ReplicationTaskScheduler;
