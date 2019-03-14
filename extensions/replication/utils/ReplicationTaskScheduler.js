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

    _getTaskKey(ctx) {
        const { entry } = ctx;
        if (entry instanceof ObjectQueueEntry) {
            const key = entry.getCanonicalKey();
            const version = entry.getVersionId();
            const contentMd5 = entry.getContentMd5();
            return `${key}:${version || ''}:${contentMd5}`;
        }
        if (entry instanceof ActionQueueEntry) {
            const { key, version, contentMd5 } = entry.getAttribute('target');
            return `${key}:${version || ''}:${contentMd5}`;
        }
        return undefined;
    }

    _getNewMasterKeyQueue(ctx, masterKey) {
        const queue = async.queue(this._processingFunc);
        queue.drain = () => delete this._runningTasksByMasterKey[masterKey];
        this._runningTasksByMasterKey[masterKey] = queue;
        return queue;
    }

    _getMasterKeyQueue(ctx, masterKey) {
        const queue = this._runningTasksByMasterKey[masterKey];
        return queue || this._getNewMasterKeyQueue(ctx, masterKey);
    }

    push(ctx, masterKey, done) {
        const taskKey = this._getTaskKey(ctx);
        if (this._runningTasks[taskKey]) {
            return done();
        }
        this._runningTasks[taskKey] = true;
        const queue = this._getMasterKeyQueue(ctx, masterKey);
        return queue.push(ctx, () => {
            delete this._runningTasks[taskKey];
            done();
        });
    }
}

module.exports = ReplicationTaskScheduler;
