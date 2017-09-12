const async = require('async');

/**
 * Schedule replication tasks for replicated buckets, by ensuring that
 * all updates to the same version of an object are serialized
*/
class ReplicationTaskScheduler {
    constructor(processingFunc) {
        this._processingFunc = processingFunc;
        this._runningTasksByVersionedKey = {};
    }

    push(entry, versionedKey, done) {
        let queue = this._runningTasksByVersionedKey[versionedKey];
        if (queue === undefined) {
            queue = async.queue(this._processingFunc);
            queue.drain =
                () => delete this._runningTasksByVersionedKey[versionedKey];
            this._runningTasksByVersionedKey[versionedKey] = queue;
        }
        queue.push(entry, done);
    }
}

module.exports = ReplicationTaskScheduler;
