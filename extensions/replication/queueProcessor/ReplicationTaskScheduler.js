const async = require('async');

/**
 * Schedule replication tasks for replicated buckets, by ensuring that
 * all updates to the same object are serialized
*/
class ReplicationTaskScheduler {
    constructor(processingFunc) {
        this._processingFunc = processingFunc;
        this._runningTasksByMasterKey = {};
    }

    push(entry, masterKey, done) {
        let queue = this._runningTasksByMasterKey[masterKey];
        if (queue === undefined) {
            queue = async.queue(this._processingFunc);
            queue.drain =
                () => delete this._runningTasksByMasterKey[masterKey];
            this._runningTasksByMasterKey[masterKey] = queue;
        }
        queue.push(entry, done);
    }
}

module.exports = ReplicationTaskScheduler;
