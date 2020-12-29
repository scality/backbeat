/**
 * Custom variant of async.mapLimit, where a failure does not call the
 * callback immediately, but waits until the pending requests complete
 * before returning the error and the latest array of results. It does
 * not trigger the remaining non-started requests after an error
 * occurs.
 *
 * The initial motivation to write this function is for cleaning up
 * orphan data after a failed replication: in this case we must wait
 * for all pending requests if an error occurs, but not trigger new
 * requests, so that we have the full list of orphans to delete
 * afterwards without missing the in-progress requests at the time the
 * error occurs.
 *
 * @param {Array} coll - collection to iterate over
 *
 * @param {number} limit - The maximum number of async operations at a
 * time
 *
 * @param {AsyncFunction} iteratee - An async function to apply to
 * each item in coll. The iteratee should complete with the
 * transformed item. Invoked with (item, callback).
 *
 * @param {function} callback - A callback which is called when all
 * iteratee functions have finished, or an error occurs. Results is an
 * array of the transformed items from the coll. Invoked with (err,
 * results).
 *
 * @return {undefined}
 */
function mapLimitWaitPendingIfError(coll, limit, iteratee, callback) {
    if (coll.length === 0) {
        return callback(null, []);
    }
    const results = [];
    let nPendingRequests = 0;
    let nextIdx = 0;
    let pendingError = null;
    const processNext = () => {
        const idx = nextIdx;
        nextIdx += 1;
        nPendingRequests += 1;
        iteratee(coll[idx], (err, res) => {
            nPendingRequests -= 1;
            results[idx] = res;
            if (err) {
                // don't trigger any new request
                nextIdx = coll.length;
                if (!pendingError) {
                    pendingError = err;
                }
                if (nPendingRequests === 0) {
                    callback(pendingError, results);
                }
            } else if (nextIdx < coll.length) {
                processNext();
            } else if (nPendingRequests === 0) {
                callback(pendingError, results);
            }
        });
    };
    while (nextIdx < Math.min(coll.length, limit)) {
        processNext();
    }
    return undefined;
}

module.exports = mapLimitWaitPendingIfError;
