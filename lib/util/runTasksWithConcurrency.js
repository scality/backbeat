/**
 * Runs an async task for each item in a collection, up to `limit` at a time.
 * On error, no new tasks are started, but already-running tasks are awaited
 * before the function resolves. Returns [firstError, results] so that callers
 * can inspect partial results (e.g. to clean up orphan data).
 *
 * The initial motivation to write this function is for cleaning up
 * orphan data after a failed replication: in this case we must wait
 * for all pending requests if an error occurs, but not trigger new
 * requests, so that we have the full list of orphans to delete
 * afterwards without missing the in-progress requests at the time the
 * error occurs.
 *
 * @param {Array}    coll  - collection to iterate over
 * @param {number}   limit - maximum number of concurrent tasks
 * @param {Function} task  - async function (item) => result
 * @return {Promise<[Error|null, Array]>} - always resolves, never rejects.
 *   Errors are not thrown. Callers must inspect the first element
 *   (the first error, or null on success), otherwise failures are silently
 *   ignored. The second element holds partial results.
 */
async function runTasksWithConcurrency(coll, limit, task) {
    if (coll.length === 0) {
        return [null, []];
    }
    const results = new Array(coll.length);
    let nextIdx = 0;
    let firstError = null; // Only the first error is reported

    const worker = async () => {
        while (nextIdx < coll.length) {
            const idx = nextIdx++;
            try {
                results[idx] = await task(coll[idx]);
            } catch (err) {
                if (firstError === null) {
                    firstError = err;
                }
                nextIdx = coll.length;
                return;
            }
        }
    };

    await Promise.all(
        Array.from({ length: Math.min(limit, coll.length) }, () => worker())
    );

    return [firstError, results];
}

module.exports = runTasksWithConcurrency;
