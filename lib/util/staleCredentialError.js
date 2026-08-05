/**
 * Helpers for retrying failures caused by a stale service credential.
 *
 * Backbeat and cloudserver both cache their service-account credentials at
 * process start, so a credential rotation leaves the two disagreeing until
 * both have restarted, and cloudserver rejects the key it does not know with
 * a 403. Smithy classifies a 403 as non-retryable (see isRetryableMiddleware),
 * which is right for a customer endpoint but drops the entry on the internal
 * routes, where a later attempt may well be served by an already-updated pod.
 */

const MAX_STALE_CREDENTIAL_RETRIES = 5;

/**
 * Check if an error denotes a stale service credential
 * @param {Error} err - The error object
 * @returns {boolean} True if the access key is unknown or its secret mismatched
 */
function isStaleCredentialError(err) {
    const code = err && (err.name || err.code);
    return code === 'InvalidAccessKeyId' || code === 'SignatureDoesNotMatch';
}

/**
 * Build a shouldRetryFunc for BackbeatTask.retry() that keeps the usual
 * err.retryable behaviour and additionally retries a bounded number of
 * stale-credential failures.
 *
 * The returned function is stateful: build one per retry() call so that the
 * budget applies to a single logical operation.
 *
 * @param {number} [maxRetries] - maximum stale-credential retries
 * @returns {function} predicate suitable for retry()'s shouldRetryFunc
 */
function retryOnStaleCredentials(maxRetries = MAX_STALE_CREDENTIAL_RETRIES) {
    let attempts = 0;
    return err => err.retryable ||
        (isStaleCredentialError(err) && attempts++ < maxRetries);
}

module.exports = {
    MAX_STALE_CREDENTIAL_RETRIES,
    isStaleCredentialError,
    retryOnStaleCredentials,
};
