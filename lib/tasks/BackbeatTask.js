const BackOff = require('backo');

/**
 * @class BackbeatTask
 */
class BackbeatTask {
    /**
     * Create a BackbeatTask.
     *
     * @constructor
     * @param {Object} [retryParams] - Configuration for retry logic.
     * @param {number} [retryParams.timeoutS=300] - Total time (in seconds) allowed for retries before giving up.
     * @param {Object} [retryParams.backoff] - Exponential backoff parameters.
     * @param {number} [retryParams.backoff.min=1000] - Minimum backoff duration in milliseconds.
     * @param {number} [retryParams.backoff.max=300000] - Maximum backoff duration in milliseconds.
     * @param {number} [retryParams.backoff.jitter=0.1] - Jitter factor to randomize each backoff.
     * @param {number} [retryParams.backoff.factor=1.5] - Multiplicative factor for each successive backoff.
     */
    constructor(retryParams) {
        this.retryParams = {
            timeoutS: (retryParams && retryParams.timeoutS) || 300,
            maxRetries: retryParams && retryParams.maxRetries,
            backoff: {
                min: (retryParams && retryParams.backoff && retryParams.backoff.min) || 1000,
                max: (retryParams && retryParams.backoff && retryParams.backoff.max) || 300000,
                jitter: (retryParams && retryParams.backoff && retryParams.backoff.jitter) || 0.1,
                factor: (retryParams && retryParams.backoff && retryParams.backoff.factor) || 1.5,
            },
        };
    }

    retryAsync({ actionDesc, logFields, noTimeout, actionFunc, shouldRetryFunc, onRetryFunc, log, maxRetries }) {
        return new Promise((resolve, reject) => {
            this.retry({
                actionDesc,
                logFields,
                noTimeout,
                shouldRetryFunc,
                onRetryFunc,
                log,
                maxRetries,
                actionFunc: (done, nbRetries) =>
                    Promise.resolve(actionFunc(nbRetries))
                        .then(result => done(null, result))
                        .catch(done),
            }, (err, result) => err ? reject(err) : resolve(result));
        });
    }

    retry(args, done) {
        const { actionDesc, logFields, noTimeout,
                actionFunc, shouldRetryFunc, onRetryFunc, log } = args;
        const backoffCtx = new BackOff(this.retryParams.backoff);
        let nbRetries = 0;
        const startTime = noTimeout ? undefined : Date.now();
        const maxRetries = args.maxRetries || this.retryParams.maxRetries;

        // FIXME workaround for S3C-4457:
        //
        // It seems the S3 client may call its callback multiple times
        // in an unknown corner case (the callback passed to the
        // send() function, like in ReplicateObject._setupRolesOnce()).
        //
        // Until we find the root cause, we catch duplicate calls and
        // log them instead of crashing the process with an exception
        // raised from the async module.
        //
        let cbCalled = false;
        const doneOnce = function doneWrapper(...args) {
            if (!cbCalled) {
                cbCalled = true;
                done.apply(done, args);
            } else {
                log.warn('callback was already called', Object.assign({
                    method: 'BackbeatTask.retry',
                }, logFields || {}));
            }
        };

        const _handleRes = (...args) => {
            const err = args[0];
            if (err) {
                if (err.retryable === undefined) {
                    // Network/socket errors are actually retryable, though they do not get flagged
                    // as such by AWS-SDK client
                    if (err.code === 'ECONNRESET' || err.code === 'EPIPE' ||
                        err.code === 'ETIMEDOUT') {
                        err.retryable = true;
                    } else if (err.name === 'TimeoutError' ||
                        err.message?.includes('ECONNRESET') ||
                        err.message?.includes('EPIPE')) {
                        // sdk v3 errors
                        err.retryable = true;
                    }
                }

                if (!shouldRetryFunc(err)) {
                    return doneOnce(...args);
                }

                const now = Date.now();
                const retriesMaxedOut = nbRetries >= maxRetries;
                const timeoutReached = startTime !== undefined &&
                    now >= (startTime + this.retryParams.timeoutS * 1000);
                // Give up if max retries reached or if timeout reached and
                // the entry has been retried at least once
                if (retriesMaxedOut || (timeoutReached && nbRetries > 0)) {
                    log.error('giving up processing as retries ended',
                        Object.assign({
                            method: 'BackbeatTask.retry',
                            nbRetries,
                            retryTotalMs: `${now - startTime}`,
                            actionDesc,
                            retriesMaxedOut,
                            timeoutReached,
                        }, logFields || {}), log);
                    return doneOnce(...args);
                }

                if (onRetryFunc) {
                    onRetryFunc(err);
                }

                const retryDelayMs = backoffCtx.duration();
                log.info('scheduling retry due to temporary failure',
                    Object.assign({ nbRetries, actionDesc,
                        method: 'BackbeatTask.retry',
                        retryDelay: `${retryDelayMs}ms` }, logFields || {}));
                nbRetries += 1;
                return setTimeout(() => actionFunc(_handleRes, nbRetries), retryDelayMs);
            }
            if (nbRetries > 0) {
                const retryTotalMs = Date.now() - startTime;
                log.info('successfully processed entry after retries',
                    Object.assign({ method: 'BackbeatTask.retry', actionDesc,
                    nbRetries, retryTotalMs }, logFields || {}));
            }
            return doneOnce(...args);
        };
        actionFunc(_handleRes, nbRetries);
    }
}

module.exports = BackbeatTask;
