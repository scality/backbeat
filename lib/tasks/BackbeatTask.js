const BackOff = require('backo');

/**
 * @class BackbeatTask
 */
class BackbeatTask {

    constructor() {
        this.retryParams = {
            timeoutS: 300,
            backoff: {
                min: 1000,
                max: 300000,
                jitter: 0.1,
                factor: 1.5,
            },
        };
    }

    retry(args, done) {
        const { actionDesc, logFields,
                actionFunc, shouldRetryFunc, onRetryFunc, log } = args;
        const backoffCtx = new BackOff(this.retryParams.backoff);
        let nbRetries = 0;
        const startTime = Date.now();

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
                if (err.retryable === undefined &&
                    (err.code === 'ECONNRESET' || err.code === 'EPIPE' ||
                     err.code === 'ETIMEDOUT')) {
                    // Network/socket errors are actually retryable, though they do not get flagged
                    // as such by AWS-SDK client
                    err.retryable = true;
                }

                if (!shouldRetryFunc(err)) {
                    return doneOnce(err);
                }
                if (onRetryFunc) {
                    onRetryFunc(err);
                }

                const now = Date.now();
                const retriesMaxedOut = nbRetries >=
                    this.retryParams.maxRetries;
                const timeoutReached = now >=
                    (startTime + this.retryParams.timeoutS * 1000);
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
                    return doneOnce(err);
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
