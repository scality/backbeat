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

        const _handleRes = (...args) => {
            const err = args[0];
            if (err) {
                if (!shouldRetryFunc(err)) {
                    return done(err);
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
                    return done(err);
                }
                const retryDelayMs = backoffCtx.duration();
                log.info('scheduling retry due to temporary failure',
                    Object.assign({ nbRetries, actionDesc,
                        method: 'BackbeatTask.retry',
                        retryDelay: `${retryDelayMs}ms` }, logFields || {}));
                nbRetries += 1;
                return setTimeout(() => actionFunc(_handleRes), retryDelayMs);
            }
            if (nbRetries > 0) {
                const retryTotalMs = Date.now() - startTime;
                log.info('successfully processed entry after retries',
                    Object.assign({ method: 'BackbeatTask.retry', actionDesc,
                    nbRetries, retryTotalMs }, logFields || {}));
            }
            return done(...args);
        };
        actionFunc(_handleRes);
    }
}

module.exports = BackbeatTask;
