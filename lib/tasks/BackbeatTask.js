const BackOff = require('backo');
const joi = require('@hapi/joi');

const BACKOFF_PARAMS = { min: 1000, max: 300000, jitter: 0.1, factor: 1.5 };

const backbeatTaskConfigJoi = {
    retryTimeoutS: joi.number().default(300),
};

/**
 * @class BackbeatTask
 */
class BackbeatTask {
    /**
     * @constructor
     * @param {Object} [config] - config object containing various
     *   task-specific configuration
     * @param {Number} [config.retryTimeoutS] - timeout of retries, in
     *   seconds
     */
    constructor(config) {
        this.config = joi.attempt(config || {}, backbeatTaskConfigJoi,
                                  'invalid backbeat task config');
    }

    retry(args, done) {
        const { actionDesc, logFields,
                actionFunc, shouldRetryFunc, onRetryFunc, log } = args;
        const backoffCtx = new BackOff(BACKOFF_PARAMS);
        let nbRetries = 0;
        const startTime = Date.now();
        const self = this;

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

        function _handleRes(...args) {
            const err = args[0];
            if (err) {
                if (!shouldRetryFunc(err)) {
                    return doneOnce(err);
                }
                if (onRetryFunc) {
                    onRetryFunc(err);
                }

                const now = Date.now();
                if (now > (startTime + self.config.retryTimeoutS * 1000)) {
                    log.error(
                        `retried for too long to ${actionDesc}, giving up`,
                        Object.assign({
                            nbRetries,
                            retryTotalMs: `${now - startTime}`,
                        }, logFields || {}), log);
                    return doneOnce(err);
                }
                const retryDelayMs = backoffCtx.duration();
                log.info(`temporary failure to ${actionDesc}, scheduled retry`,
                         Object.assign({ nbRetries,
                                         retryDelay: `${retryDelayMs}ms` },
                                       logFields || {}));
                nbRetries += 1;
                return setTimeout(() => actionFunc(_handleRes, nbRetries), retryDelayMs);
            }
            if (nbRetries > 0) {
                const now = Date.now();
                log.info(`succeeded to ${actionDesc} after retries`,
                         Object.assign({
                             nbRetries,
                             retryTotalMs: `${now - startTime}`,
                         }, logFields || {}));
            }
            return doneOnce(...args);
        }
        actionFunc(_handleRes, nbRetries);
    }
}

module.exports = BackbeatTask;
