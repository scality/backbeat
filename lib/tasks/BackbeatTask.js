const BackOff = require('backo');
const joi = require('joi');

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
        const backoffCtx = new BackOff(this.retryParams.backoff);
        let nbRetries = 0;
        const startTime = Date.now();

        function _handleRes(...args) {
            const err = args[0];
            if (err) {
                if (!shouldRetryFunc(err)) {
                    return done(err);
                }
                if (onRetryFunc) {
                    onRetryFunc(err);
                }

                const now = Date.now();
                const retriesExceeded = nbRetries > this.retryParams.maxRetries;
                const timeoutReached = now >
                    (startTime + this.retryParams.timeoutS * 1000);
                if (retriesExceeded || timeoutReached) {
                    log.error('giving up replication as retries exceeded',
                        Object.assign({
                            nbRetries,
                            retryTotalMs: `${now - startTime}`,
                            actionDesc,
                            retriesExceeded,
                            timeoutReached,
                        }, logFields || {}), log);
                    return done(err);
                }
                const retryDelayMs = backoffCtx.duration();
                log.info('scheduling retry due to temporary failure',
                    Object.assign({ nbRetries, actionDesc,
                        retryDelay: `${retryDelayMs}ms` }, logFields || {}));
                nbRetries += 1;
                return setTimeout(() => actionFunc(_handleRes), retryDelayMs);
            }
            if (nbRetries > 0) {
                const retryTotalMs = Date.now() - startTime;
                log.info('replication succeeded after retries',
                    Object.assign({ actionDesc, nbRetries, retryTotalMs },
                        logFields || {}));
            }
            return done(...args);
        }
        actionFunc(_handleRes);
    }
}

module.exports = BackbeatTask;
