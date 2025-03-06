const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');

const BackbeatTask = require('../../../../lib/tasks/BackbeatTask');

const logger = new werelogs.Logger('BackbeatTask:test');

describe('BackbeatTask', () => {
    describe('constructor', () => {
        it('should set default retryParams when none is provided', () => {
            const task = new BackbeatTask();
            const expectedParams = {
                timeoutS: 300,
                backoff: {
                    min: 1000,
                    max: 300000,
                    jitter: 0.1,
                    factor: 1.5,
                },
            };
            assert.deepStrictEqual(task.retryParams, expectedParams);
        });

        it('should use provided retryParams', () => {
            const customParams = {
                timeoutS: 100,
                backoff: {
                    min: 2000,
                    max: 600000,
                    jitter: 0.2,
                    factor: 2,
                },
            };
            const task = new BackbeatTask(customParams);
            assert.deepStrictEqual(task.retryParams, customParams);
        });

        it('should use provided timeoutS', () => {
            const customParams = {
                timeoutS: 100,
            };

            const expectedParams = {
                timeoutS: 100,
                backoff: {
                    min: 1000,
                    max: 300000,
                    jitter: 0.1,
                    factor: 1.5,
                },
            };
            const task = new BackbeatTask(customParams);
            assert.deepStrictEqual(task.retryParams, expectedParams);
        });

        it('should use provided backoff.min', () => {
            const customParams = {
                backoff: {
                    min: 2000,
                },
            };

            const expectedParams = {
                timeoutS: 300,
                backoff: {
                    min: 2000,
                    max: 300000,
                    jitter: 0.1,
                    factor: 1.5,
                },
            };
            const task = new BackbeatTask(customParams);
            assert.deepStrictEqual(task.retryParams, expectedParams);
        });
    });

    describe('retry method with sinon fake timers', () => {
        let clock;

        beforeEach(() => {
            clock = sinon.useFakeTimers();
        });

        afterEach(() => {
            clock.restore();
        });

        it('should respect a custom timeoutS and stop retrying once exceeded', done => {
            const task = new BackbeatTask({
                timeoutS: 2,
                backoff: {
                    min: 10,
                    max: 100,
                    jitter: 0,
                    factor: 1,
                },
            });

            let attempts = 0;
            task.retry({
                actionDesc: 'test short timeout',
                actionFunc: cb => {
                    attempts += 1;
                    cb(new Error('test error'));
                },
                shouldRetryFunc: () => true,
                onRetryFunc: () => {
                    logger.info(`retry attempt #${attempts}`);
                },
                log: logger,
            }, err => {
                assert(err, 'expected an error after retries timed out');
                assert(attempts > 1, `expected multiple attempts, got ${attempts}`);
                done();
            });

            clock.tick(5000);
        });

        it('should handle and trace double callback (S3C-4457 workaround)', done => {
            const task = new BackbeatTask();

            let cbCalledCount = 0;
            let inRetry = false;

            task.retry({
                actionDesc: 'do test action',
                actionFunc: cb => {
                    logger.info('actionFunc called');
                    if (inRetry) {
                        cb();
                    } else {
                        cb(new Error('OOPS'));
                        inRetry = true;
                    }
                    setTimeout(cb, 2000);
                },
                shouldRetryFunc: () => true,
                logFields: {
                    testLog: 'a log field',
                },
                log: logger,
            }, err => {
                assert.ifError(err, 'did not expect an error after final success');
                cbCalledCount += 1;
            });

            clock.tick(3000);

            assert.strictEqual(cbCalledCount, 1, 'callback was unexpectedly called multiple times');
            done();
        });
    });
});
