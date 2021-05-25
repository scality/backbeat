const assert = require('assert');

const werelogs = require('werelogs');

const BackbeatTask = require('../../../../lib/tasks/BackbeatTask');

const logger = new werelogs.Logger('BackbeatTask:test');

describe('BackbeatTask', () => {
    it('BackbeatTask.retry() should handle and trace double callback (S3C-4457)', done => {
        let cbCalled = false;
        let inRetry = false;
        const task = new BackbeatTask();
        task.retry({
            actionDesc: 'do test action',
            actionFunc: cb => {
                logger.info('actionFunc');
                if (inRetry) {
                    cb();
                } else {
                    cb(new Error('OOPS'));
                    inRetry = true;
                }
                // trigger a double callback after 5 seconds, ending with success
                setTimeout(cb, 2000);
            },
            shouldRetryFunc: () => true,
            logFields: {
                testLog: 'a log field',
            },
            log: logger,
        }, err => {
            assert.ifError(err);
            assert(!cbCalled);
            cbCalled = true;
            // Wait to check that the callback does not get called
            // again, to make sure the double callback is caught in
            // the retry logic.
            setTimeout(done, 2000);
        });
    }).timeout(10000);
});
