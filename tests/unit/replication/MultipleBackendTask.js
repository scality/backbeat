const assert = require('assert');
const jsutil = require('arsenal').jsutil;

const config = require('../../config.json');
const MultipleBackendTask =
    require('../../../extensions/replication/tasks/MultipleBackendTask');
const log = require('../../utils/fakeLogger');
const { sourceEntry, destEntry } = require('../../utils/mockEntries');

let multipleBackendTask = null;

function createMultipleBackendTask() {
    return new MultipleBackendTask({
        getStateVars: () => ({
            repConfig: {
                queueProcessor: {
                    retryTimeoutS: 300,
                },
            },
            destConfig: config.extensions.replication.destination,
            site: 'test-site-2',
        }),
    });
}

function requestInitiateMPU(params, done) {
    const { retryable } = params;

    multipleBackendTask.backbeatSource = {
        multipleBackendInitiateMPU: () => ({
            httpRequest: { headers: {} },
            send: cb => cb({ retryable }),
            on: (action, cb) => cb(),
        }),
    };

    multipleBackendTask
        ._getAndPutMultipartUpload(sourceEntry, destEntry, log, err => {
            assert(err);
            return done();
        });
}

describe('MultipleBackendTask', function test() {
    this.timeout(5000);

    describe('::initiateMultipartUpload', () => {
        beforeEach(() => {
            multipleBackendTask = createMultipleBackendTask();
        });
        it('should use exponential backoff if retryable error ', done => {
            const doneOnce = jsutil.once(done);
            setTimeout(() => {
                // inhibits further retries
                multipleBackendTask.config.retryTimeoutS = 0;
                doneOnce();
            }, 4000); // Retries will exceed test timeout.
            requestInitiateMPU({ retryable: true }, doneOnce);
        });

        it('should not use exponential backoff if non-retryable error ', done =>
            requestInitiateMPU({ retryable: false }, done));
    });
});
