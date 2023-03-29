'use strict'; // eslint-disable-line

const assert = require('assert');
const { errors } = require('arsenal');

class BackbeatClientMock {
    constructor(failures) {
        this.failures = failures;

        this.calls = {};
        this.stubMethod('deleteObjectFromExpiration', {});
    }

    makeRetryableError() {
        const err = errors.ServiceUnavailable.customizeDescription('failing on purpose');
        err.retryable = true;
        return err;
    }

    stubMethod(methodName, successResult, stubError) {
        this.calls[methodName] = 0;

        this[methodName] = (params, done) => {
            this.calls[methodName]++;

            if (this.failures[methodName] >= this.calls[methodName]) {
                const error = stubError || this.makeRetryableError();

                if (done) {
                    return process.nextTick(done, error);
                }

                return {
                    send: cb => process.nextTick(cb, error),
                    on: () => {},
                };
            }

            if (done) {
                return process.nextTick(done, null, successResult);
            }

            return {
                send: cb => process.nextTick(cb, null, successResult),
                on: () => {},
            };
        };
    }

    verifyRetries() {
        Object.keys(this.failures).forEach(f => {
            assert.strictEqual(this.calls[f], this.failures[f] + 1,
                `did not retry ${this.failures[f]} times`);
        });
    }

    verifyNoRetries() {
        Object.keys(this.failures).forEach(f => {
            assert.strictEqual(this.calls[f], 1,
                `called ${this.calls[f]} times, expected 1`);
        });
    }
}

module.exports = {
    BackbeatClientMock,
};
