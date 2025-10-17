'use strict';

const assert = require('assert');
const { errors } = require('arsenal');

class BackbeatClientMock {
    constructor(failures) {
        this.failures = failures;
        this.calls = {};
        Object.keys(failures).forEach(commandName => {
            this.calls[commandName] = 0;
        });
    }

    makeRetryableError() {
        const err = errors.ServiceUnavailable.customizeDescription('failing on purpose');
        err.retryable = true;
        return err;
    }

    send(command) {
        const commandName = command.constructor.name;

        if (!this.calls[commandName]) {
            this.calls[commandName] = 0;
        }

        this.calls[commandName]++;

        if (this.failures[commandName] && this.failures[commandName] >= this.calls[commandName]) {
            return Promise.reject(this.makeRetryableError());
        }

        return Promise.resolve({});
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
