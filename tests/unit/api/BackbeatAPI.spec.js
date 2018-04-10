const assert = require('assert');

const BackbeatAPI = require('../../../lib/api/BackbeatAPI');
const BackbeatRequest = require('../../../lib/api/BackbeatRequest');
const config = require('../../config.json');

const fakeLogger = {
    trace: () => {},
    error: () => {},
    info: () => {},
    debug: () => {},
};

describe('BackbeatAPI', () => {
    let bbapi;

    before(() => {
        bbapi = new BackbeatAPI(config, fakeLogger, { timer: true });
    });

    it('should validate routes', () => {
        // valid routes
        [
            '/_/metrics/crr/all',
            '/_/healthcheck',
            '/_/metrics/crr/all/backlog',
            '/_/metrics/crr/all/completions',
            '/_/metrics/crr/all/throughput',
        ].forEach(path => {
            const req = new BackbeatRequest({ url: path });

            assert.ok(bbapi.isValidRoute(req));
        });

        // invalid routes
        [
            '/_/invalid/crr/all',
            '/_/metrics/ext/all',
            '/_/metrics/crr/test',
            '/_/metrics/crr/all/backlo',
            '/_/metrics/crr/all/completionss',
        ].forEach(path => {
            const req = new BackbeatRequest({ url: path });

            assert.equal(bbapi.isValidRoute(req), false);
        });
    });
});
