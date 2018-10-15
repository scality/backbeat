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
            { url: '/_/metrics/crr/all', method: 'GET' },
            { url: '/_/healthcheck', method: 'GET' },
            { url: '/_/metrics/crr/all/backlog', method: 'GET' },
            { url: '/_/metrics/crr/all/completions', method: 'GET' },
            { url: '/_/metrics/crr/all/failures', method: 'GET' },
            { url: '/_/metrics/crr/all/throughput', method: 'GET' },
            { url: '/_/crr/failed/mybucket/mykey/vId', method: 'GET' },
            { url: '/_/crr/failed?mymarker', method: 'GET' },
            // invalid params but will default to getting all buckets
            { url: '/_/crr/failed/mybucket', method: 'GET' },
            { url: '/_/crr/failed', method: 'POST' },
        ].forEach(request => {
            const req = new BackbeatRequest(request);
            const routeError = bbapi.findValidRoute(req);

            assert.equal(routeError, null);
        });

        // invalid routes
        [
            { url: '/_/invalid/crr/all', method: 'GET' },
            { url: '/_/metrics/ext/all', method: 'GET' },
            { url: '/_/metrics/crr/test', method: 'GET' },
            { url: '/_/metrics/crr/all/backlo', method: 'GET' },
            { url: '/_/metrics/crr/all/completionss', method: 'GET' },
            { url: '/_/metrics/crr/all/failures', method: 'POST' },
            { url: '/_/metrics/crr/all/fail', method: 'GET' },
            { url: '/_/invalid/crr/all', method: 'GET' },
            // // invalid http verb
            { url: '/_/healthcheck', method: 'POST' },
        ].forEach(request => {
            const req = new BackbeatRequest(request);
            const routeError = bbapi.findValidRoute(req);

            assert(routeError);
        });
    });

    it('should calculate the average throughput through redis intervals',
    () => {
        bbapi._getData = function overwriteGetData(details, data, cb) {
            return cb(null, [
                // ops
                {
                    requests: [25, 0, 0, 25],
                    errors: [0, 0, 0, 0],
                },
                // bytes
                {
                    requests: [1024, 0, 0, 1024],
                    errors: [0, 0, 0, 0],
                },
            ]);
        };

        bbapi.getThroughput('', (err, data) => {
            assert.ifError(err);

            assert(data.throughput);
            assert(data.throughput.description);
            assert(data.throughput.results);

            const results = data.throughput.results;
            const count = Number.parseFloat(results.count);
            const size = Number.parseFloat(results.size);
            // count must be at least 0.03 given the first interval and never be
            // over double its size. This can only happen when interval time has
            // just reset and the just-expired interval data fully applies
            assert(count >= 0.03 && count <= 0.06);
            assert(size >= 1.14 && size <= 2.28);
        });
    });
});
