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
