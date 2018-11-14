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

    const destconfig = config.extensions.replication.destination;
    const { site } = destconfig.bootstrapList[0];
    // TODO-FIX: currently not filtering sites by service
    const ingestSite = site;

    before(() => {
        bbapi = new BackbeatAPI(config, fakeLogger, { timer: true });
    });

    // valid routes
    [
        { url: '/_/metrics/crr/all', method: 'GET' },
        { url: '/_/healthcheck', method: 'GET' },
        { url: '/_/metrics/crr/all/backlog', method: 'GET' },
        { url: '/_/metrics/crr/all/completions', method: 'GET' },
        { url: '/_/metrics/crr/all/failures', method: 'GET' },
        { url: '/_/metrics/crr/all/throughput', method: 'GET' },
        { url: '/_/monitoring/metrics', method: 'GET' },
        { url: '/_/crr/failed/mybucket/mykey?versionId=test-myvId',
            method: 'GET' },
        { url: '/_/crr/failed?mymarker', method: 'GET' },
        // invalid params but will default to getting all buckets
        { url: '/_/crr/failed/mybucket', method: 'GET' },
        { url: '/_/crr/failed', method: 'POST' },
        { url: '/_/crr/pause', method: 'POST' },
        { url: '/_/crr/resume', method: 'POST' },
        { url: '/_/crr/resume/all/schedule', method: 'POST' },
        { url: '/_/crr/resume', method: 'GET' },
        { url: '/_/crr/status', method: 'GET' },
        { url: '/_/ingestion/pause', method: 'POST' },
        { url: '/_/ingestion/resume', method: 'POST' },
        { url: '/_/ingestion/resume/all/schedule', method: 'POST' },
        { url: '/_/ingestion/resume', method: 'GET' },
        { url: '/_/ingestion/status', method: 'GET' },
        { url: `/_/metrics/crr/${site}/throughput/mybucket/mykey` +
            '?versionId=test-myvId', method: 'GET' },
        { url: `/_/metrics/crr/${site}/progress/mybucket/mykey` +
            '?versionId=test-myvId', method: 'GET' },
        // valid site for given service
        { url: `/_/crr/pause/${site}`, method: 'POST' },
        { url: `/_/crr/resume/${site}`, method: 'POST' },
        { url: `/_/crr/status/${site}`, method: 'GET' },
        { url: `/_/ingestion/pause/${ingestSite}`, method: 'POST' },
        { url: `/_/ingestion/resume/${ingestSite}`, method: 'POST' },
        { url: `/_/ingestion/status/${ingestSite}`, method: 'GET' },
    ].forEach(request => {
        it(`should validate route: ${request.method} ${request.url}`, () => {
            const req = new BackbeatRequest(request);
            const routeError = bbapi.findValidRoute(req);

            assert.equal(routeError, null);
        });
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
        { url: '/_/metrics/pause/all', method: 'GET' },
        // invalid http verb
        { url: '/_/healthcheck', method: 'POST' },
        { url: '/_/monitoring/metrics', method: 'POST' },
        { url: '/_/crr/pause', method: 'GET' },
        { url: '/_/crr/status', method: 'POST' },
        { url: '/_/ingestion/pause', method: 'GET' },
        { url: '/_/ingestion/status', method: 'POST' },
        { url: '/_/metrics/crr/unknown-site/throughput/mybucket/mykey' +
            '?versionId=test-myvId', method: 'GET' },
        { url: '/_/metrics/crr/unknown-site/progress/mybucket/mykey' +
            '?versionId=test-myvId', method: 'GET' },
        // TODO-FIX: Once site filter by service is enabled
        // invalid site for given service
        // { url: `/_/crr/pause/${ingestSite}`, method: 'POST' },
        // { url: `/_/crr/resume/${ingestSite}`, method: 'POST' },
        // { url: `/_/crr/status/${ingestSite}`, method: 'GET' },
        // { url: `/_/ingestion/pause/${site}`, method: 'POST' },
        // { url: `/_/ingestion/resume/${site}`, method: 'POST' },
        // { url: `/_/ingestion/status/${site}`, method: 'GET' },
    ].forEach(request => {
        it(`should invalidate route: ${request.method} ${request.url}`, () => {
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
