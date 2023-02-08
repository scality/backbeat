const assert = require('assert');

const BackbeatAPI = require('../../../lib/api/BackbeatAPI');
const BackbeatRequest = require('../../../lib/api/BackbeatRequest');
const config = require('../../../lib/Config');
const fakeLogger = require('../../utils/fakeLogger');
const setupIngestionSiteMock = require('../../utils/mockIngestionSite');
const locationConfig = require('../../../conf/locationConfig.json');

describe('BackbeatAPI', () => {
    let bbapi;

    const destconfig = config.extensions.replication.destination;
    const { site } = destconfig.bootstrapList[0];
    const ingestSite = `${destconfig.bootstrapList[1].site}-ingestion`;
    const coldSite = Object.keys(locationConfig)
        .filter(site => locationConfig[site].isCold)[0];

    before(() => {
        setupIngestionSiteMock();
        bbapi = new BackbeatAPI(config, fakeLogger, { timer: true });
    });

    // valid routes
    [
        { url: '/_/metrics/crr/all', method: 'GET' },
        { url: '/_/metrics/ingestion/all', method: 'GET' },
        { url: '/_/healthcheck', method: 'GET' },
        { url: '/_/metrics/crr/all/backlog', method: 'GET' },
        { url: '/_/metrics/crr/all/completions', method: 'GET' },
        { url: '/_/metrics/crr/all/failures', method: 'GET' },
        { url: '/_/metrics/crr/all/throughput', method: 'GET' },
        { url: '/_/metrics/ingestion/all/completions', method: 'GET' },
        { url: `/_/metrics/ingestion/${ingestSite}/completions`,
            method: 'GET' },
        { url: '/_/metrics/ingestion/all/throughput', method: 'GET' },
        { url: `/_/metrics/ingestion/${ingestSite}/throughput`, method: 'GET' },
        { url: '/_/metrics/ingestion/all/pending', method: 'GET' },
        { url: `/_/metrics/ingestion/${ingestSite}/pending`, method: 'GET' },
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
        { url: '/_/lifecycle/pause', method: 'POST' },
        { url: '/_/lifecycle/resume', method: 'POST' },
        { url: '/_/lifecycle/resume/all/schedule', method: 'POST' },
        { url: '/_/lifecycle/resume', method: 'GET' },
        { url: '/_/lifecycle/status', method: 'GET' },
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
        { url: `/_/lifecycle/pause/${coldSite}`, method: 'POST' },
        { url: `/_/lifecycle/resume/${coldSite}`, method: 'POST' },
        { url: `/_/lifecycle/status/${coldSite}`, method: 'GET' },
        { url: '/_/configuration/workflows', method: 'POST' },
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
        // unavailable routes for given service
        { url: '/_/metrics/ingestion/all/backlog', method: 'GET' },
        { url: '/_/metrics/ingestion/all/failures', method: 'GET' },
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
        { url: '/_/metrics/ingestion/all/throughput', method: 'POST' },
        { url: `/_/metrics/ingestion/${site}/completions`, method: 'POST' },
        { url: `/_/metrics/ingestion/${site}/pending`, method: 'POST' },
        // invalid site for given service
        { url: `/_/ingestion/pause/${site}`, method: 'POST' },
        { url: `/_/ingestion/resume/${site}`, method: 'POST' },
        { url: `/_/ingestion/status/${site}`, method: 'GET' },
        // invalid site for given service
        { url: `/_/crr/pause/${ingestSite}`, method: 'POST' },
        { url: `/_/crr/resume/${ingestSite}`, method: 'POST' },
        { url: `/_/crr/status/${ingestSite}`, method: 'GET' },
        { url: `/_/ingestion/pause/${site}`, method: 'POST' },
        { url: `/_/ingestion/resume/${site}`, method: 'POST' },
        { url: `/_/ingestion/status/${site}`, method: 'GET' },
        { url: '/_/lifecycle/pause/non-existent', method: 'POST' },
        { url: '/_/lifecycle/resume/non-existent', method: 'POST' },
        { url: '/_/lifecycle/status/non-existent', method: 'GET' },
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

        bbapi.getThroughput({ service: 'crr' }, (err, data) => {
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

    [
        {
            details: {
                service: 'lifecycle',
                site: 'all',
                extensions: {
                    lifecycle: [
                        'us-east-1',
                        'us-east-2',
                        'location-dmf-v1',
                        'all',
                    ],
                },
            },
            expected: [
                'us-east-1',
                'us-east-2',
                'location-dmf-v1',
            ]
        },
        {
            details: {
                service: 'lifecycle',
                site: 'us-east-1',
                extensions: {
                    lifecycle: [
                        'us-east-1',
                        'us-east-2',
                        'location-dmf-v1',
                        'all',
                    ],
                },
            },
            expected: ['us-east-1']
        },
    ].forEach(params => {
        it('getRequestedSites:: should return correct list of requested sites', () => {
            const sites = bbapi.getRequestedSites(params.details);
            assert.deepStrictEqual(sites, params.expected);
        });
    });
});
