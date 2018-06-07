const assert = require('assert');

const BackbeatRequest = require('../../../lib/api/BackbeatRequest');

describe('BackbeatRequest helper class', () => {
    it('should not store any route details for healthcheck routes', () => {
        const req = new BackbeatRequest({ url: '/_/healthcheck' });
        const details = req.getRouteDetails();

        assert.deepStrictEqual(details, {});
    });

    describe('_parseRoute', () => {
        it('should parse metrics routes and store internally as route details',
        () => {
            const req = new BackbeatRequest({
                url: '/_/metrics/crr/all',
                method: 'GET',
            });
            const details = req.getRouteDetails();

            assert.strictEqual(details.category, 'metrics');
            assert.strictEqual(details.extension, 'crr');
            assert.strictEqual(details.site, 'all');
            assert.strictEqual(details.type, 'all');

            const req2 = new BackbeatRequest({
                url: '/_/metrics/crr/test/backlog',
                method: 'GET',
            });
            const details2 = req2.getRouteDetails();

            assert.strictEqual(details2.site, 'test');
            assert.strictEqual(details2.type, 'backlog');
        });

        it('should parse crr failed routes and store internally as route ' +
        'details', () => {
            const req = new BackbeatRequest({
                url: '/_/crr/failed?marker=testmarker',
                method: 'GET',
            });
            const details = req.getRouteDetails();

            assert.strictEqual(details.extension, 'crr');
            assert.strictEqual(details.status, 'failed');
            assert.strictEqual(details.marker, 'testmarker');
            assert.strictEqual(req.getHTTPMethod(), 'GET');

            const req2 = new BackbeatRequest({
                url: '/_/crr/failed',
                method: 'POST',
            });
            const details2 = req2.getRouteDetails();

            assert.strictEqual(details2.extension, 'crr');
            assert.strictEqual(details2.status, 'failed');
            assert.strictEqual(req2.getHTTPMethod(), 'POST');

            const req3 = new BackbeatRequest({
                url: '/_/crr/failed/mybucket/mykey/myvId',
                method: 'GET',
            });
            const details3 = req3.getRouteDetails();

            assert.strictEqual(details3.extension, 'crr');
            assert.strictEqual(details3.status, 'failed');
            assert.strictEqual(details3.bucket, 'mybucket');
            assert.strictEqual(details3.key, 'mykey');
            assert.strictEqual(details3.versionId, 'myvId');
        });

        it('should parse monitoring routes and store internally as route ' +
        'details', () => {
            const req = new BackbeatRequest({
                url: '/_/monitoring/metrics',
                method: 'GET',
            });
            const details = req.getRouteDetails();

            assert.strictEqual(details.category, 'monitoring');
            assert.strictEqual(details.type, 'metrics');
        });
    });

    it('should set route without prefix if valid route has valid prefix',
    () => {
        const req = new BackbeatRequest({
            url: '/_/healthcheck',
            method: 'GET',
        });
        const route = req.getRoute();
        const validPrefix = req.getHasValidPrefix();

        assert.strictEqual(route, 'healthcheck');
        assert.strictEqual(validPrefix, true);

        const req2 = new BackbeatRequest({
            url: '/healthcheck',
            method: 'GET',
        });
        const route2 = req2.getRoute();
        const validPrefix2 = req2.getHasValidPrefix();

        // Uses the original route when prefix is incorrect (for error logs)
        assert.strictEqual(route2, '/healthcheck');
        assert.strictEqual(validPrefix2, false);
    });
});
