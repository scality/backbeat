const assert = require('assert');

const BackbeatRequest = require('../../../lib/api/BackbeatRequest');

describe('BackbeatRequest helper class', () => {
    it('should not store any route details for healthcheck routes', () => {
        const req = new BackbeatRequest({ url: '/_/healthcheck' });
        const details = req.getRouteDetails();

        assert.deepStrictEqual(details, {});
    });

    it('should parse routes and store internally as route details', () => {
        const req = new BackbeatRequest({ url: '/_/metrics/crr/all' });
        const details = req.getRouteDetails();

        assert.strictEqual(details.category, 'metrics');
        assert.strictEqual(details.extension, 'crr');
        assert.strictEqual(details.site, 'all');
        assert.strictEqual(details.metric, undefined);

        const req2 = new BackbeatRequest(
            { url: '/_/metrics/crr/test/backlog' });
        const details2 = req2.getRouteDetails();

        assert.strictEqual(details2.site, 'test');
        assert.strictEqual(details2.metric, 'backlog');
    });
});
