const assert = require('assert');
const routes = require('../../../lib/api/routes');

describe('routes', () => {
    it('should generate the correct routes with default locations', () => {
        const locations = {
            crr: ['site1', 'site2'],
            ingestion: ['site3'],
            lifecycle: ['site4'],
        };
        const disableAdditionalRoutes = false;
        const result = routes(locations, disableAdditionalRoutes);

        assert.strictEqual(result.length, 19);

        assert.deepStrictEqual(result[0], {
            httpMethod: 'GET',
            category: 'healthcheck',
            type: 'basic',
            method: 'getHealthcheck',
            extensions: {},
        });

        assert.deepStrictEqual(result[1], {
            httpMethod: 'GET',
            category: 'metrics',
            type: 'backlog',
            extensions: { crr: ['site1', 'site2', 'all'] },
            method: 'getBacklog',
            dataPoints: ['opsPending', 'bytesPending'],
        });

        assert.deepStrictEqual(result[2], {
            httpMethod: 'GET',
            category: 'metrics',
            type: 'completions',
            extensions: {
                crr: ['site1', 'site2', 'all'],
                ingestion: ['site3', 'all'],
            },
            method: 'getCompletions',
            dataPoints: ['opsDone', 'bytesDone'],
        });
    });

    it('should generate the correct routes with additional routes disabled', () => {
        const locations = {
            crr: ['site1', 'site2'],
            ingestion: ['site3'],
            lifecycle: ['site4'],
        };
        const disableAdditionalRoutes = true;
        const result = routes(locations, disableAdditionalRoutes);

        assert.strictEqual(result.length, 8);
        assert.deepStrictEqual(result[0], {
            httpMethod: 'GET',
            category: 'healthcheck',
            type: 'basic',
            method: 'getHealthcheck',
            extensions: {},
        });

        assert.deepStrictEqual(result[1], {
            httpMethod: 'GET',
            category: 'metrics',
            type: 'backlog',
            extensions: { crr: ['site1', 'site2', 'all'] },
            method: 'getBacklog',
            dataPoints: ['opsPending', 'bytesPending'],
        });

        assert.deepStrictEqual(result[2], {
            httpMethod: 'GET',
            category: 'metrics',
            type: 'completions',
            extensions: {
                crr: ['site1', 'site2', 'all'],
                ingestion: ['site3', 'all'],
            },
            method: 'getCompletions',
            dataPoints: ['opsDone', 'bytesDone'],
        });
    });

    it('should handle empty locations', () => {
        const locations = {};
        const disableAdditionalRoutes = false;
        const result = routes(locations, disableAdditionalRoutes);

        assert.strictEqual(result.length, 19);

        assert.deepStrictEqual(result[0], {
            httpMethod: 'GET',
            category: 'healthcheck',
            type: 'basic',
            method: 'getHealthcheck',
            extensions: {},
        });

        assert.deepStrictEqual(result[1], {
            httpMethod: 'GET',
            category: 'metrics',
            type: 'backlog',
            extensions: { crr: ['all'] },
            method: 'getBacklog',
            dataPoints: ['opsPending', 'bytesPending'],
        });

        assert.deepStrictEqual(result[2], {
            httpMethod: 'GET',
            category: 'metrics',
            type: 'completions',
            extensions: {
                crr: ['all'],
                ingestion: ['all'],
            },
            method: 'getCompletions',
            dataPoints: ['opsDone', 'bytesDone'],
        });

    });
});
