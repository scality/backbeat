/*
    This file contains all unique details about API routes exposed by Backbeats
    API server.
*/

const redisKeys = require('../../extensions/replication/constants').redisKeys;

module.exports = [
    // Route: /_/healthcheck
    {
        httpMethod: 'GET',
        category: 'healthcheck',
        type: 'basic',
        method: 'getHealthcheck',
        extensions: {},
    },
    // Route: /_/metrics/crr/all/backlog
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'backlog',
        extensions: { crr: ['all'] },
        method: 'getBacklog',
        dataPoints: [redisKeys.ops, redisKeys.opsDone, redisKeys.bytes,
            redisKeys.bytesDone],
    },
    // Route: /_/metrics/crr/all/completions
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'completions',
        extensions: { crr: ['all'] },
        method: 'getCompletions',
        dataPoints: [redisKeys.opsDone, redisKeys.bytesDone],
    },
    // Route: /_/metrics/crr/all/throughput
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'throughput',
        extensions: { crr: ['all'] },
        method: 'getThroughput',
        dataPoints: [redisKeys.opsDone, redisKeys.bytesDone],
    },
    // Route: /_/metrics/crr/all
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'all',
        extensions: { crr: ['all'] },
        method: 'getAllMetrics',
        dataPoints: [redisKeys.ops, redisKeys.opsDone, redisKeys.bytes,
            redisKeys.bytesDone],
    },
    // Route: /_/crr/failed?sitename=<site>&marker=<marker>
    {
        httpMethod: 'GET',
        type: 'all',
        extensions: { crr: ['failed'] },
        method: 'getSiteFailedCRR',
    },
    // Route: /_/crr/failed/<bucket>/<key>?versionId=<versionId>
    {
        httpMethod: 'GET',
        type: 'specific',
        extensions: { crr: ['failed'] },
        method: 'getFailedCRR',
    },
    // Route: /_/crr/failed
    {
        httpMethod: 'POST',
        type: 'all',
        extensions: { crr: ['failed'] },
        method: 'retryFailedCRR',
    },
];
