/*
    This file contains all unique details about API routes exposed by Backbeats
    API server.
*/

const redisKeys = require('../../extensions/replication/constants').redisKeys;

module.exports = [
    {
        httpMethod: 'GET',
        category: 'healthcheck',
        type: 'basic',
        method: 'getHealthcheck',
        extensions: {},
    },
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'backlog',
        extensions: { crr: ['all'] },
        method: 'getBacklog',
        dataPoints: [redisKeys.ops, redisKeys.opsDone, redisKeys.bytes,
            redisKeys.bytesDone],
    },
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'completions',
        extensions: { crr: ['all'] },
        method: 'getCompletions',
        dataPoints: [redisKeys.opsDone, redisKeys.bytesDone],
    },
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'throughput',
        extensions: { crr: ['all'] },
        method: 'getThroughput',
        dataPoints: [redisKeys.opsDone, redisKeys.bytesDone],
    },
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
