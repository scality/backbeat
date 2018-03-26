/*
    This file contains all unique details about API routes exposed by Backbeats
    API server.
*/

const redisKeys = require('../../extensions/replication/constants').redisKeys;

module.exports = [
    {
        category: 'healthcheck',
        type: 'basic',
        method: 'getHealthcheck',
    },
    {
        category: 'metrics',
        type: 'backlog',
        extensions: { crr: ['all'] },
        method: 'getBacklog',
        dataPoints: [redisKeys.ops, redisKeys.opsDone, redisKeys.bytes,
            redisKeys.bytesDone],
    },
    {
        category: 'metrics',
        type: 'completions',
        extensions: { crr: ['all'] },
        method: 'getCompletions',
        dataPoints: [redisKeys.opsDone, redisKeys.bytesDone],
    },
    {
        category: 'metrics',
        type: 'throughput',
        extensions: { crr: ['all'] },
        method: 'getThroughput',
        dataPoints: [redisKeys.opsDone, redisKeys.bytesDone],
    },
    {
        category: 'metrics',
        type: 'all',
        extensions: { crr: ['all'] },
        method: 'getAllMetrics',
        dataPoints: [redisKeys.ops, redisKeys.opsDone, redisKeys.bytes,
            redisKeys.bytesDone],
    },
];
