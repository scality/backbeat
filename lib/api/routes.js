/*
    This file contains all unique details about API routes exposed by Backbeats
    API server.
*/

const redisKeys = require('../../extensions/replication/constants').redisKeys;

const testIsOn = process.env.CI === 'true';
const config = testIsOn ? require('../../tests/config.json') :
                          require('../../conf/Config');
const boostrapList = config.extensions.replication.destination.bootstrapList;
const allSites = boostrapList.map(item => item.site);

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
        extensions: { crr: [...allSites, 'all'] },
        method: 'getBacklog',
        dataPoints: [redisKeys.ops, redisKeys.opsDone, redisKeys.bytes,
            redisKeys.bytesDone],
    },
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'completions',
        extensions: { crr: [...allSites, 'all'] },
        method: 'getCompletions',
        dataPoints: [redisKeys.opsDone, redisKeys.bytesDone],
    },
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'throughput',
        extensions: { crr: [...allSites, 'all'] },
        method: 'getThroughput',
        dataPoints: [redisKeys.opsDone, redisKeys.bytesDone],
    },
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'all',
        extensions: { crr: [...allSites, 'all'] },
        method: 'getAllMetrics',
        dataPoints: [redisKeys.ops, redisKeys.opsDone, redisKeys.bytes,
            redisKeys.bytesDone],
    },
    {
        httpMethod: 'GET',
        type: 'all',
        extensions: { crr: ['failed'] },
        method: 'getAllFailedCRR',
    },
    {
        httpMethod: 'GET',
        type: 'specific',
        extensions: { crr: ['failed'] },
        method: 'getFailedCRR',
    },
    {
        httpMethod: 'POST',
        type: 'all',
        extensions: { crr: ['failed'] },
        method: 'retryFailedCRR',
    },
];
