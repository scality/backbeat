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
    // Route: /_/healthcheck
    {
        httpMethod: 'GET',
        category: 'healthcheck',
        type: 'basic',
        method: 'getHealthcheck',
        extensions: {},
    },
    // Route: /_/metrics/crr/<location>/pending
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'pending',
        extensions: { crr: [...allSites, 'all'] },
        method: 'getPending',
        dataPoints: [redisKeys.opsPending, redisKeys.bytesPending],
    },
    // Route: /_/metrics/crr/<location>/backlog
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'backlog',
        extensions: { crr: [...allSites, 'all'] },
        method: 'getBacklog',
        dataPoints: [redisKeys.ops, redisKeys.opsDone, redisKeys.bytes,
            redisKeys.bytesDone],
    },
    // Route: /_/metrics/crr/<location>/completions
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'completions',
        extensions: { crr: [...allSites, 'all'] },
        method: 'getCompletions',
        dataPoints: [redisKeys.opsDone, redisKeys.bytesDone],
    },
    // Route: /_/metrics/crr/<location>/failures
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'failures',
        extensions: { crr: [...allSites, 'all'] },
        method: 'getFailedMetrics',
        dataPoints: [redisKeys.opsFail, redisKeys.bytesFail],
    },
    // Route: /_/metrics/crr/<location>/throughput
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'throughput',
        extensions: { crr: [...allSites, 'all'] },
        method: 'getThroughput',
        dataPoints: [redisKeys.opsDone, redisKeys.bytesDone],
    },
    // Route: /_/metrics/crr/<location>/all
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'all',
        extensions: { crr: [...allSites, 'all'] },
        method: 'getAllMetrics',
        dataPoints: [redisKeys.ops, redisKeys.opsDone, redisKeys.opsFail,
            redisKeys.bytes, redisKeys.bytesDone, redisKeys.bytesFail,
            redisKeys.opsPending, redisKeys.bytesPending],
    },
    // Route: /_/metrics/crr/<site>/progress/<bucket>/<key>
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'progress',
        level: 'object',
        extensions: { crr: [...allSites] },
        method: 'getObjectProgress',
        dataPoints: [redisKeys.objectBytes, redisKeys.objectBytesDone],
    },
    // Route: /_/metrics/crr/<site>/throughput/<bucket>/<key>
    {
        httpMethod: 'GET',
        category: 'metrics',
        type: 'throughput',
        level: 'object',
        extensions: { crr: [...allSites] },
        method: 'getObjectThroughput',
        dataPoints: [redisKeys.objectBytesDone],
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
