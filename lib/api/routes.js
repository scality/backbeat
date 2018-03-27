/*
    This file contains all unique details about API routes exposed by Backbeats
    API server.
*/

const redisKeys = require('../../extensions/replication/constants').redisKeys;

const testIsOn = process.env.TEST_SWITCH === '1';
const config = testIsOn ? require('../../tests/config.json') :
                          require('../../conf/Config');


const boostrapList = config.extensions.replication.destination.bootstrapList;
const allSites = boostrapList.map(item => item.site);

module.exports = [
    {
        category: 'healthcheck',
        type: 'basic',
        method: 'getHealthcheck',
    },
    {
        category: 'metrics',
        type: 'backlog',
        extensions: { crr: [...allSites, 'all'] },
        method: 'getBacklog',
        dataPoints: [redisKeys.ops, redisKeys.opsDone, redisKeys.bytes,
            redisKeys.bytesDone],
    },
    {
        category: 'metrics',
        type: 'completions',
        extensions: { crr: [...allSites, 'all'] },
        method: 'getCompletions',
        dataPoints: [redisKeys.opsDone, redisKeys.bytesDone],
    },
    {
        category: 'metrics',
        type: 'throughput',
        extensions: { crr: [...allSites, 'all'] },
        method: 'getThroughput',
        dataPoints: [redisKeys.opsDone, redisKeys.bytesDone],
    },
    {
        category: 'metrics',
        type: 'all',
        extensions: { crr: [...allSites, 'all'] },
        method: 'getAllMetrics',
        dataPoints: [redisKeys.ops, redisKeys.opsDone, redisKeys.bytes,
            redisKeys.bytesDone],
    },
];
