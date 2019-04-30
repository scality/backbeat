'use strict'; // eslint-disable-line strict

const assert = require('assert');

const { RedisClient } = require('arsenal').metrics;
const Metrics = require('../../../lib/api/Metrics');
const apiRoutes = require('../../../lib/api/routes');

// expirations
const EXPIRY = 86400; // 24 hours
const THROUGHPUT_EXPIRY = 900; // 15 minutes

// setup redis client
const config = {
    host: '127.0.0.1',
    port: 6379,
    enableOfflineQueue: false,
};
const fakeLogger = {
    trace: () => {},
    error: () => {},
};
const redisClient = new RedisClient(config, fakeLogger);

// setup stats model
const sites = ['site1', 'site2'];
const metrics = new Metrics({
    redisConfig: config,
    crrSites: ['site1', 'site2', 'all'],
    internalStart: Date.now() - (EXPIRY * 1000), // 24 hours ago.
}, fakeLogger);

// Since many methods were overwritten, these tests should validate the changes
// made to the original methods
describe('Metrics class', () => {
    afterEach(() => redisClient.clear(() => {}));

    it('should not crash on empty results', done => {
        const redisKeys = {
            ops: 'bb:crr:ops',
            bytes: 'bb:crr:bytes',
            opsDone: 'bb:crr:opsdone',
            bytesDone: 'bb:crr:bytesdone',
            bytesFail: 'bb:crr:bytesfail',
            opsFail: 'bb:crr:opsfail',
            failedCRR: 'bb:crr:failed',
            opsPending: 'bb:crr:bytespending',
            bytesPending: 'bb:crr:opspending',
        };
        const routes = apiRoutes(redisKeys, { crr: sites });
        const details = routes.find(route =>
            route.category === 'metrics' && route.type === 'all');
        details.site = 'all';
        details.service = 'crr';
        metrics.getAllMetrics(details, (err, res) => {
            assert.ifError(err);
            const expected = {
                pending: {
                    description: 'Number of pending replication operations ' +
                        '(count) and bytes (size)',
                    results: {
                        count: 0,
                        size: 0,
                    },
                },
                backlog: {
                    description: 'Number of incomplete replication operations' +
                        ' (count) and number of incomplete bytes transferred' +
                        ' (size)',
                    results: {
                        count: 0,
                        size: 0,
                    },
                },
                completions: {
                    description: 'Number of completed replication operations' +
                        ' (count) and number of bytes transferred (size) in ' +
                        `the last ${EXPIRY} seconds`,
                    results: {
                        count: 0,
                        size: 0,
                    },
                },
                failures: {
                    description: 'Number of failed replication operations ' +
                        `(count) and bytes (size) in the last ${EXPIRY} ` +
                        'seconds',
                    results: {
                        count: 0,
                        size: 0,
                    },
                },
                throughput: {
                    description: 'Current throughput for replication' +
                        ' operations in ops/sec (count) and bytes/sec (size) ' +
                        `in the last ${THROUGHPUT_EXPIRY} seconds`,
                    results: {
                        count: '0.00',
                        size: '0.00',
                    },
                },
            };
            assert.deepStrictEqual(res, expected);
            done();
        });
    });
});
