const assert = require('assert');
const async = require('async');
const http = require('http');
const Redis = require('ioredis');
const { Producer } = require('node-rdkafka');
const zookeeper = require('node-zookeeper-client');
const { RedisClient } = require('arsenal').metrics;
const { StatsModel } = require('arsenal').metrics;

const config = require('../../config.json');
const { makeRequest, getRequest, getResponseBody } =
    require('../utils/httpHelpers');
const getUrl = require('../utils/getUrl');
const fakeLogger = require('../utils/fakeLogger');
const { addMembers } = require('../utils/sortedSetHelpers');

const redisConfig = { host: '127.0.0.1', port: 6379 };
const TEST_REDIS_KEY_FAILED_CRR = 'test:bb:crr:failed';
const ZK_TEST_CRR_STATE_PATH = '/backbeattest/state';
const EPHEMERAL_NODE = 1;

const defaultOptions = {
    host: config.server.host,
    port: config.server.port,
    method: 'GET',
};

describe('API routes', () => {
    const redis = new Redis();
    const redisClient = new RedisClient(redisConfig, fakeLogger);
    const interval = 300;
    const expiry = 900;
    const statsClient = new StatsModel(redisClient, interval, expiry);

    it('should get a 404 route not found error response', () => {
        const url = getUrl('/_/invalidpath');

        http.get(url, res => {
            assert.equal(res.statusCode, 404);
        });
    });

    it('should get a 405 method not allowed from invalid http verb', done => {
        const options = Object.assign({}, defaultOptions);
        options.method = 'DELETE';
        options.path = '/_/healthcheck';

        const req = http.request(options, res => {
            assert.equal(res.statusCode, 405);
        });
        req.on('error', err => {
            assert.ifError(err);
        });
        req.end();
        done();
    });

    describe('healthcheck route', () => {
        let data;
        let healthcheckTimer;
        let resCode;
        let testProducer;

        function _doHealthcheckRequest(done) {
            const url = getUrl('/_/healthcheck');

            http.get(url, res => {
                resCode = res.statusCode;

                let rawData = '';
                res.on('data', chunk => {
                    rawData += chunk;
                });
                res.on('end', () => {
                    data = JSON.parse(rawData);
                    if (done) {
                        // only set in before() processing
                        done();
                    }
                });
            });
        }

        before(done => {
            async.series([
                next => {
                    testProducer = new Producer({
                        'metadata.broker.list': config.kafka.hosts,
                    });
                    testProducer.connect();
                    testProducer.on('ready', () => next());
                    testProducer.on('event.error', error => {
                        assert.ifError(error);
                    });
                },
                // create topics by fetching metadata from these topics
                // (works if auto.create.topics.enabled is true)
                next => testProducer.getMetadata({
                    topic: config.extensions.replication.topic,
                    timeout: 10000,
                }, next),
                next => testProducer.getMetadata({
                    topic: config.extensions.replication.replicationStatusTopic,
                    timeout: 10000,
                }, next),
                next => {
                    _doHealthcheckRequest(next);
                    // refresh healthcheck result, as after creating
                    // topics they take some time to appear in the
                    // healthcheck results
                    healthcheckTimer = setInterval(_doHealthcheckRequest,
                                                   2000);
                },
            ], done);
        });

        after(() => {
            clearInterval(healthcheckTimer);
        });

        it('should get a response with data', done => {
            assert.equal(resCode, 200);
            assert(data);
            return done();
        });

        it('should have valid keys', done => {
            assert(data.topics);
            let timer = undefined;
            function _checkValidKeys() {
                const repTopic =
                          data.topics[config.extensions.replication.topic];
                if (!repTopic) {
                    return undefined;
                }
                clearInterval(timer);
                assert(Array.isArray(repTopic.partitions));
                assert(data.internalConnections);
                // NOTE: isrHealth is not checked here because circleci
                // kafka will have one ISR only. Maybe isrHealth should
                // be a test for end-to-end
                assert.strictEqual(
                    data.internalConnections.zookeeper.status, 'ok');
                assert.strictEqual(
                    data.internalConnections.kafkaProducer.status, 'ok');
                return done();
            }
            timer = setInterval(_checkValidKeys, 1000);
        }).timeout(20000);
    });

    describe('metrics routes', function dF() {
        this.timeout(10000);
        const OPS = 'test:bb:ops';
        const BYTES = 'test:bb:bytes';
        const OBJECT_BYTES = 'test:bb:object:bytes';
        const OPS_DONE = 'test:bb:opsdone';
        const OBJECT_BYTES_DONE = 'test:bb:object:bytesdone';
        const OPS_FAIL = 'test:bb:opsfail';
        const BYTES_DONE = 'test:bb:bytesdone';
        const BYTES_FAIL = 'test:bb:bytesfail';
        const OPS_PENDING = 'test:bb:opspending';
        const BYTES_PENDING = 'test:bb:bytespending';
        const BUCKET_NAME = 'test-bucket';
        const OBJECT_KEY = 'test/object-key';
        const VERSION_ID = 'test-version-id';
        const testStartTime = Date.now();

        const destconfig = config.extensions.replication.destination;
        const site1 = destconfig.bootstrapList[0].site;
        const site2 = destconfig.bootstrapList[1].site;
        statsClient.reportNewRequest(`${site1}:${BYTES}`, 2198);
        statsClient.reportNewRequest(`${site1}:${BUCKET_NAME}:` +
            `${OBJECT_KEY}:${VERSION_ID}:${OBJECT_BYTES}`, 100);
        statsClient.reportNewRequest(`${site1}:${OPS_DONE}`, 450);
        statsClient.reportNewRequest(`${site1}:${OPS_FAIL}`, 150);
        statsClient.reportNewRequest(`${site1}:${BYTES_DONE}`, 1027);
        statsClient.reportNewRequest(`${site1}:${BUCKET_NAME}:` +
            `${OBJECT_KEY}:${VERSION_ID}:${OBJECT_BYTES_DONE}`, 50);
        statsClient.reportNewRequest(`${site1}:${BYTES_FAIL}`, 375);

        statsClient.reportNewRequest(`${site2}:${OPS}`, 900);
        statsClient.reportNewRequest(`${site2}:${BYTES}`, 2943);
        statsClient.reportNewRequest(`${site2}:${OPS_DONE}`, 300);
        statsClient.reportNewRequest(`${site2}:${OPS_FAIL}`, 55);
        statsClient.reportNewRequest(`${site2}:${BYTES_DONE}`, 1874);
        statsClient.reportNewRequest(`${site2}:${BYTES_FAIL}`, 575);

        const testVersionId =
            '3938353030303836313334343731393939393939524730303120203';
        const members = [
            `test-bucket:test-key:${testVersionId}0:${site1}`,
            `test-bucket:test-key:${testVersionId}1:${site2}`,
        ];

        before(done =>
            async.parallel([
                next => addMembers(redisClient, site1, members, next),
                next => redisClient.incrby(`${site1}:${OPS_PENDING}`, 2, next),
                next => redisClient.incrby(`${site1}:${BYTES_PENDING}`, 1024,
                    next),
                next => redisClient.incrby(`${site2}:${OPS_PENDING}`, 2, next),
                next => redisClient.incrby(`${site2}:${BYTES_PENDING}`, 1024,
                    next),
                next => {
                    // site1
                    const timestamps = statsClient.getSortedSetHours(
                        testStartTime);
                    async.each(timestamps, (ts, tsCB) =>
                        async.times(10, (n, timeCB) => {
                            const key = `${TEST_REDIS_KEY_FAILED_CRR}:` +
                                `${site1}:${ts}`;
                            redisClient.zadd(key, 10 + n, `test-${n}`, timeCB);
                        }, tsCB), next);
                },
                next => {
                    // site2
                    const timestamps = statsClient.getSortedSetHours(
                        testStartTime);
                    async.each(timestamps, (ts, tsCB) =>
                        async.times(10, (n, timeCB) => {
                            const key = `${TEST_REDIS_KEY_FAILED_CRR}:` +
                                `${site2}:${ts}`;
                            redisClient.zadd(key, 10 + n, `test-${n}`, timeCB);
                        }, tsCB), next);
                },
            ], done));

        after(done => {
            const redis = new Redis();
            redis.flushall(done);
        });

        const metricsPaths = [
            '/_/metrics/crr/all',
            '/_/metrics/crr/all/backlog',
            '/_/metrics/crr/all/completions',
            '/_/metrics/crr/all/failures',
            '/_/metrics/crr/all/throughput',
            '/_/metrics/crr/all/pending',
            `/_/metrics/crr/${site1}/progress/bucket/object?versionId=version`,
            `/_/metrics/crr/${site1}/throughput/bucket/object` +
                '?versionId=version',
        ];
        metricsPaths.forEach(path => {
            it(`should get a 200 response for route: ${path}`, done => {
                const url = getUrl(path);

                http.get(url, res => {
                    assert.equal(res.statusCode, 200);
                    done();
                });
            });

            it(`should get correct data keys for route: ${path}`, done => {
                getRequest(path, (err, res) => {
                    assert.ifError(err);
                    // Object-level throughput route.
                    if (res.description && res.throughput) {
                        assert.deepEqual(Object.keys(res), ['description',
                            'throughput']);
                        assert.equal(typeof res.description, 'string');
                        assert.equal(typeof res.throughput, 'string');
                        return done();
                    }
                    // Object-level progress route.
                    if (res.description && res.progress) {
                        assert.deepEqual(Object.keys(res), ['description',
                            'pending', 'completed', 'progress']);
                        assert.equal(typeof res.description, 'string');
                        assert.equal(typeof res.pending, 'number');
                        assert.equal(typeof res.completed, 'number');
                        assert.equal(typeof res.progress, 'string');
                        return done();
                    }
                    // Site-level metrics routes.
                    const key = Object.keys(res)[0];
                    assert(res[key].description);
                    assert.equal(typeof res[key].description, 'string');
                    if (res[key].results) {
                        assert(res[key].results);
                        assert.deepEqual(Object.keys(res[key].results),
                            ['count', 'size']);
                    }
                    return done();
                });
            });
        });

        const allWrongPaths = [
            // general wrong paths
            '/',
            '/metrics/crr/all',
            '/_/metrics',
            '/_/metrics/backlog',
            // wrong category field
            '/_/m/crr/all',
            '/_/metric/crr/all',
            '/_/metric/crr/all/backlog',
            '/_/metricss/crr/all',
            // wrong extension field
            '/_/metrics/c/all',
            '/_/metrics/c/all/backlog',
            '/_/metrics/crrr/all',
            // wrong site field
            // wrong type field
            '/_/metrics/crr/all/backlo',
            '/_/metrics/crr/all/backlogs',
            '/_/metrics/crr/all/completion',
            '/_/metrics/crr/all/completionss',
            '/_/metrics/crr/all/throughpu',
            '/_/metrics/crr/all/throughputs',
            '/_/metrics/crr/all/pendin',
            '/_/metrics/crr/all/pendings',
            `/_/metrics/crr/${site1}/progresss`,
            // given bucket without object key
            `/_/metrics/crr/${site1}/progress/bucket`,
            `/_/metrics/crr/${site1}/progress/bucket/`,
            `/_/metrics/crr/${site1}/throughput/bucket`,
            `/_/metrics/crr/${site1}/throughput/bucket/`,
            // given bucket without version ID
            `/_/metrics/crr/${site1}/progress/bucket/object`,
            `/_/metrics/crr/${site1}/progress/bucket/object?versionId=`,
        ];
        allWrongPaths.forEach(path => {
            it(`should get a 404 response for route: ${path}`, done => {
                const url = getUrl(path);

                http.get(url, res => {
                    assert.equal(res.statusCode, 404);
                    assert.equal(res.statusMessage, 'Not Found');
                    done();
                });
            });
        });

        it('should return an error for unknown site given', done => {
            getRequest('/_/metrics/crr/wrong-site/completions', err => {
                assert.equal(err.statusCode, 404);
                assert.equal(err.statusMessage, 'Not Found');
                done();
            });
        });

        it('should get the right data for route: ' +
        `/_/metrics/crr/${site1}/backlog`, done => {
            getRequest(`/_/metrics/crr/${site1}/backlog`, (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                // Backlog now uses pending metrics
                assert.equal(res[key].results.count, 2);
                assert.equal(res[key].results.size, 1024);
                done();
            });
        });

        it('should get the right data for route: ' +
        '/_/metrics/crr/all/backlog', done => {
            getRequest('/_/metrics/crr/all/backlog', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                // Backlog now uses pending metrics
                assert.equal(res[key].results.count, 4);
                assert.equal(res[key].results.size, 2048);
                done();
            });
        });

        it('should get the right data for route: ' +
        `/_/metrics/crr/${site1}/completions`, done => {
            getRequest(`/_/metrics/crr/${site1}/completions`, (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                // Completions count = OPS_DONE
                assert.equal(res[key].results.count, 450);
                // Completions bytes = BYTES_DONE
                assert.equal(res[key].results.size, 1027);
                done();
            });
        });

        it('should get the right data for route: ' +
        '/_/metrics/crr/all/completions', done => {
            getRequest('/_/metrics/crr/all/completions', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                // Completions count = OPS_DONE
                assert.equal(res[key].results.count, 750);
                // Completions bytes = BYTES_DONE
                assert.equal(res[key].results.size, 2901);
                done();
            });
        });

        it('should get the right data for route: ' +
        `/_/metrics/crr/${site1}/failures`, done => {
            getRequest(`/_/metrics/crr/${site1}/failures`, (err, res) => {
                assert.ifError(err);

                const testTime = statsClient.getSortedSetCurrentHour(
                    testStartTime);
                const current = statsClient.getSortedSetCurrentHour(Date.now());

                // Need to adjust results if oldest set already expired
                let adjustResult = 0;
                if (current !== testTime) {
                    // single site
                    adjustResult -= 10;
                }

                const key = Object.keys(res)[0];
                // Failures count scans all object fail keys
                assert.equal(res[key].results.count, 242 - adjustResult);
                // Failures bytes is no longer used
                assert.equal(res[key].results.size, 0);
                done();
            });
        });

        it('should get the right data for route: ' +
        '/_/metrics/crr/all/failures', done => {
            getRequest('/_/metrics/crr/all/failures', (err, res) => {
                assert.ifError(err);

                const testTime = statsClient.getSortedSetCurrentHour(
                    testStartTime);
                const current = statsClient.getSortedSetCurrentHour(Date.now());

                // Need to adjust results if oldest set already expired
                let adjustResult = 0;
                if (current !== testTime) {
                    // both sites
                    adjustResult -= 20;
                }

                const key = Object.keys(res)[0];
                // Failures count scans all object fail keys
                assert.equal(res[key].results.count, 482 - adjustResult);
                // Failures bytes is no longer used
                assert.equal(res[key].results.size, 0);
                done();
            });
        });

        it('should get the right data for route: ' +
        `/_/metrics/crr/${site1}/throughput`, done => {
            getRequest(`/_/metrics/crr/${site1}/throughput`, (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                // Throughput count = OPS_DONE / EXPIRY
                assert.equal(res[key].results.count, 0.5);
                // Throughput bytes = BYTES_DONE / EXPIRY
                assert.equal(res[key].results.size, 1.14);
                done();
            });
        });

        it('should get the right data for route: ' +
        '/_/metrics/crr/all/throughput', done => {
            getRequest('/_/metrics/crr/all/throughput', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                // Throughput count = OPS_DONE / EXPIRY
                assert.equal(res[key].results.count, 0.83);
                // Throughput bytes = BYTES_DONE / EXPIRY
                assert.equal(res[key].results.size, 3.22);
                done();
            });
        });

        it('should get the right data for route: ' +
        `/_/metrics/crr/${site1}/pending`, done => {
            getRequest(`/_/metrics/crr/${site1}/pending`, (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                assert.equal(res[key].results.count, 2);
                assert.equal(res[key].results.size, 1024);
                done();
            });
        });

        it('should get the right data for route: ' +
        '/_/metrics/crr/all/pending', done => {
            getRequest('/_/metrics/crr/all/pending', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                assert.equal(res[key].results.count, 4);
                assert.equal(res[key].results.size, 2048);
                done();
            });
        });

        it('should return all metrics for route: ' +
        `/_/metrics/crr/${site1}`, done => {
            getRequest(`/_/metrics/crr/${site1}`, (err, res) => {
                assert.ifError(err);
                const keys = Object.keys(res);
                assert(keys.includes('backlog'));
                assert(keys.includes('completions'));
                assert(keys.includes('throughput'));
                assert(keys.includes('failures'));
                assert(keys.includes('pending'));

                const testTime = statsClient.getSortedSetCurrentHour(
                    testStartTime);
                const current = statsClient.getSortedSetCurrentHour(Date.now());

                // Need to adjust results if oldest set already expired
                let adjustResult = 0;
                if (current !== testTime) {
                    // single site
                    adjustResult -= 10;
                }

                // backlog matches pending
                assert(res.backlog.description);
                assert.equal(res.backlog.results.count, 2);
                assert.equal(res.backlog.results.size, 1024);

                assert(res.completions.description);
                // Completions count = OPS_DONE
                assert.equal(res.completions.results.count, 450);
                // Completions bytes = BYTES_DONE
                assert.equal(res.completions.results.size, 1027);

                assert(res.throughput.description);
                // Throughput count = OPS_DONE / EXPIRY
                assert.equal(res.throughput.results.count, 0.5);
                // Throughput bytes = BYTES_DONE / EXPIRY
                assert.equal(res.throughput.results.size, 1.14);

                assert(res.failures.description);
                // Failures count scans all object fail keys
                assert.equal(res.failures.results.count, 242 - adjustResult);
                // Failures bytes is no longer used
                assert.equal(res.failures.results.size, 0);

                assert(res.pending.description);
                assert.equal(res.pending.results.count, 2);
                assert.equal(res.pending.results.size, 1024);

                done();
            });
        });

        it('should return all metrics for route: ' +
        '/_/metrics/crr/all', done => {
            getRequest('/_/metrics/crr/all', (err, res) => {
                assert.ifError(err);
                const keys = Object.keys(res);
                assert(keys.includes('backlog'));
                assert(keys.includes('completions'));
                assert(keys.includes('throughput'));
                assert(keys.includes('failures'));
                assert(keys.includes('pending'));

                const testTime = statsClient.getSortedSetCurrentHour(
                    testStartTime);
                const current = statsClient.getSortedSetCurrentHour(Date.now());

                // Need to adjust results if oldest set already expired
                let adjustResult = 0;
                if (current !== testTime) {
                    // both sites
                    adjustResult -= 20;
                }

                // backlog matches pending
                assert(res.backlog.description);
                assert.equal(res.backlog.results.count, 4);
                assert.equal(res.backlog.results.size, 2048);

                assert(res.completions.description);
                // Completions count = OPS_DONE
                assert.equal(res.completions.results.count, 750);
                // Completions bytes = BYTES_DONE
                assert.equal(res.completions.results.size, 2901);

                assert(res.throughput.description);
                // Throughput count = OPS_DONE / EXPIRY
                assert.equal(res.throughput.results.count, 0.83);
                // Throughput bytes = BYTES_DONE / EXPIRY
                assert.equal(res.throughput.results.size, 3.22);

                assert(res.failures.description);
                // Failures count scans all object fail keys
                assert.equal(res.failures.results.count, 482 - adjustResult);
                // Failures bytes is no longer used
                assert.equal(res.failures.results.size, 0);

                assert(res.pending.description);
                assert.equal(res.pending.results.count, 4);
                assert.equal(res.pending.results.size, 2048);

                done();
            });
        });

        it(`should return all metrics for route: /_/metrics/crr/${site1}` +
            `/progress/${BUCKET_NAME}/${OBJECT_KEY}?versionId=${VERSION_ID}`,
            done =>
            getRequest(`/_/metrics/crr/${site1}/progress/${BUCKET_NAME}/` +
                `${OBJECT_KEY}?versionId=${VERSION_ID}`, (err, res) => {
                assert.ifError(err);
                assert(res.description);
                assert.strictEqual(res.pending, 50);
                assert.strictEqual(res.completed, 50);
                assert.strictEqual(res.progress, '50%');
                done();
            }));

        it(`should return all metrics for route: /_/metrics/crr/${site1}` +
            `/throughput/${BUCKET_NAME}/${OBJECT_KEY}?versionId=${VERSION_ID}`,
            done =>
            getRequest(`/_/metrics/crr/${site1}/throughput/${BUCKET_NAME}/` +
                `${OBJECT_KEY}?versionId=${VERSION_ID}`, (err, res) => {
                assert.ifError(err);
                assert(res.description);
                assert.strictEqual(res.throughput, '0.06');
                done();
            }));

        describe('No metrics data in Redis', () => {
            before(done => {
                redis.keys('*:test:bb:*').then(keys => {
                    const pipeline = redis.pipeline();
                    keys.forEach(key => {
                        pipeline.del(key);
                    });
                    pipeline.exec(done);
                });
            });

            it('should return a response even if redis data does not exist: ' +
            'all CRR metrics', done => {
                getRequest('/_/metrics/crr/all', (err, res) => {
                    assert.ifError(err);

                    const keys = Object.keys(res);
                    assert(keys.includes('backlog'));
                    assert(keys.includes('completions'));
                    assert(keys.includes('throughput'));
                    assert(keys.includes('failures'));
                    assert(keys.includes('pending'));

                    assert(res.backlog.description);
                    assert.equal(res.backlog.results.count, 0);
                    assert.equal(res.backlog.results.size, 0);

                    assert(res.completions.description);
                    assert.equal(res.completions.results.count, 0);
                    assert.equal(res.completions.results.size, 0);

                    assert(res.throughput.description);
                    assert.equal(res.throughput.results.count, 0);
                    assert.equal(res.throughput.results.size, 0);

                    assert(res.failures.description);
                    // Failures are based on object metrics
                    assert.equal(typeof res.failures.results.count, 'number');
                    assert.equal(typeof res.failures.results.size, 'number');

                    assert(res.pending.description);
                    assert.equal(res.pending.results.count, 0);
                    assert.equal(res.pending.results.size, 0);
                    done();
                });
            });

            it('should return a response even if redis data does not exist: ' +
            'object progress CRR metrics', done =>
                getRequest(`/_/metrics/crr/${site1}/progress/bucket/object` +
                    '?versionId=version', (err, res) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(res, {
                        description: 'Number of bytes to be replicated ' +
                            '(pending), number of bytes transferred to the ' +
                            'destination (completed), and percentage of the ' +
                            'object that has completed replication (progress)',
                        pending: 0,
                        completed: 0,
                        progress: '0%',
                    });
                    done();
                }));

            it('should return a response even if redis data does not exist: ' +
            'object throughput CRR metrics', done =>
                getRequest(`/_/metrics/crr/${site1}/throughput/bucket/object` +
                    '?versionId=version', (err, res) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(res, {
                        description: 'Current throughput for object ' +
                            'replication in bytes/sec (throughput)',
                        throughput: '0.00',
                    });
                    done();
                }));
        });
    });

    describe('CRR Pause/Resume service routes', () => {
        let redis1;
        let redis2;
        let cache1 = [];
        let cache2 = [];
        let channel1;
        let channel2;

        let zkClient;

        const emptyBody = '';
        const crrConfigs = config.extensions.replication;
        const crrTopic = crrConfigs.topic;
        const destconfig = crrConfigs.destination;

        const firstSite = destconfig.bootstrapList[0].site;
        const secondSite = destconfig.bootstrapList[1].site;

        const futureDate = new Date();
        futureDate.setHours(futureDate.getHours() + 5);

        function setupZkClient(cb) {
            const { connectionString } = config.zookeeper;
            zkClient = zookeeper.createClient(connectionString);
            zkClient.connect();
            zkClient.once('connected', () => {
                async.series([
                    next => zkClient.mkdirp(ZK_TEST_CRR_STATE_PATH, err => {
                        if (err && err.name !== 'NODE_EXISTS') {
                            return next(err);
                        }
                        return next();
                    }),
                    next => {
                        // emulate first site to be active (not paused)
                        const path = `${ZK_TEST_CRR_STATE_PATH}/${firstSite}`;
                        const data =
                            Buffer.from(JSON.stringify({ paused: false }));
                        zkClient.create(path, data, EPHEMERAL_NODE, next);
                    },
                    next => {
                        // emulate second site to be paused
                        const path = `${ZK_TEST_CRR_STATE_PATH}/${secondSite}`;
                        const data = Buffer.from(JSON.stringify({
                            paused: true,
                            scheduledResume: futureDate.toString(),
                        }));
                        zkClient.create(path, data, EPHEMERAL_NODE, next);
                    },
                ], err => {
                    if (err) {
                        process.stdout.write('error occurred in zookeeper ' +
                        'setup for CRR pause/resume');
                        return cb(err);
                    }
                    return cb();
                });
            });
        }

        before(done => {
            redis1 = new Redis();
            redis2 = new Redis();

            channel1 = `${crrTopic}-${firstSite}`;
            redis1.subscribe(channel1, err => assert.ifError(err));
            redis1.on('message', (channel, message) => {
                cache1.push({ channel, message });
            });

            channel2 = `${crrTopic}-${secondSite}`;
            redis2.subscribe(channel2, err => assert.ifError(err));
            redis2.on('message', (channel, message) => {
                cache2.push({ channel, message });
            });

            setupZkClient(done);
        });

        afterEach(() => {
            cache1 = [];
            cache2 = [];
        });

        after(() => {
            if (zkClient) {
                zkClient.close();
                zkClient = null;
            }
        });

        const validRequests = [
            { path: '/_/crr/pause', method: 'POST' },
            { path: '/_/crr/resume', method: 'POST' },
            { path: '/_/crr/resume/all', method: 'POST' },
            { path: `/_/crr/resume/${firstSite}`, method: 'POST' },
            { path: '/_/crr/status', method: 'GET' },
            { path: `/_/crr/status/${firstSite}`, method: 'GET' },
            { path: '/_/crr/resume/all', method: 'GET' },
        ];
        validRequests.forEach(entry => {
            it(`should get a 200 response for route: ${entry.path}`, done => {
                const options = Object.assign({}, defaultOptions, {
                    method: entry.method,
                    path: entry.path,
                });
                const req = http.request(options, res => {
                    assert.equal(res.statusCode, 200);
                    done();
                });
                req.end();
            });
        });

        const invalidRequests = [
            { path: '/_/crr/pause/invalid-site', method: 'POST' },
            { path: '/_/crr/resume/invalid-site', method: 'POST' },
            { path: '/_/crr/status', method: 'POST' },
            { path: '/_/crr/status/invalid-site', method: 'GET' },
        ];
        invalidRequests.forEach(entry => {
            it(`should get a 404 response for route: ${entry.path}`, done => {
                const options = Object.assign({}, defaultOptions, {
                    method: entry.method,
                    path: entry.path,
                });
                const req = http.request(options, res => {
                    assert.equal(res.statusCode, 404);
                    assert.equal(res.statusMessage, 'Not Found');
                    assert.equal(cache1.length, 0);
                    assert.equal(cache2.length, 0);
                    done();
                });
                req.end();
            });
        });

        it('should receive a pause request on all site channels from route ' +
        '/_/crr/pause', done => {
            const options = Object.assign({}, defaultOptions, {
                method: 'POST',
                path: '/_/crr/pause',
            });
            makeRequest(options, emptyBody, err => {
                assert.ifError(err);

                setTimeout(() => {
                    assert.strictEqual(cache1.length, 1);
                    assert.strictEqual(cache2.length, 1);

                    assert.deepStrictEqual(cache1[0].channel, channel1);
                    assert.deepStrictEqual(cache2[0].channel, channel2);

                    const message1 = JSON.parse(cache1[0].message);
                    const message2 = JSON.parse(cache2[0].message);
                    const expected = { action: 'pauseService' };
                    assert.deepStrictEqual(message1, expected);
                    assert.deepStrictEqual(message2, expected);
                    done();
                }, 1000);
            });
        });

        it('should receive a pause request on specified site from route ' +
        `/_/crr/pause/${firstSite}`, done => {
            const options = Object.assign({}, defaultOptions, {
                method: 'POST',
                path: `/_/crr/pause/${firstSite}`,
            });
            makeRequest(options, emptyBody, err => {
                assert.ifError(err);

                setTimeout(() => {
                    assert.strictEqual(cache1.length, 1);
                    assert.strictEqual(cache2.length, 0);

                    assert.deepStrictEqual(cache1[0].channel, channel1);

                    const message = JSON.parse(cache1[0].message);
                    const expected = { action: 'pauseService' };
                    assert.deepStrictEqual(message, expected);
                    done();
                }, 1000);
            });
        });

        it('should receive a resume request on all site channels from route ' +
        '/_/crr/resume', done => {
            const options = Object.assign({}, defaultOptions, {
                method: 'POST',
                path: '/_/crr/resume',
            });
            makeRequest(options, emptyBody, err => {
                assert.ifError(err);

                setTimeout(() => {
                    assert.strictEqual(cache1.length, 1);
                    assert.strictEqual(cache2.length, 1);
                    assert.deepStrictEqual(cache1[0].channel, channel1);
                    assert.deepStrictEqual(cache2[0].channel, channel2);

                    const message1 = JSON.parse(cache1[0].message);
                    const message2 = JSON.parse(cache2[0].message);
                    const expected = { action: 'resumeService' };
                    assert.deepStrictEqual(message1, expected);
                    assert.deepStrictEqual(message2, expected);
                    done();
                }, 1000);
            });
        });

        it('should get scheduled resume jobs for all sites using route ' +
        '/_/crr/resume/all/schedule', done => {
            getRequest('/_/crr/resume/all/schedule', (err, res) => {
                assert.ifError(err);
                const expected = {
                    'test-site-1': 'none',
                    'test-site-2': futureDate.toString(),
                };
                assert.deepStrictEqual(expected, res);
                done();
            });
        });

        it('should receive a scheduled resume request with specified hours ' +
        `from route /_/crr/resume/${firstSite}/schedule`, done => {
            const options = Object.assign({}, defaultOptions, {
                method: 'POST',
                path: `/_/crr/resume/${firstSite}/schedule`,
            });
            const body = JSON.stringify({ hours: 1 });
            makeRequest(options, body, (err, res) => {
                assert.ifError(err);
                setTimeout(() => {
                    getResponseBody(res, err => {
                        assert.ifError(err);

                        assert.strictEqual(cache1.length, 1);
                        assert.deepStrictEqual(cache1[0].channel, channel1);
                        const message = JSON.parse(cache1[0].message);
                        assert.equal('resumeService', message.action);
                        const date = new Date();
                        const scheduleDate = new Date(message.date);
                        assert(scheduleDate > date);
                        // make sure the scheduled time does not exceed expected
                        const millisecondPerHour = 60 * 60 * 1000;
                        assert(scheduleDate - date <= millisecondPerHour);
                        done();
                    });
                }, 1000);
            });
        });

        it('should receive a scheduled resume request without specified ' +
        `hours from route /_/crr/resume/${firstSite}/schedule`, done => {
            const options = Object.assign({}, defaultOptions, {
                method: 'POST',
                path: `/_/crr/resume/${firstSite}/schedule`,
            });
            makeRequest(options, emptyBody, err => {
                assert.ifError(err);

                setTimeout(() => {
                    assert.strictEqual(cache1.length, 1);
                    assert.deepStrictEqual(cache1[0].channel, channel1);

                    const message = JSON.parse(cache1[0].message);
                    assert.equal('resumeService', message.action);

                    const date = new Date();
                    const scheduleDate = new Date(message.date);
                    assert(scheduleDate > date);

                    // make sure scheduled time between 5 and 6 hours from now
                    const millisecondPerHour = 60 * 60 * 1000;
                    assert((scheduleDate - date <= millisecondPerHour * 6) &&
                           (scheduleDate - date) >= millisecondPerHour * 5);
                    done();
                }, 1000);
            });
        });

        it('should remove a scheduled resume request when receiving a DELETE ' +
        `request to route /_/crr/resume/${secondSite}/schedule`, done => {
            const options = Object.assign({}, defaultOptions, {
                method: 'DELETE',
                path: `/_/crr/resume/${secondSite}/schedule`,
            });
            makeRequest(options, emptyBody, (err, res) => {
                assert.ifError(err);

                setTimeout(() => {
                    getResponseBody(res, err => {
                        assert.ifError(err);

                        assert.strictEqual(cache2.length, 1);

                        const message = JSON.parse(cache2[0].message);
                        assert.equal('deleteScheduledResumeService',
                            message.action);
                        done();
                    });
                });
            });
        });

        it('should receive a status request on all site channels from route ' +
        '/_/crr/status', done => {
            getRequest('/_/crr/status', (err, res) => {
                assert.ifError(err);
                const expected = {
                    'test-site-1': 'enabled',
                    'test-site-2': 'disabled',
                };
                assert.deepStrictEqual(expected, res);
                done();
            });
        });
    });
});
