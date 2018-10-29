const assert = require('assert');
const async = require('async');
const http = require('http');
const Redis = require('ioredis');
const { Producer } = require('node-rdkafka');

const { RedisClient, StatsModel } = require('arsenal').metrics;

const config = require('../../config.json');
const { makePOSTRequest, getResponseBody } =
    require('../utils/makePOSTRequest');
const S3Mock = require('../utils/S3Mock');
const VaultMock = require('../utils/VaultMock');
const redisConfig = {
    host: config.redis.host,
    port: config.redis.port,
};
const TEST_REDIS_KEY_FAILED_CRR = 'test:bb:crr:failed';

const fakeLogger = {
    trace: () => {},
    error: () => {},
    info: () => {},
    debug: () => {},
};

const defaultOptions = {
    host: config.server.host,
    port: config.server.port,
    method: 'GET',
};

function getUrl(options, path) {
    return `http://${options.host}:${options.port}${path}`;
}

function addMembers(redisClient, site, members, cb) {
    const statsClient = new StatsModel(redisClient);
    const epoch = Date.now();
    const twelveHoursAgo = epoch - (60 * 60 * 1000) * 12;
    let score = statsClient.normalizeTimestampByHour(new Date(twelveHoursAgo));
    const key = `${TEST_REDIS_KEY_FAILED_CRR}:${site}:${score}`;
    const cmds = members.map(member => (['zadd', key, ++score, member]));
    redisClient.batch(cmds, cb);
}

function addManyMembers(redisClient, site, members, normalizedScore, score,
    cb) {
    let givenScore = score;
    const key = `${TEST_REDIS_KEY_FAILED_CRR}:${site}:${normalizedScore}`;
    const cmds = members.map(member => (['zadd', key, ++givenScore, member]));
    redisClient.batch(cmds, cb);
}

function deleteKeys(redisClient, cb) {
    redisClient.batch([['flushall']], cb);
}

function makeRetryPOSTRequest(body, cb) {
    const options = Object.assign({}, defaultOptions, {
        method: 'POST',
        path: '/_/crr/failed',
    });
    makePOSTRequest(options, body, cb);
}

function getRequest(path, done) {
    const params = {
        host: '127.0.0.1',
        port: 8900,
        method: 'GET',
        path,
    };
    // eslint-disable-next-line
    const req = http.request(params, res => {
        if (res.statusCode !== 200) {
            return done(res);
        }

        const chunks = [];
        res.on('data', chunk => {
            chunks.push(chunk);
        });
        res.on('end', () => {
            let body;
            try {
                body = JSON.parse(Buffer.concat(chunks).toString());
            } catch (e) {
                return done(e);
            }
            return done(null, body);
        });
    });
    req.on('error', err => done(err));
    req.end();
}

describe('Backbeat Server', () => {
    let s3Mock;
    let vaultMock;
    let redisClient;

    before(() => {
        s3Mock = new S3Mock();
        vaultMock = new VaultMock();
        http.createServer((req, res) => s3Mock.onRequest(req, res))
            .listen(config.extensions.replication.source.s3.port);
        http.createServer((req, res) => vaultMock.onRequest(req, res))
            .listen(config.extensions.replication.source.auth.vault.port);
        redisClient = new RedisClient(redisConfig, fakeLogger);
    });

    it('should get a 404 route not found error response', () => {
        const url = getUrl(defaultOptions, '/_/invalidpath');

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
            const url = getUrl(defaultOptions, '/_/healthcheck');

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

    describe.only('API routes', function dF() {
        this.timeout(10000);
        const interval = 300;
        const expiry = 900;
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

        // Needed in-case failures set expires during tests
        const testStartTime = Date.now();

        const destconfig = config.extensions.replication.destination;
        const site1 = destconfig.bootstrapList[0].site;
        const site2 = destconfig.bootstrapList[1].site;

        let statsClient;
        let redis;

        before(done => {
            redis = new Redis();
            statsClient = new StatsModel(redisClient, interval, expiry);

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

            return async.parallel([
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
            ], done);
        });

        after(() => {
            redis.keys('*test:bb:*').then(keys => {
                const pipeline = redis.pipeline();
                keys.forEach(key => {
                    pipeline.del(key);
                });
                return pipeline.exec();
            });
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
                const url = getUrl(defaultOptions, path);

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
                const url = getUrl(defaultOptions, path);

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
                assert.equal(res[key].results.count, 240 - adjustResult);
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
                assert.equal(res[key].results.count, 480 - adjustResult);
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
                // Throughput bytes = (BYTES_DONE / 1000) / EXPIRY
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
                assert.equal(res.failures.results.count, 240 - adjustResult);
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
                // Completions bytes = BYTES_DONE / 1000
                assert.equal(res.completions.results.size, 2901);

                assert(res.throughput.description);
                // Throughput count = OPS_DONE / EXPIRY
                assert.equal(res.throughput.results.count, 0.83);
                // Throughput bytes = (BYTES_DONE / 1000) / EXPIRY
                assert.equal(res.throughput.results.size, 3.22);

                assert(res.failures.description);
                // Failures count scans all object fail keys
                assert.equal(res.failures.results.count, 480 - adjustResult);
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
            before(() => {
                redis.keys('*:test:bb:*').then(keys => {
                    const pipeline = redis.pipeline();
                    keys.forEach(key => {
                        pipeline.del(key);
                    });
                    return pipeline.exec();
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

    describe('CRR Retry routes', () => {
        const retryPaths = [
            '/_/crr/failed',
            '/_/crr/failed/test-bucket/test-key?versionId=test-versionId',
        ];
        retryPaths.forEach(path => {
            it(`should get a 200 response for route: GET ${path}`, done => {
                const url = getUrl(defaultOptions, path);

                http.get(url, res => {
                    assert.equal(res.statusCode, 200);
                    done();
                });
            });
        });

        const retryQueryPaths = [
            '/_/crr/failed?marker=foo',
            '/_/crr/failed?marker=',
            '/_/crr/failed?sitename=',
        ];
        retryQueryPaths.forEach(path => {
            it(`should get a 400 response for route: ${path}`, done => {
                const url = getUrl(defaultOptions, path);

                http.get(url, res => {
                    assert.equal(res.statusCode, 400);
                    done();
                });
            });
        });

        it('should get a 200 response for route: POST /_/crr/failed', done => {
            const member = ['test-bucket:test-key:test-versionId'];
            addMembers(redisClient, 'test-site', member, err => {
                assert.ifError(err);
                const body = JSON.stringify([{
                    Bucket: 'test-bucket',
                    Key: 'test-key',
                    VersionId: 'test-versionId',
                    StorageClass: 'test-site',
                }]);
                return makeRetryPOSTRequest(body, (err, res) => {
                    assert.ifError(err);
                    assert.strictEqual(res.statusCode, 200);
                    done();
                });
            });
        }).timeout(10000);

        const invalidPOSTRequestBodies = [
            'a',
            {},
            [],
            ['a'],
            [{
                // Missing Bucket property.
                Key: 'b',
                VersionId: 'c',
                StorageClass: 'd',
            }],
            [{
                // Missing Key property.
                Bucket: 'a',
                VersionId: 'c',
                StorageClass: 'd',
            }],
            [{
                // Missing VersionId property.
                Bucket: 'a',
                Key: 'b',
                StorageClass: 'd',
            }],
            [{
                // Missing StorageClass property.
                Bucket: 'a',
                Key: 'b',
                VersionId: 'c',
            }],
        ];
        invalidPOSTRequestBodies.forEach(body => {
            const invalidBody = JSON.stringify(body);
            it('should get a 400 response for route: /_/crr/failed when ' +
            `given an invalid request body: ${invalidBody}`, done => {
                makeRetryPOSTRequest(invalidBody, (err, res) => {
                    assert.strictEqual(res.statusCode, 400);
                    return getResponseBody(res, (err, resBody) => {
                        assert.ifError(err);
                        const body = JSON.parse(resBody);
                        assert(body.MalformedPOSTRequest);
                        return done();
                    });
                });
            });
        });

        describe('Retry feature with Redis', () => {
            // Version ID calculated from the mock object MD.
            const testVersionId =
                '39383530303038363133343437313939393939395247303031202030';

            before(done => deleteKeys(redisClient, done));

            afterEach(done => deleteKeys(redisClient, done));

            it('should get correct data for GET route: /_/crr/failed when no ' +
            'key has been created', done => {
                getRequest('/_/crr/failed', (err, res) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(res, {
                        IsTruncated: false,
                        Versions: [],
                    });
                    done();
                });
            });

            it('should return empty array for route: /_/crr/failed when ' +
            'no sitename is given', done => {
                const member = `test-bucket-1:test-key:${testVersionId}`;
                addMembers(redisClient, 'test-site', [member], err => {
                    assert.ifError(err);
                    getRequest('/_/crr/failed', (err, res) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(res, {
                            IsTruncated: false,
                            Versions: [],
                        });
                        done();
                    });
                });
            });

            it('should get correct data for GET route: /_/crr/failed when ' +
            'the key has been created and there is one key', done => {
                const member = `test-bucket-1:test-key:${testVersionId}`;
                addMembers(redisClient, 'test-site', [member], err => {
                    assert.ifError(err);
                    getRequest('/_/crr/failed?marker=0&sitename=test-site',
                    (err, res) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(res, {
                            IsTruncated: false,
                            Versions: [{
                                Bucket: 'test-bucket-1',
                                Key: 'test-key',
                                VersionId: testVersionId,
                                StorageClass: 'test-site',
                                Size: 1,
                                LastModified: '2018-03-30T22:22:34.384Z',
                            }],
                        });
                        done();
                    });
                });
            });

            it('should get correct data for GET route: /_/crr/failed when ' +
            'unknown marker is given',
            done => {
                const site = 'test-site';
                const member = `test-bucket-1:test-key:${testVersionId}`;
                addMembers(redisClient, site, [member], err => {
                    assert.ifError(err);
                    const endpoint =
                        `/_/crr/failed?marker=999999999999999&sitename=${site}`;
                    return getRequest(endpoint, (err, res) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(res, {
                            IsTruncated: false,
                            Versions: [],
                        });
                        done();
                    });
                });
            });

            it('should get correct data for GET route: /_/crr/failed and ' +
            'normalize members when there are duplicate failures across hours',
            done => {
                const hour = 60 * 60 * 1000;
                const oneHoursAgo = new Date(Date.now() - hour);
                const twoHoursAgo = new Date(Date.now() - (hour * 2));
                const statsClient = new StatsModel(redisClient);
                const norm1 = statsClient.normalizeTimestampByHour(oneHoursAgo);
                const norm2 = statsClient.normalizeTimestampByHour(twoHoursAgo);
                const site = 'test-site';
                const bucket = 'test-bucket';
                const objectKey = 'test-key';
                const member = `${bucket}:${objectKey}:${testVersionId}`;
                return async.series([
                    next => addManyMembers(redisClient, site, [member],
                        norm1, norm1 + 1, next),
                    next => addManyMembers(redisClient, site, [member],
                        norm2, norm2 + 2, next),
                    next =>
                        getRequest(`/_/crr/failed?marker=0&sitename=${site}`,
                            (err, res) => {
                                assert.ifError(err);
                                assert.deepStrictEqual(res, {
                                    IsTruncated: false,
                                    Versions: [{
                                        Bucket: bucket,
                                        Key: objectKey,
                                        VersionId: testVersionId,
                                        StorageClass: site,
                                        Size: 1,
                                        LastModified:
                                            '2018-03-30T22:22:34.384Z',
                                    }],
                                });
                                return next();
                            }),
                    next => async.map([
                        `${TEST_REDIS_KEY_FAILED_CRR}:${site}:${norm1}`,
                        `${TEST_REDIS_KEY_FAILED_CRR}:${site}:${norm2}`,
                    ],
                    (key, cb) => redisClient.zcard(key, cb),
                    (err, results) => {
                        assert.ifError(err);
                        const sum = results[0] + results[1];
                        assert.strictEqual(sum, 1);
                        return next();
                    }),
                ], done);
            });

            it('should get correct data for GET route: /_/crr/failed when ' +
            'the key has been created and there are multiple members',
            done => {
                const members = [
                    'test-bucket:test-key:test-versionId',
                    'test-bucket-1:test-key-1:test-versionId-1',
                    'test-bucket-2:test-key-2:test-versionId-2',
                ];
                addMembers(redisClient, 'test-site', members, err => {
                    assert.ifError(err);
                    getRequest('/_/crr/failed?marker=0&sitename=test-site',
                    (err, res) => {
                        assert.ifError(err);
                        assert.strictEqual(res.IsTruncated, false);
                        assert.strictEqual(res.Versions.length, 3);
                        // We cannot guarantee order because it depends on how
                        // Redis fetches the keys.
                        members.forEach(member => {
                            // eslint-disable-next-line no-unused-vars
                            const [bucket, key, versionId] =
                                member.split(':');
                            assert(res.Versions.some(o => (
                                o.Bucket === bucket &&
                                o.Key === key &&
                                o.VersionId === testVersionId &&
                                o.StorageClass === 'test-site'
                            )));
                        });
                        done();
                    });
                });
            });

            it('should get correct data for GET route: /_/crr/failed when ' +
            'the two site keys have been created',
            done => {
                const members = [
                    'test-bucket:test-key:test-versionId',
                    'test-bucket-1:test-key-1:test-versionId-1',
                    'test-bucket-2:test-key-2:test-versionId-2',
                ];
                async.series([
                    next => addMembers(redisClient, 'test-site', members, next),
                    next => addMembers(redisClient, 'test-site-1', members,
                        next),
                    next => getRequest('/_/crr/failed?sitename=test-site',
                        (err, res) => {
                            assert.ifError(err);
                            assert.strictEqual(res.IsTruncated, false);
                            assert.strictEqual(res.Versions.length, 3);
                            members.forEach(member => {
                                // eslint-disable-next-line no-unused-vars
                                const [bucket, key, versionId] =
                                    member.split(':');
                                assert(res.Versions.some(o => (
                                    o.Bucket === bucket &&
                                    o.Key === key &&
                                    o.VersionId === testVersionId &&
                                    o.StorageClass === 'test-site'
                                )));
                            });
                            next();
                        }),
                    next => getRequest('/_/crr/failed?sitename=test-site-1',
                        (err, res) => {
                            assert.ifError(err);
                            assert.strictEqual(res.IsTruncated, false);
                            assert.strictEqual(res.Versions.length, 3);
                            members.forEach(member => {
                                // eslint-disable-next-line no-unused-vars
                                const [bucket, key, versionId] =
                                    member.split(':');
                                assert(res.Versions.some(o => (
                                    o.Bucket === bucket &&
                                    o.Key === key &&
                                    o.VersionId === testVersionId &&
                                    o.StorageClass === 'test-site-1'
                                )));
                            });
                            next();
                        }),
                ], done);
            });

            it('should get correct data at scale for GET route: ' +
            '/_/crr/failed when failures occur across hours',
            function f(done) {
                this.timeout(30000);
                const hours = Array.from(Array(24).keys());
                async.eachLimit(hours, 10, (hour, callback) => {
                    const delta = (60 * 60 * 1000) * hour;
                    let epoch = Date.now() - delta;
                    return async.timesLimit(150, 10, (i, next) => {
                        const members = [
                            `bucket-${i}:key-a-${i}-${hour}:versionId-${i}`,
                            `bucket-${i}:key-b-${i}-${hour}:versionId-${i}`,
                            `bucket-${i}:key-c-${i}-${hour}:versionId-${i}`,
                            `bucket-${i}:key-d-${i}-${hour}:versionId-${i}`,
                            `bucket-${i}:key-e-${i}-${hour}:versionId-${i}`,
                        ];
                        const statsClient = new StatsModel(redisClient);
                        const normalizedScore = statsClient
                            .normalizeTimestampByHour(new Date(epoch));
                        epoch += 5;
                        return addManyMembers(redisClient, 'test-site', members,
                            normalizedScore, epoch, next);
                    }, callback);
                }, err => {
                    assert.ifError(err);
                    const memberCount = (150 * 5) * 24;
                    const set = new Set();
                    let marker = 0;
                    async.timesSeries(memberCount, (i, next) =>
                        getRequest('/_/crr/failed?' +
                            `marker=${marker}&sitename=test-site`,
                            (err, res) => {
                                assert.ifError(err);
                                res.Versions.forEach(version => {
                                    // Ensure we have no duplicate results.
                                    assert(!set.has(version.Key));
                                    set.add(version.Key);
                                });
                                if (res.IsTruncated === false) {
                                    assert.strictEqual(res.NextMarker,
                                        undefined);
                                    assert.strictEqual(set.size, memberCount);
                                    return done();
                                }
                                assert.strictEqual(res.IsTruncated, true);
                                assert.strictEqual(
                                    typeof(res.NextMarker), 'number');
                                marker = res.NextMarker;
                                return next();
                            }), done);
                });
            });

            it('should get correct data at scale for GET route: ' +
            '/_/crr/failed when failures occur in the same hour',
            function f(done) {
                this.timeout(30000);
                const statsClient = new StatsModel(redisClient);
                const epoch = Date.now();
                let twelveHoursAgo = epoch - (60 * 60 * 1000) * 12;
                const normalizedScore = statsClient
                    .normalizeTimestampByHour(new Date(twelveHoursAgo));
                return async.timesLimit(2000, 10, (i, next) => {
                    const members = [
                        `bucket-${i}:key-a-${i}:versionId-${i}`,
                        `bucket-${i}:key-b-${i}:versionId-${i}`,
                        `bucket-${i}:key-c-${i}:versionId-${i}`,
                        `bucket-${i}:key-d-${i}:versionId-${i}`,
                        `bucket-${i}:key-e-${i}:versionId-${i}`,
                    ];
                    twelveHoursAgo += 5;
                    return addManyMembers(redisClient, 'test-site', members,
                        normalizedScore, twelveHoursAgo, next);
                }, err => {
                    assert.ifError(err);
                    const memberCount = 2000 * 5;
                    const set = new Set();
                    let marker = 0;
                    async.timesSeries(memberCount, (i, next) =>
                        getRequest('/_/crr/failed?' +
                            `marker=${marker}&sitename=test-site`,
                            (err, res) => {
                                assert.ifError(err);
                                res.Versions.forEach(version => {
                                    // Ensure we have no duplicate results.
                                    assert(!set.has(version.Key));
                                    set.add(version.Key);
                                });
                                if (res.IsTruncated === false) {
                                    assert.strictEqual(res.NextMarker,
                                        undefined);
                                    assert.strictEqual(set.size, memberCount);
                                    return done();
                                }
                                assert.strictEqual(res.IsTruncated, true);
                                assert.strictEqual(
                                    typeof(res.NextMarker), 'number');
                                marker = res.NextMarker;
                                return next();
                            }), done);
                });
            });

            it('should get correct data for GET route: ' +
            '/_/crr/failed/<bucket>/<key>?versionId=<versionId> when there ' +
            'is no key',
            done => {
                getRequest('/_/crr/failed/test-bucket/test-key?' +
                    'versionId=test-versionId',
                (err, res) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(res, {
                        IsTruncated: false,
                        Versions: [],
                    });
                    done();
                });
            });

            it('should get correct data for GET route: ' +
            '/_/crr/failed/<bucket>/<key>?versionId=<versionId>', done => {
                const member = 'test-bucket:test-key/a:test-versionId';
                const member1 = 'test-bucket:test-key-1/a:test-versionId';
                async.series([
                    next =>
                        addMembers(redisClient, 'test-site-2', [member], next),
                    next =>
                        addMembers(redisClient, 'test-site-2', [member1], next),
                    next =>
                        addMembers(redisClient, 'test-site-1', [member], next),
                    next => {
                        const route = '/_/crr/failed/test-bucket/test-key/a?' +
                            'versionId=test-versionId';
                        return getRequest(route, (err, res) => {
                            assert.ifError(err);
                            assert.strictEqual(res.IsTruncated, false);
                            assert.strictEqual(res.Versions.length, 2);
                            const matches = [{
                                member,
                                site: 'test-site-2',
                            }, {
                                member,
                                site: 'test-site-1',
                            }];
                            matches.forEach(match => {
                                const { member, site } = match;
                                // eslint-disable-next-line no-unused-vars
                                const [bucket, key, versionId] =
                                    member.split(':');
                                assert(res.Versions.some(o => (
                                    o.Bucket === bucket &&
                                    o.Key === key &&
                                    o.VersionId === testVersionId &&
                                    o.StorageClass === site
                                )));
                            });
                            return next();
                        });
                    },
                ], err => {
                    assert.ifError(err);
                    return done();
                });
            });

            it('should get correct data for POST route: ' +
            '/_/crr/failed when no key has been matched', done => {
                const body = JSON.stringify([{
                    Bucket: 'bucket',
                    Key: 'key',
                    VersionId: 'versionId',
                    StorageClass: 'site',
                }]);
                makeRetryPOSTRequest(body, (err, res) => {
                    assert.ifError(err);
                    getResponseBody(res, (err, resBody) => {
                        assert.ifError(err);
                        const body = JSON.parse(resBody);
                        assert.deepStrictEqual(body, []);
                        done();
                    });
                });
            });

            it('should get correct data for POST route: /_/crr/failed ' +
            'when there are multiple matching keys', done => {
                const member = `test-bucket:test-key:${testVersionId}`;
                async.series([
                    next =>
                        addMembers(redisClient, 'test-site-1', [member], next),
                    next =>
                        addMembers(redisClient, 'test-site-2', [member], next),
                    next =>
                        addMembers(redisClient, 'test-site-3', [member], next),
                ], err => {
                    assert.ifError(err);
                    const body = JSON.stringify([{
                        Bucket: 'test-bucket',
                        Key: 'test-key',
                        VersionId: testVersionId,
                        StorageClass: 'test-site-1',
                    }, {
                        Bucket: 'test-bucket',
                        Key: 'test-key',
                        VersionId: testVersionId,
                        // Should not be in response.
                        StorageClass: 'test-site-unknown',
                    }, {
                        Bucket: 'test-bucket',
                        Key: 'test-key',
                        VersionId: testVersionId,
                        StorageClass: 'test-site-2',
                    }, {
                        Bucket: 'test-bucket',
                        Key: 'test-key',
                        VersionId: testVersionId,
                        StorageClass: 'test-site-3',
                    }]);
                    makeRetryPOSTRequest(body, (err, res) => {
                        assert.ifError(err);
                        getResponseBody(res, (err, resBody) => {
                            assert.ifError(err);
                            const body = JSON.parse(resBody);
                            assert.deepStrictEqual(body, [{
                                Bucket: 'test-bucket',
                                Key: 'test-key',
                                VersionId: testVersionId,
                                StorageClass: 'test-site-1',
                                ReplicationStatus: 'PENDING',
                                LastModified: '2018-03-30T22:22:34.384Z',
                                Size: 1,
                            }, {
                                Bucket: 'test-bucket',
                                Key: 'test-key',
                                VersionId: testVersionId,
                                StorageClass: 'test-site-2',
                                ReplicationStatus: 'PENDING',
                                LastModified: '2018-03-30T22:22:34.384Z',
                                Size: 1,
                            }, {
                                Bucket: 'test-bucket',
                                Key: 'test-key',
                                VersionId: testVersionId,
                                StorageClass: 'test-site-3',
                                ReplicationStatus: 'PENDING',
                                LastModified: '2018-03-30T22:22:34.384Z',
                                Size: 1,
                            }]);
                            done();
                        });
                    });
                });
            }).timeout(10000);

            it('should get correct data at scale for POST route: /_/crr/failed',
            function f(done) {
                this.timeout(30000);
                const reqBody = [];
                async.timesLimit(10, 10, (i, next) => {
                    reqBody.push({
                        Bucket: `bucket-${i}`,
                        Key: `key-${i}`,
                        VersionId: testVersionId,
                        StorageClass: `site-${i}-a`,
                    }, {
                        Bucket: `bucket-${i}`,
                        Key: `key-${i}`,
                        VersionId: testVersionId,
                        StorageClass: `site-${i}-b`,
                    }, {
                        Bucket: `bucket-${i}`,
                        Key: `key-${i}`,
                        VersionId: testVersionId,
                        StorageClass: `site-${i}-c`,
                    }, {
                        Bucket: `bucket-${i}`,
                        Key: `key-${i}`,
                        VersionId: testVersionId,
                        StorageClass: `site-${i}-d`,
                    }, {
                        Bucket: `bucket-${i}`,
                        Key: `key-${i}`,
                        VersionId: testVersionId,
                        StorageClass: `site-${i}-e`,
                    });
                    const member = `bucket-${i}:key-${i}:${testVersionId}`;
                    async.series([
                        next => addMembers(redisClient, `site-${i}-a`, [member],
                            next),
                        next => addMembers(redisClient, `site-${i}-b`, [member],
                            next),
                        next => addMembers(redisClient, `site-${i}-c`, [member],
                            next),
                        next => addMembers(redisClient, `site-${i}-d`, [member],
                            next),
                        next => addMembers(redisClient, `site-${i}-e`, [member],
                            next),
                    ], next);
                }, err => {
                    assert.ifError(err);
                    const body = JSON.stringify(reqBody);
                    assert.ifError(err);
                    makeRetryPOSTRequest(body, (err, res) => {
                        assert.ifError(err);
                        getResponseBody(res, (err, resBody) => {
                            assert.ifError(err);
                            const body = JSON.parse(resBody);
                            assert.strictEqual(body.length, 10 * 5);
                            done();
                        });
                    });
                });
            });
        });
    });
});
