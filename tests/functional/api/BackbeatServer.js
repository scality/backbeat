const assert = require('assert');
const async = require('async');
const http = require('http');
const Redis = require('ioredis');
const { Producer } = require('node-rdkafka');

const { RedisClient } = require('arsenal').metrics;

const StatsModel = require('../../../lib/models/StatsModel');
const config = require('../../config.json');
const { makePOSTRequest, getResponseBody } =
    require('../utils/makePOSTRequest');
const redisConfig = { host: '127.0.0.1', port: 6379 };
const REDIS_KEY_FAILED_CRR = 'test:bb:crr:failed';

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

function setHash(redisClient, keys, value, cb) {
    const cmds = keys.map(key => (['hset', REDIS_KEY_FAILED_CRR, key, value]));
    redisClient.batch(cmds, cb);
}

function deleteHash(redisClient, cb) {
    const cmds = ['del', REDIS_KEY_FAILED_CRR];
    redisClient.batch([cmds], cb);
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

    describe('metrics routes', () => {
        const interval = 300;
        const expiry = 900;
        const OPS = 'test:bb:ops';
        const BYTES = 'test:bb:bytes';
        const OPS_DONE = 'test:bb:opsdone';
        const BYTES_DONE = 'test:bb:bytesdone';

        const destconfig = config.extensions.replication.destination;
        const site1 = destconfig.bootstrapList[0].site;
        const site2 = destconfig.bootstrapList[1].site;

        let redisClient;
        let statsClient;
        let redis;

        before(done => {
            redis = new Redis();
            redisClient = new RedisClient(redisConfig, fakeLogger);
            statsClient = new StatsModel(redisClient, interval, expiry);

            statsClient.reportNewRequest(`${site1}:${OPS}`, 1725);
            statsClient.reportNewRequest(`${site1}:${BYTES}`, 219800);
            statsClient.reportNewRequest(`${site1}:${OPS_DONE}`, 450);
            statsClient.reportNewRequest(`${site1}:${BYTES_DONE}`, 102700);

            statsClient.reportNewRequest(`${site2}:${OPS}`, 900);
            statsClient.reportNewRequest(`${site2}:${BYTES}`, 294300);
            statsClient.reportNewRequest(`${site2}:${OPS_DONE}`, 300);
            statsClient.reportNewRequest(`${site2}:${BYTES_DONE}`, 187400);

            done();
        });

        after(() => {
            redis.keys('*:test:bb:*').then(keys => {
                const pipeline = redis.pipeline();
                keys.forEach(key => {
                    pipeline.del(key);
                });
                return pipeline.exec();
            });
        });

        // TODO: refactor this
        const metricsPaths = [
            '/_/metrics/crr/all',
            '/_/metrics/crr/all/backlog',
            '/_/metrics/crr/all/completions',
            '/_/metrics/crr/all/throughput',
        ];

        metricsPaths.forEach(path => {
            it(`should get a 200 response for route: ${path}`, done => {
                const url = getUrl(defaultOptions, path);

                http.get(url, res => {
                    assert.equal(res.statusCode, 200);
                    done();
                });
            });

            it(`should get correct data keys for route: ${path}`,
            done => {
                getRequest(path, (err, res) => {
                    assert.ifError(err);
                    const key = Object.keys(res)[0];
                    assert(res[key].description);
                    assert.equal(typeof res[key].description, 'string');

                    assert(res[key].results);
                    assert.deepEqual(Object.keys(res[key].results),
                        ['count', 'size']);
                    done();
                });
            });
        });

        const retryPaths = [
            '/_/crr/failed',
            '/_/crr/failed/test-bucket/test-key/test-versionId',
        ];

        retryPaths.forEach(path => {
            it(`should get a 200 response for route: ${path}`, done => {
                const url = getUrl(defaultOptions, path);

                http.get(url, res => {
                    assert.equal(res.statusCode, 200);
                    done();
                });
            });
        });

        // TODO: refactor this
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
        ];

        allWrongPaths.forEach(path => {
            it(`should get a 404 response for route: ${path}`,
            done => {
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

        it('should get a 200 response for route: /_/crr/failed', done => {
            const body = JSON.stringify([{
                bucket: 'bucket',
                key: 'key',
                versionId: 'versionId',
                site: 'site',
            }]);
            makeRetryPOSTRequest(body, (err, res) => {
                assert.ifError(err);
                assert.strictEqual(res.statusCode, 200);
                done();
            });
        });

        const invalidPOSTRequestBodies = [
            'a',
            {},
            [],
            ['a'],
            [{
                // Missing bucket property.
                key: 'b',
                versionId: 'c',
                site: 'd',
            }],
            [{
                // Missing key property.
                bucket: 'a',
                versionId: 'c',
                site: 'd',
            }],
            [{
                // Missing versionId property.
                bucket: 'a',
                key: 'b',
                site: 'd',
            }],
            [{
                // Missing site property.
                bucket: 'a',
                key: 'b',
                versionId: 'c',
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
            afterEach(done => deleteHash(redisClient, done));

            it('should get correct data for GET route: /_/crr/failed when no ' +
            'hash key has been created', done => {
                getRequest('/_/crr/failed', (err, res) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(res, []);
                    done();
                });
            });

            it('should get correct data for GET route: /_/crr/failed when ' +
            'the hash has been created and there is one hash key', done => {
                const key = 'test-bucket:test-key:test-versionId:test-site';
                setHash(redisClient, [key], '{}', err => {
                    assert.ifError(err);
                    getRequest('/_/crr/failed', (err, res) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(res, [{
                            bucket: 'test-bucket',
                            key: 'test-key',
                            versionId: 'test-versionId',
                            site: 'test-site',
                        }]);
                        done();
                    });
                });
            });

            it('should get correct data for GET route: /_/crr/failed when ' +
            'the hash has been created and there are multiple hash keys',
            done => {
                const keys = [
                    'test-bucket:test-key:test-versionId:test-site',
                    'test-bucket-1:test-key-1:test-versionId-1:test-site-1',
                    'test-bucket-2:test-key-2:test-versionId-2:test-site-2',
                ];
                setHash(redisClient, keys, '{}', err => {
                    assert.ifError(err);
                    getRequest('/_/crr/failed', (err, res) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(res, [{
                            bucket: 'test-bucket',
                            key: 'test-key',
                            versionId: 'test-versionId',
                            site: 'test-site',
                        }, {
                            bucket: 'test-bucket-1',
                            key: 'test-key-1',
                            versionId: 'test-versionId-1',
                            site: 'test-site-1',
                        }, {
                            bucket: 'test-bucket-2',
                            key: 'test-key-2',
                            versionId: 'test-versionId-2',
                            site: 'test-site-2',
                        }]);
                        done();
                    });
                });
            });

            it('should get correct data at scale for GET route: /_/crr/failed',
            function f(done) {
                this.timeout(30000);
                async.timesLimit(20000, 10, (i, next) => {
                    const keys = [
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-a`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-b`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-c`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-d`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-e`,
                    ];
                    setHash(redisClient, keys, '{}', next);
                }, err => {
                    assert.ifError(err);
                    getRequest('/_/crr/failed', (err, res) => {
                        assert.ifError(err);
                        assert.strictEqual(res.length, 20000 * 5);
                        done();
                    });
                });
            });

            it('should get correct data for GET route: ' +
            '/_/crr/failed/<bucket>/<key>/<versionId> when there is no key',
            done => {
                getRequest('/_/crr/failed/test-bucket/test-key/test-versionId',
                (err, res) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(res, []);
                    done();
                });
            });

            it('should get correct data for GET route: ' +
            '/_/crr/failed/<bucket>/<key>/<versionId> when there is one hash ' +
            'key', done => {
                const keys = [
                    'test-bucket:test-key:test-versionId:test-site',
                    'test-bucket:test-key:test-versionId:test-site-2',
                    'test-bucket-1:test-key-1:test-versionId-1:test-site',
                ];
                setHash(redisClient, keys, '{}', err => {
                    assert.ifError(err);
                    const route =
                        '/_/crr/failed/test-bucket/test-key/test-versionId';
                    return getRequest(route, (err, res) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(res, [{
                            bucket: 'test-bucket',
                            key: 'test-key',
                            versionId: 'test-versionId',
                            site: 'test-site',
                        }, {
                            bucket: 'test-bucket',
                            key: 'test-key',
                            versionId: 'test-versionId',
                            site: 'test-site-2',
                        }]);
                        return done();
                    });
                });
            });

            it('should get correct data for GET route: /_/crr/failed when no ' +
            'hash key has been matched', done => {
                const body = JSON.stringify([{
                    bucket: 'bucket',
                    key: 'key',
                    versionId: 'versionId',
                    site: 'site',
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
            'when there are multiple matching hash keys', done => {
                const keys = [
                    'test-bucket:test-key:test-versionId:test-site-1',
                    'test-bucket:test-key:test-versionId:test-site-2',
                    'test-bucket:test-key:test-versionId:test-site-3',
                ];
                setHash(redisClient, keys, '{}', err => {
                    assert.ifError(err);
                    const body = JSON.stringify([{
                        bucket: 'test-bucket',
                        key: 'test-key',
                        versionId: 'test-versionId',
                        site: 'test-site-1',
                    }, {
                        bucket: 'test-bucket',
                        key: 'test-key',
                        versionId: 'test-versionId',
                        site: 'test-site-unknown', // Should not be in response.
                    }, {
                        bucket: 'test-bucket',
                        key: 'test-key',
                        versionId: 'test-versionId',
                        site: 'test-site-2',
                    }, {
                        bucket: 'test-bucket',
                        key: 'test-key',
                        versionId: 'test-versionId',
                        site: 'test-site-3',
                    }]);
                    makeRetryPOSTRequest(body, (err, res) => {
                        assert.ifError(err);
                        getResponseBody(res, (err, resBody) => {
                            assert.ifError(err);
                            const body = JSON.parse(resBody);
                            assert.deepStrictEqual(body, [{
                                bucket: 'test-bucket',
                                key: 'test-key',
                                versionId: 'test-versionId',
                                site: 'test-site-1',
                                status: 'PENDING',
                            }, {
                                bucket: 'test-bucket',
                                key: 'test-key',
                                versionId: 'test-versionId',
                                site: 'test-site-2',
                                status: 'PENDING',
                            }, {
                                bucket: 'test-bucket',
                                key: 'test-key',
                                versionId: 'test-versionId',
                                site: 'test-site-3',
                                status: 'PENDING',
                            }]);
                            done();
                        });
                    });
                });
            });

            it('should get correct data at scale for POST route: /_/crr/failed',
            function f(done) {
                this.timeout(30000);
                const reqBody = [];
                async.timesLimit(2000, 10, (i, next) => {
                    reqBody.push({
                        bucket: `bucket-${i}`,
                        key: `key-${i}`,
                        versionId: `versionId-${i}`,
                        site: `site-${i}-a`,
                    }, {
                        bucket: `bucket-${i}`,
                        key: `key-${i}`,
                        versionId: `versionId-${i}`,
                        site: `site-${i}-b`,
                    }, {
                        bucket: `bucket-${i}`,
                        key: `key-${i}`,
                        versionId: `versionId-${i}`,
                        site: `site-${i}-c`,
                    }, {
                        bucket: `bucket-${i}`,
                        key: `key-${i}`,
                        versionId: `versionId-${i}`,
                        site: `site-${i}-d`,
                    }, {
                        bucket: `bucket-${i}`,
                        key: `key-${i}`,
                        versionId: `versionId-${i}`,
                        site: `site-${i}-e`,
                    });
                    const keys = [
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-a`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-b`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-c`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-d`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-e`,
                    ];
                    setHash(redisClient, keys, '{}', next);
                }, err => {
                    assert.ifError(err);
                    const body = JSON.stringify(reqBody);
                    assert.ifError(err);
                    makeRetryPOSTRequest(body, (err, res) => {
                        assert.ifError(err);
                        getResponseBody(res, (err, resBody) => {
                            assert.ifError(err);
                            const body = JSON.parse(resBody);
                            assert.strictEqual(body.length, 2000 * 5);
                            done();
                        });
                    });
                });
            });
        });

        it('should get the right data for route: ' +
        `/_/metrics/crr/${site1}/backlog`, done => {
            getRequest(`/_/metrics/crr/${site1}/backlog`, (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                // Backlog count = OPS - OPS_DONE
                assert.equal(res[key].results.count, 1275);
                // Backlog size = (BYTES - BYTES_DONE) / 1000
                assert.equal(res[key].results.size, 117.1);
                done();
            });
        });

        it('should get the right data for route: ' +
        '/_/metrics/crr/all/backlog', done => {
            getRequest('/_/metrics/crr/all/backlog', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                // Backlog count = OPS - OPS_DONE
                assert.equal(res[key].results.count, 1875);
                // Backlog size = (BYTES - BYTES_DONE) / 1000
                assert.equal(res[key].results.size, 224.0);
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
                // Completions bytes = BYTES_DONE / 1000
                assert.equal(res[key].results.size, 102.7);
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
                // Completions bytes = BYTES_DONE / 1000
                assert.equal(res[key].results.size, 290.1);
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
                // Throughput bytes = (BYTES_DONE / 1000) / EXPIRY
                assert.equal(res[key].results.size, 0.11);
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
                assert.equal(res[key].results.size, 0.32);
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

                assert(res.backlog.description);
                // Backlog count = OPS - OPS_DONE
                assert.equal(res.backlog.results.count, 1275);
                // Backlog size = (BYTES - BYTES_DONE) / 1000
                assert.equal(res.backlog.results.size, 117.1);

                assert(res.completions.description);
                // Completions count = OPS_DONE
                assert.equal(res.completions.results.count, 450);
                // Completions bytes = BYTES_DONE / 1000
                assert.equal(res.completions.results.size, 102.7);

                assert(res.throughput.description);
                // Throughput count = OPS_DONE / EXPIRY
                assert.equal(res.throughput.results.count, 0.5);
                // Throughput bytes = (BYTES_DONE / 1000) / EXPIRY
                assert.equal(res.throughput.results.size, 0.11);

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

                assert(res.backlog.description);
                // Backlog count = OPS - OPS_DONE
                assert.equal(res.backlog.results.count, 1875);
                // Backlog size = (BYTES - BYTES_DONE) / 1000
                assert.equal(res.backlog.results.size, 224.0);

                assert(res.completions.description);
                // Completions count = OPS_DONE
                assert.equal(res.completions.results.count, 750);
                // Completions bytes = BYTES_DONE / 1000
                assert.equal(res.completions.results.size, 290.1);

                assert(res.throughput.description);
                // Throughput count = OPS_DONE / EXPIRY
                assert.equal(res.throughput.results.count, 0.83);
                // Throughput bytes = (BYTES_DONE / 1000) / EXPIRY
                assert.equal(res.throughput.results.size, 0.32);

                done();
            });
        });
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
});
