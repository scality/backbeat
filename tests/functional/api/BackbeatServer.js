const assert = require('assert');
const async = require('async');
const http = require('http');
const Redis = require('ioredis');
const { Producer } = require('node-rdkafka');

const { RedisClient, StatsClient } = require('arsenal').metrics;

const config = require('../../config.json');
const allRoutes = require('../../../lib/api/routes');
const redisConfig = { host: '127.0.0.1', port: 6379 };

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

        let redisClient;
        let statsClient;
        let redis;

        before(done => {
            redis = new Redis();
            redisClient = new RedisClient(redisConfig, fakeLogger);
            statsClient = new StatsClient(redisClient, interval, expiry);

            statsClient.reportNewRequest(OPS, 1725);
            statsClient.reportNewRequest(BYTES, 219800);
            statsClient.reportNewRequest(OPS_DONE, 450);
            statsClient.reportNewRequest(BYTES_DONE, 102700);

            done();
        });

        after(() => {
            redis.keys('test:bb:*').then(keys => {
                const pipeline = redis.pipeline();
                keys.forEach(key => {
                    pipeline.del(key);
                });
                return pipeline.exec();
            });
        });

        allRoutes.forEach(route => {
            if (route.path.indexOf('healthcheck') === -1) {
                it(`should get a 200 response for route: ${route.path}`,
                done => {
                    const url = getUrl(defaultOptions, route.path);

                    http.get(url, res => {
                        assert.equal(res.statusCode, 200);
                        done();
                    });
                });

                it(`should get correct data keys for route: ${route.path}`,
                done => {
                    getRequest(route.path, (err, res) => {
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
            }
        });

        it('should get the right data for route: /_/metrics/backlog',
        done => {
            getRequest('/_/metrics/backlog', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                assert.equal(res[key].results.count, 1275);
                assert.equal(res[key].results.size, 117.1);
                done();
            });
        });

        it('should get the right data for route: /_/metrics/completions',
        done => {
            getRequest('/_/metrics/completions', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                assert.equal(res[key].results.count, 450);
                assert.equal(res[key].results.size, 102.7);
                done();
            });
        });

        it('should get the right data for route: /_/metrics/throughput',
        done => {
            getRequest('/_/metrics/throughput', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                assert.equal(res[key].results.count, 0.5);
                assert.equal(res[key].results.size, 0.11);
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
        options.method = 'POST';
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
