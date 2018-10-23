const assert = require('assert');
const async = require('async');
const http = require('http');
const Redis = require('ioredis');
const { Producer } = require('node-rdkafka');
const { RedisClient } = require('arsenal').metrics;
const { StatsModel } = require('arsenal').metrics;

const config = require('../../config.json');
const { getRequest } = require('../utils/httpHelpers');
const getUrl = require('../utils/getUrl');
const fakeLogger = require('../utils/fakeLogger');

const redisConfig = { host: '127.0.0.1', port: 6379 };

describe('API routes', () => {
    const redisClient = new RedisClient(redisConfig, fakeLogger);
    const interval = 300;
    const expiry = 900;
    const statsClient = new StatsModel(redisClient, interval, expiry);

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
        const OPS_DONE = 'test:bb:opsdone';
        const BYTES_DONE = 'test:bb:bytesdone';
        statsClient.reportNewRequest(OPS, 1725);
        statsClient.reportNewRequest(BYTES, 2198);
        statsClient.reportNewRequest(OPS_DONE, 450);
        statsClient.reportNewRequest(BYTES_DONE, 1027);

        after(done => {
            const redis = new Redis();
            redis.flushall(done);
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
                const url = getUrl(path);

                http.get(url, res => {
                    assert.equal(res.statusCode, 200);
                    done();
                });
            });

            it(`should get correct data keys for route: ${path}`, done => {
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
        '/_/metrics/crr/all/backlog', done => {
            getRequest('/_/metrics/crr/all/backlog', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                // Backlog count = OPS - OPS_DONE
                assert.equal(res[key].results.count, 1275);
                // Backlog size = BYTES - BYTES_DONE
                assert.equal(res[key].results.size, 1171);
                done();
            });
        });

        it('should get the right data for route: ' +
        '/_/metrics/crr/all/completions', done => {
            getRequest('/_/metrics/crr/all/completions', (err, res) => {
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
        '/_/metrics/crr/all/throughput', done => {
            getRequest('/_/metrics/crr/all/throughput', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                // Throughput count = OPS_DONE / EXPIRY
                assert.equal(res[key].results.count, 0.5);
                // Throughput bytes = BYTES_DONE / EXPIRY
                assert.equal(res[key].results.size, 1.14);
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
                assert.equal(res.backlog.results.count, 1275);
                // Backlog size = BYTES - BYTES_DONE
                assert.equal(res.backlog.results.size, 1171);

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

                done();
            });
        });
    });

    it('should get a 404 route not found error response', () => {
        const url = getUrl('/_/invalidpath');

        http.get(url, res => {
            assert.equal(res.statusCode, 404);
        });
    });

    it('should get a 405 method not allowed from invalid http verb', done => {
        const options = {
            host: config.server.host,
            port: config.server.port,
            method: 'DELETE',
            path: '/_/healthcheck',
        };
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
