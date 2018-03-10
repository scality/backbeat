const assert = require('assert');
const http = require('http');
const Redis = require('ioredis');
const { Client, Producer } = require('kafka-node');

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
        let resCode;
        before(done => {
            const kafkaClient = new Client(config.zookeeper.connectionString,
                'TestClient');
            const testProducer = new Producer(kafkaClient);
            testProducer.createTopics([
                config.extensions.replication.topic,
                config.extensions.replication.replicationStatusTopic,
            ], false, () => {});

            const url = getUrl(defaultOptions, '/_/healthcheck');

            http.get(url, res => {
                resCode = res.statusCode;

                let rawData = '';
                res.on('data', chunk => {
                    rawData += chunk;
                });
                res.on('end', () => {
                    data = JSON.parse(rawData);
                    done();
                });
            });
        });

        it('should get a response with data', done => {
            assert(Object.keys(data).length > 0);
            assert.equal(resCode, 200);
            return done();
        });

        it('should have valid keys', done => {
            const keys = [].concat(...data.map(d => Object.keys(d)));

            assert(keys.includes('metadata'));
            assert(keys.includes('internalConnections'));
            return done();
        });

        it('should be healthy', done => {
            // NOTE: isrHealth is not checked here because circleci
            // kafka will have one ISR only. Maybe isrHealth should
            // be a test for end-to-end
            let internalConnections;
            data.forEach(obj => {
                if (Object.keys(obj).includes('internalConnections')) {
                    internalConnections = obj.internalConnections;
                }
            });
            Object.keys(internalConnections).forEach(key => {
                assert.ok(internalConnections[key]);
            });

            return done();
        });
    });

    describe('metrics routes', () => {
        const interval = 100;
        const expiry = 300;
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
            done();
        });

        afterEach(() => {
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
            statsClient.reportNewRequest(OPS, 5);
            statsClient.reportNewRequest(BYTES, 7100);

            getRequest('/_/metrics/backlog', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                assert.equal(res[key].results.count, 5);
                assert.equal(res[key].results.size, 7.10);
                done();
            });
        });

        it('should get the right data for route: /_/metrics/completions',
        done => {
            statsClient.reportNewRequest(OPS_DONE, 12);
            statsClient.reportNewRequest(BYTES_DONE, 1970);

            getRequest('/_/metrics/completions', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                assert.equal(res[key].results.count, 12);
                assert.equal(res[key].results.size, 1.97);
                done();
            });
        });

        it('should get the right data for route: /_/metrics/throughput',
        done => {
            statsClient.reportNewRequest(OPS_DONE, 300);
            statsClient.reportNewRequest(BYTES_DONE, 187400);

            getRequest('/_/metrics/throughput', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                assert.equal(res[key].results.count, 0.33);
                assert.equal(res[key].results.size, 0.21);
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
