'use strict'; // eslint-disable-line strict

const async = require('async');
const zookeeper = require('node-zookeeper-client');

const { errors } = require('arsenal');
const { RedisClient, StatsClient } = require('arsenal').metrics;

const BackbeatProducer = require('../BackbeatProducer');
const Healthcheck = require('./Healthcheck');
const routes = require('./routes');

// StatsClient constant defaults
// TODO: This should be moved to constants file
const INTERVAL = 300; // 5 minutes
const EXPIRY = 900; // 15 minutes

/**
 * Class representing Backbeat API endpoints and internals
 *
 * @class
 */
class BackbeatAPI {
    /**
     * @constructor
     * @param {object} config - configurations for setup
     * @param {werelogs.Logger} logger - Logger object
     * @param {object} optional - optional fields
     */
    constructor(config, logger, optional) {
        this._zkConfig = config.zookeeper;
        this._kafkaConfig = config.kafka;
        this._repConfig = config.extensions.replication;
        this._crrTopic = this._repConfig.topic;
        this._crrStatusTopic = this._repConfig.replicationStatusTopic;
        this._metricsTopic = config.metrics.topic;
        this._queuePopulator = config.queuePopulator;
        this._kafkaHost = config.kafka.hosts;
        this._redisConfig = config.redis;
        this._logger = logger;

        this._crrProducer = null;
        this._crrStatusProducer = null;
        this._metricProducer = null;
        this._healthcheck = null;
        this._zkClient = null;

        // TODO: this should rely on the data stored in Redis and not an
        //  internal timer
        if (optional && optional.timer) {
            // set to old date so routes below will use EXPIRY
            this._internalStart = new Date(1);
        } else {
            this._internalStart = Date.now();
        }

        const redisClient = new RedisClient(this._redisConfig, this._logger);
        this._statsClient = new StatsClient(redisClient, INTERVAL, EXPIRY);
    }

    /**
     * Check if incoming request is valid
     * @param {string} route - request route
     * @return {boolean} true/false
     */
    isValidRoute(route) {
        return routes.some(r => r.path.includes(route));
    }

    /**
     * Check if Zookeeper and Producer are connected
     * @return {boolean} true/false
     */
    isConnected() {
        return this._zkClient.getState().name === 'SYNC_CONNECTED'
            && this._checkProducersReady();
    }

    _checkProducersReady() {
        return this._crrProducer.isReady() && this._metricProducer.isReady()
            && this._crrStatusProducer.isReady();
    }

    /**
     * Check if Zookeeper and Producer are connected
     * @return {boolean} true/false
     */
    isConnected() {
        return this._zkClient.getState().name === 'SYNC_CONNECTED'
            && this._checkProducersReady();
    }

    /**
     * Get Kafka healthcheck
     * @param {object} details - route details from lib/api/routes.js
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    getHealthcheck(details, cb) {
        return this._healthcheck.getHealthcheck((err, data) => {
            if (err) {
                this._logger.error('error getting healthcheck', err);
                return cb(errors.InternalError);
            }
            return cb(null, data);
        });
    }

    /**
     * Get data points which are the keys used to query Redis
     * @param {object} details - route details from lib/api/routes.js
     * @param {array} data - provides already fetched data in order of
     *   dataPoints mentioned for each route in lib/api/routes.js. This can be
     *   undefined.
     * @param {function} cb - callback(error, data), where data returns
     *   data stored in Redis.
     * @return {array} dataPoints array defined in lib/api/routes.js
     */
    _getData(details, data, cb) {
        if (!data) {
            const dataPoints = details.dataPoints;
            return this._queryStats(dataPoints, cb);
        }
        return cb(null, data);
    }

    /**
     * Get replication backlog in ops count and size in MB
     * @param {object} details - route details from lib/api/routes.js
     * @param {function} cb - callback(error, data)
     * @param {array} data - optional field providing already fetched data
     *   in order of dataPoints mentioned for each route in lib/api/routes.js
     * @return {undefined}
     */
    getBacklog(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error occurred getting backlog', {
                    method: 'BackbeatAPI.backlog',
                });
                return cb(errors.InternalError);
            }
            const d = res.map(r => r.requests);

            let opsBacklog = d[0] - d[1];
            if (opsBacklog < 0) opsBacklog = 0;
            let bytesBacklog = d[2] - d[3];
            if (bytesBacklog < 0) bytesBacklog = 0;
            const response = {
                backlog: {
                    description: 'Number of incomplete replication ' +
                        'operations (count) and number of incomplete MB ' +
                        'transferred (size)',
                    results: {
                        count: opsBacklog,
                        size: (bytesBacklog / 1000).toFixed(2),
                    },
                },
            };
            return cb(null, response);
        });
    }

    /**
     * Get completed replicated stats by ops count and size in MB
     * @param {object} details - route details from lib/api/routes.js
     * @param {function} cb - callback(error, data)
     * @param {array} data - optional field providing already fetched data
     *   in order of dataPoints mentioned for each route in lib/api/routes.js
     * @return {undefined}
     */
    getCompletions(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting replication completions', {
                    method: 'BackbeatAPI.completions',
                });
                return cb(errors.InternalError);
            }

            // Find if time since start is less than EXPIRY time
            const timeSinceStart = Math.floor(
                (Date.now() - this._internalStart) / 1000);
            // if timeSinceStart, then round the number to nearest 10's
            const timeDisplay = timeSinceStart < EXPIRY ?
                (Math.ceil(timeSinceStart / 10) * 10) : EXPIRY;

            const response = {
                completions: {
                    description: 'Number of completed replication operations ' +
                        '(count) and number of MB transferred (size) in the ' +
                        `last ${timeDisplay} seconds`,
                    results: {
                        count: res[0].requests,
                        size: (res[1].requests / 1000).toFixed(2),
                    },
                },
            };
            return cb(null, response);
        });
    }

    /**
     * Get current throughput in ops/sec and MB/sec
     * Throughput is the number of units processed in a given time
     * @param {object} details - route details from lib/api/routes.js
     * @param {function} cb - callback(error, data)
     * @param {array} data - optional field providing already fetched data
     *   in order of dataPoints mentioned for each route in lib/api/routes.js
     * @return {undefined}
     */
    getThroughput(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err) {
                this._logger.error('error getting throughput', {
                    method: 'BackbeatAPI.throughput',
                });
                return cb(errors.InternalError);
            }

            const [opsThroughput, bytesThroughput] = res.map(r => {
                // Find if time since start is less than r.sampleDuration
                const timeSinceStart = Math.floor(
                    (Date.now() - this._internalStart) / 1000);
                if (timeSinceStart < r.sampleDuration) {
                    return (r.requests / timeSinceStart).toFixed(2);
                }
                return (r.requests / r.sampleDuration).toFixed(2);
            });

            const response = {
                throughput: {
                    description: 'Current throughput for replication ' +
                        'operations in ops/sec (count) and MB/sec (size)',
                    results: {
                        count: opsThroughput,
                        size: (bytesThroughput / 1000).toFixed(2),
                    },
                },
            };
            return cb(null, response);
        });
    }

    /**
     * Get all metrics
     * @param {object} details - route details from lib/api/routes.js
     * @param {function} cb = callback(error, data)
     * @param {array} data - optional field providing already fetched data
     *   in order of dataPoints mentioned for each route in lib/api/routes.js
     * @return {undefined}
     */
    getAllMetrics(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting all metrics', {
                    method: 'BackbeatAPI.getAllMetrics',
                });
                return cb(errors.InternalError);
            }
            // res = [ ops, ops_done, bytes, bytes_done ]
            return async.parallel([
                done => this.getBacklog({ dataPoints: new Array(4) }, done,
                    res),
                done => this.getCompletions({ dataPoints: new Array(2) }, done,
                    [res[1], res[3]]),
                done => this.getThroughput({ dataPoints: new Array(2) }, done,
                    [res[1], res[3]]),
            ], (err, results) => {
                if (err) {
                    this._logger.error('error getting all metrics', {
                        method: 'BackbeatAPI.getAllMetrics',
                    });
                    return cb(errors.InternalError);
                }
                const store = Object.assign({}, ...results);
                return cb(null, store);
            });
        });
    }

    /**
     * Query StatsClient for all ops given
     * @param {array} ops - array of redis key names to query
     * @param {function} cb - callback(err, res)
     * @return {undefined}
     */
    _queryStats(ops, cb) {
        return async.map(ops, (op, done) => {
            this._statsClient.getStats(this._logger, op, done);
        }, cb);
    }

    /**
     * Setup internals
     * @param {function} cb - callback(error)
     * @return {undefined}
     */
    setupInternals(cb) {
        async.parallel([
            done => this._setZookeeper(done),
            done => this._setProducer(this._metricsTopic, (err, producer) => {
                if (err) {
                    return done(err);
                }
                this._metricProducer = producer;
                return done();
            }),
            done => this._setProducer(this._crrTopic, (err, producer) => {
                if (err) {
                    return done(err);
                }
                this._crrProducer = producer;
                return done();
            }),
            done => this._setProducer(this._crrStatusTopic, (err, producer) => {
                if (err) {
                    return done(err);
                }
                this._crrStatusProducer = producer;
                return done();
            }),
        ], err => {
            if (err) {
                this._logger.error('error setting up internal clients');
                return cb(err);
            }
            this._healthcheck = new Healthcheck(this._repConfig, this._zkClient,
                this._crrProducer, this._crrStatusProducer,
                this._metricProducer);
            this._logger.info('BackbeatAPI setup ready');
            return cb();
        });
    }

    _setProducer(topic, cb) {
        const producer = new BackbeatProducer({
            kafka: { hosts: this._kafkaConfig.hosts },
            topic,
        });

        producer.once('error', cb);
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', error => {
                this._logger.error('error from backbeat producer', { error });
            });
            return cb(null, producer);
        });
    }

    _setZookeeper(cb) {
        const populatorZkPath = this._queuePopulator.zookeeperPath;
        const zookeeperUrl =
            `${this._zkConfig.connectionString}${populatorZkPath}`;

        const zkClient = zookeeper.createClient(zookeeperUrl, {
            autoCreateNamespace: this._zkConfig.autoCreateNamespace,
        });
        zkClient.connect();

        zkClient.once('error', cb);
        zkClient.once('connected', () => {
            zkClient.removeAllListeners('error');
            this._zkClient = zkClient;
            return cb();
        });
    }
}

module.exports = BackbeatAPI;
