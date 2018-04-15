'use strict'; // eslint-disable-line strict

const async = require('async');
const zookeeper = require('node-zookeeper-client');

const { errors } = require('arsenal');
const { RedisClient } = require('arsenal').metrics;

const StatsModel = require('../models/StatsModel');
const BackbeatProducer = require('../BackbeatProducer');
const QueueEntry = require('../../lib/models/QueueEntry');
const Healthcheck = require('./Healthcheck');
const routes = require('./routes');
const { redisKeys } = require('../../extensions/replication/constants');
const promClient = require('prom-client');

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

        this._validSites = this._repConfig.destination.bootstrapList.map(
            item => item.site);

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

        this._redisClient = new RedisClient(this._redisConfig, this._logger);
        this._statsClient = new StatsModel(this._redisClient, INTERVAL,
            EXPIRY);
    }

    /**
     * Check if incoming request is valid
     * @param {BackbeatRequest} bbRequest - holds relevant data about request
     * @return {boolean} true/false
     */
    isValidRoute(bbRequest) {
        const rDetails = bbRequest.getRouteDetails();
        if (!rDetails) {
            return false;
        }
        const route = bbRequest.getRoute();
        // first validate healthcheck routes
        if (route.substring(3) === 'healthcheck') {
            return true;
        }
        if (route.substring(3) === 'monitoring/metrics') {
            return true;
        }

        /*
            {
                category: 'metrics',
                extension: 'crr',
                site: 'my-site-name',
                metric: 'backlog', (optional)
            }
        */
        // check metric routes
        // Are there any routes with matching extension?
        const extensions = routes.reduce((store, r) => {
            if (r.extensions[rDetails.extension] &&
                r.extensions[rDetails.extension].includes(rDetails.status)) {
                store.push(Object.keys(r.extensions));
            } else if (rDetails.category === 'metrics') {
                store.push(Object.keys(r.extensions));
            }
            return store;
        }, []);
        if (![].concat.apply([], extensions).includes(rDetails.extension)) {
            return false;
        }

        let specifiedType;
        const validRoutes = [];
        routes.forEach(r => {
            if (r.extensions[rDetails.extension] &&
                r.extensions[rDetails.extension].includes(rDetails.status)) {
                validRoutes.push(r);
                return;
            }
            if (!Object.keys(r.extensions).includes(rDetails.extension)) {
                return;
            }
            if (!r.extensions[rDetails.extension].includes(rDetails.site)) {
                return;
            }
            if (rDetails.metric && r.type === rDetails.metric) {
                specifiedType = r.type;
            }
            validRoutes.push(r);
        });

        if (validRoutes.length === 0) {
            return false;
        }

        // since this is an optional field, if a metric type was specified
        // in the route and it didn't match any metric types defined in
        // `routes.js`
        if (rDetails.metric && !specifiedType) {
            return false;
        }

        return true;
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
            const site = details.site;
            return this._queryStats(dataPoints, site, cb);
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
            if (err && err.type) {
                this._logger.error('error getting metric: backlog', {
                    origin: err.method,
                    method: 'BackbeatAPI.getBacklog',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting metrics: backlog', {
                    method: 'BackbeatAPI.getBacklog',
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
            if (err && err.type) {
                this._logger.error('error getting metric: completions', {
                    origin: err.method,
                    method: 'BackbeatAPI.getCompletions',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting metrics: completions', {
                    method: 'BackbeatAPI.getCompletions',
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
            if (err && err.type) {
                this._logger.error('error getting metric: throughput', {
                    origin: err.method,
                    method: 'BackbeatAPI.getThroughput',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err) {
                this._logger.error('error getting metrics: throughput', {
                    method: 'BackbeatAPI.getThroughput',
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
            if (err && err.type) {
                this._logger.error('error getting metric: all', {
                    origin: err.method,
                    method: 'BackbeatAPI.getAllMetrics',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting metrics: all', {
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
                    this._logger.error('error getting metrics: all', {
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
     * Collects metrics from Prometheus variables and register for response
     * @param {*} details - route details from lib/api/routes.js
     * @param {*} cb = callback(error, data)
     * @returns {undefined}
     */
    monitoringHandler(details, cb) {
        const promMetrics = promClient.register.metrics();
        return cb(null, promMetrics);
    }

    /**
     * Find all failed CRR operations that match the bucket, key, and versionID.
     * @param {Object} details - The route details
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    getFailedCRR(details, cb) {
        const { bucket, key, versionId } = details;
        const pattern = `${bucket}:${key}:${versionId}:*`;
        const cmds = ['hscan', redisKeys.failedCRR, 0, 'MATCH', pattern];
        this._redisClient.batch([cmds], (err, res) => {
            if (err) {
                return cb(err);
            }
            const [cmdErr, cursor] = res[0];
            if (cmdErr) {
                return cb(cmdErr);
            }
            const collection = cursor[1];
            const response = [];
            for (let i = 0; i < collection.length; i += 2) {
                const hashKey = collection[i];
                const [bucket, key, versionId, site] = hashKey.split(':');
                response.push({ bucket, key, versionId, site });
            }
            return cb(null, response);
        });
    }

    /**
     * Get all CRR operations that have failed.
     * @param {Object} details - The route details
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    getAllFailedCRR(details, cb) {
        const cmds = ['hgetall', redisKeys.failedCRR];
        this._redisClient.batch([cmds], (err, res) => {
            if (err) {
                return cb(err);
            }
            const [cmdErr, hashes] = res[0];
            if (cmdErr) {
                return cb(cmdErr);
            }
            const hashKeys = hashes ? Object.keys(hashes) : [];
            const response = hashKeys.map(hashKey => {
                const [bucket, key, versionId, site] = hashKey.split(':');
                return { bucket, key, versionId, site };
            });
            return cb(null, response);
        });
    }

    /**
     * For the given queue enry's site, send an entry with PENDING status to the
     * replication status topic, then send an entry to the replication topic so
     * that the queue processor re-attempts replication.
     * @param {QueueEntry} queueEntry - The queue entry constructed from the
     * failed kafka entry that was stored as a Redis hash value.
     * @param {Function} cb - The callback.
     * @return {undefined}
     */
    _pushToCRRRetryKafkaTopics(queueEntry, cb) {
        const site = queueEntry.getSite();
        const pendingEntry = queueEntry.toPendingEntry(site);
        const retryEntry = queueEntry.toRetryEntry(site);
        // Send messages in series to ensure status is updated in correct order.
        return async.series([
            next => this._crrStatusProducer
                .send([pendingEntry.toKafkaEntry(site)], next),
            next => this._crrProducer
                .send([retryEntry.toKafkaEntry(site)], next),
        ], cb);
    }

    /**
     * Process the stringified kafka entries that were stored in Redis, passing
     * each to `_pushToCRRRetryKafkaTopics`. For each kafka entry, construct the
     * expected HTTP response, passed as the second argument to the callback.
     * @param {Array} entries - The array of result values from Redis.
     * @param {Function} cb - The callback.
     * @return {undefined}
     */
    _processFailedKafkaEntries(entries, cb) {
        const response = [];
        return async.eachLimit(entries, 10, (entry, next) => {
            // If the hash key did not exist, entry is `null`.
            if (entry === null) {
                return next();
            }
            const kafkaEntry = { value: entry };
            const queueEntry = QueueEntry.createFromKafkaEntry(kafkaEntry);
            return this._pushToCRRRetryKafkaTopics(queueEntry, err => {
                if (err) {
                    this._logger.error('error pushing to kafka topics', {
                        method: 'BackbeatAPI._processFailedKafkaEntries',
                    });
                    return next(err);
                }
                response.push({
                    bucket: queueEntry.getBucket(),
                    key: queueEntry.getObjectKey(),
                    versionId: queueEntry.getEncodedVersionId(),
                    site: queueEntry.getSite(),
                    status: 'PENDING',
                });
                return next();
            });
        }, err => cb(err, response));
    }

    /**
     * Retry all CRR operations that have failed.
     * @param {Object} details - The route details
     * @param {String} body - The POST request body string
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    retryFailedCRR(details, body, cb) {
        const { error, reqBody } = this._parseRetryFailedCRR(body);
        if (error) {
            return cb(error);
        }
        const fields = reqBody.map(o => {
            const { bucket, key, versionId, site } = o;
            return `${bucket}:${key}:${versionId}:${site}`;
        });
        const cmds = ['hmget', redisKeys.failedCRR, ...fields];
        return this._redisClient.batch([cmds], (err, res) => {
            if (err) {
                return cb(err);
            }
            const [cmdErr, results] = res[0];
            if (cmdErr) {
                return cb(cmdErr);
            }
            return this._processFailedKafkaEntries(results, cb);
        });
    }

    /**
     * Validate that the POST request body has the necessary content.
     * @param {String} body - The POST request body string
     * @param {Function} cb - The callback to call
     * @return {Object} - Object containing any error and the request body
     */
    _parseRetryFailedCRR(body) {
        const msg = 'The body of your POST request is not well-formed';
        let reqBody;
        try {
            reqBody = JSON.parse(body);
        } catch (e) {
            return {
                error: errors.MalformedPOSTRequest.customizeDescription(msg),
            };
        }
        if (!Array.isArray(reqBody) || reqBody.length === 0) {
            return {
                error: errors.MalformedPOSTRequest.customizeDescription(
                    `${msg}: body must be a non-empty array`),
            };
        }
        let errMsg;
        reqBody.find(o => {
            if (typeof o !== 'object') {
                errMsg = `${msg}: body must be an array of objects`;
                return true;
            }
            const requiredProperties = ['bucket', 'key', 'versionId', 'site'];
            requiredProperties.find(prop => {
                if (typeof o[prop] !== 'string' || o[prop] === '') {
                    errMsg = `${msg}: ${prop} must be a non-empty string`;
                    return true;
                }
                return false;
            });
            return false;
        });
        if (errMsg) {
            return {
                error: errors.MalformedPOSTRequest.customizeDescription(errMsg),
            };
        }
        return { reqBody };
    }

    /**
     * Query StatsClient for all ops given
     * @param {array} ops - array of redis key names to query
     * @param {string} site - site name or '*' wildcard
     * @param {function} cb - callback(err, res)
     * @return {undefined}
     */
    _queryStats(ops, site, cb) {
        return async.map(ops, (op, done) => {
            if (site === 'all') {
                const queryString = `*:${op}:*`;
                return this._redisClient.scan(queryString, undefined,
                (err, res) => {
                    if (err) {
                        // escalate error to log later
                        return done({
                            message: `Redis error: ${err.message}`,
                            type: errors.InternalError,
                            method: 'BackbeatAPI._queryStats',
                        });
                    }
                    const allKeys = res.map(key => {
                        const arr = key.split(':');
                        // Remove the "requests:<timestamp>" and process
                        return arr.slice(0, arr.length - 2).join(':');
                    });
                    const reducedKeys = [...new Set(allKeys)];

                    return this._statsClient.getAllStats(this._logger,
                        reducedKeys, done);
                });
            }
            // Query only a single given site or storage class
            // First, validate the site or storage class
            if (!this._validSites.includes(site)) {
                // escalate error to log later
                return done({
                    message: 'invalid site name provided',
                    type: errors.RouteNotFound,
                    method: 'BackbeatAPI._queryStats',
                });
            }
            const queryString = `${site}:${op}`;
            return this._statsClient.getStats(this._logger, queryString, done);
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
