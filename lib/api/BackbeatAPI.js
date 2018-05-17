'use strict'; // eslint-disable-line strict

const async = require('async');
const zookeeper = require('node-zookeeper-client');

const { errors } = require('arsenal');
const { RedisClient, StatsModel } = require('arsenal').metrics;
const { Metrics } = require('arsenal').backbeat;

const BackbeatProducer = require('../BackbeatProducer');
const QueueEntry = require('../../lib/models/QueueEntry');
const Healthcheck = require('./Healthcheck');
const routes = require('./routes');
const { redisKeys } = require('../../extensions/replication/constants');
const monitoringClient = require('../clients/monitoringHandler').client;

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
            (EXPIRY + INTERVAL));
        const metricsConfig = {
            redisConfig: this._redisConfig,
            validSites: this._validSites,
            internalStart: this._internalStart,
        };
        const metrics = new Metrics(metricsConfig, logger);
        Object.assign(this, {
            _queryStats: metrics._queryStats,
            _getData: metrics._getData,
            getBacklog: metrics.getBacklog,
            getCompletions: metrics.getCompletions,
            getThroughput: metrics.getThroughput,
            getAllMetrics: metrics.getAllMetrics,
        });
    }

    /**
     * Validate possible query strings.
     * @param {BackbeatRequest} bbRequest - Relevant data about request
     * @return {Object|null} - The error object or `null` if no error
     */
    validateQuery(bbRequest) {
        const { marker } = bbRequest.getRouteDetails();
        if (marker !== undefined && (marker === '' || isNaN(marker))) {
            return errors.InvalidQueryParameter
                .customizeDescription('marker must be a number');
        }
        return null;
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
        // first validate healthcheck routes or prom routes
        const route = bbRequest.getRoute();
        if (route === 'healthcheck' || route === 'monitoring/metrics') {
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
     * Collects metrics from Prometheus variables and register for response
     * @param {Object} details - route details from lib/api/routes.js
     * @param {Function} cb = callback(error, data)
     * @returns {undefined}
     */
    monitoringHandler(details, cb) {
        const promMetrics = monitoringClient.register.metrics();
        return cb(null, promMetrics);
    }

    /**
     * Builds the failed CRR response.
     * @param {String} cursor - The Redis HSCAN cursor
     * @param {Array} hashes - The collection of Redis hashes for the iteration
     * @return {Object} - The response object
     */
    _getFailedCRRResponse(cursor, hashes) {
        const response = {
            IsTruncated: Number.parseInt(cursor, 10) !== 0,
            Versions: [],
        };
        if (response.IsTruncated) {
            response.NextMarker = Number.parseInt(cursor, 10);
        }
        for (let i = 0; i < hashes.length; i += 2) {
            const [bucket, key, versionId, site] = hashes[i].split(':');
            const entry = hashes[i + 1];
            const value = JSON.parse(JSON.parse(entry).value);
            response.Versions.push({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
                StorageClass: site,
                Size: value['content-length'],
                LastModified: value['last-modified'],
            });
        }
        return response;
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
        const cmds =
            ['hscan', redisKeys.failedCRR, 0, 'MATCH', pattern, 'COUNT', 1000];
        this._redisClient.batch([cmds], (err, res) => {
            if (err) {
                return cb(err);
            }
            const [cmdErr, collection] = res[0];
            if (cmdErr) {
                return cb(cmdErr);
            }
            const [cursor, hashes] = collection;
            return cb(null, this._getFailedCRRResponse(cursor, hashes));
        });
    }

    /**
     * Get all CRR operations that have failed.
     * @param {Object} details - The route details
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    getAllFailedCRR(details, cb) {
        const marker = Number.parseInt(details.marker, 10) || 0;
        const cmds = ['hscan', redisKeys.failedCRR, marker, 'COUNT', 1000];
        this._redisClient.batch([cmds], (err, res) => {
            if (err) {
                return cb(err);
            }
            const [cmdErr, collection] = res[0];
            if (cmdErr) {
                return cb(cmdErr);
            }
            const [cursor, hashes] = collection;
            return cb(null, this._getFailedCRRResponse(cursor, hashes));
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
     * Delete the failed CRR Redis hash field.
     * @param {String} field - The field in the hash to delete
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    _deleteFailedCRRField(field, cb) {
        const cmds = ['hdel', redisKeys.failedCRR, field];
        return this._redisClient.batch([cmds], (err, res) => {
            if (err) {
                this._logger.error('error deleting redis hash field', {
                    method: 'BackbeatAPI._deleteFailedCRRField',
                    key: redisKeys.failedCRR,
                    field,
                    error: err,
                });
                return cb(err);
            }
            const [cmdErr] = res[0];
            if (cmdErr) {
                this._logger.error('error deleting redis hash field', {
                    method: 'BackbeatAPI._deleteFailedCRRField',
                    key: redisKeys.failedCRR,
                    field,
                    error: cmdErr,
                });
                return cb(cmdErr);
            }
            return cb();
        });
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
                const Bucket = queueEntry.getBucket();
                const Key = queueEntry.getObjectKey();
                const VersionId = queueEntry.getEncodedVersionId();
                const StorageClass = queueEntry.getSite();
                response.push({
                    Bucket,
                    Key,
                    VersionId,
                    StorageClass,
                    Size: queueEntry.getContentLength(),
                    LastModified: queueEntry.getLastModified(),
                    ReplicationStatus: 'PENDING',
                });
                const field = `${Bucket}:${Key}:${VersionId}:${StorageClass}`;
                return this._deleteFailedCRRField(field, err => {
                    if (err) {
                        this._logger.error('could not delete redis hash key ' +
                        'after pushing to kafka topics', {
                            method: 'BackbeatAPI._processFailedKafkaEntries',
                            error: err,
                        });
                        return next(err);
                    }
                    return next();
                });
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
            const { Bucket, Key, VersionId, StorageClass } = o;
            return `${Bucket}:${Key}:${VersionId}:${StorageClass}`;
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
            const requiredProperties =
                ['Bucket', 'Key', 'VersionId', 'StorageClass'];
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
