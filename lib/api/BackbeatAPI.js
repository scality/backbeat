'use strict'; // eslint-disable-line strict

const async = require('async');
const Redis = require('ioredis');
const zookeeper = require('node-zookeeper-client');

const { errors } = require('arsenal');
const { RedisClient, StatsModel } = require('arsenal').metrics;
const { Metrics } = require('arsenal').backbeat;
const getRoutesFn = require('arsenal').backbeat.routes;

const BackbeatProducer = require('../BackbeatProducer');
const ObjectQueueEntry =
    require('../../extensions/replication/utils/ObjectQueueEntry');
const ObjectFailureEntry =
    require('../../extensions/replication/utils/ObjectFailureEntry');
const BackbeatMetadataProxy =
    require('../../extensions/replication/utils/BackbeatMetadataProxy');
const Healthcheck = require('./Healthcheck');
const { redisKeys } = require('../../extensions/replication/constants');
const getFailedCRRKey = require('../util/getFailedCRRKey');
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
        // Redis instance for publishing messages to BackbeatConsumers
        this._redisPublisher = new Redis(this._redisConfig);
        this._backbeatMetadataProxy =
            new BackbeatMetadataProxy(this._repConfig.source);
        // Redis instance is used for getting/setting keys
        this._redisClient = new RedisClient(this._redisConfig, this._logger);
        // Redis expiry increased by an additional interval so we can reference
        // the immediate older data for average throughput calculation
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

    _updateValidSites() {
        const bootstrapList = this._repConfig.destination.bootstrapList;
        const sites = bootstrapList.map(item => item.site);
        this._validSites = sites;
    }

    /**
     * Find valid route from list of routes defined in Arsenal backbeat routes
     * Sets the matched route on the BackbeatRequest object with possible
     * extra properties.
     * @param {BackbeatRequest} bbRequest - holds relevant data about request
     * @return {Object|null} - The error object or `null` if no error
     */
    findValidRoute(bbRequest) {
        /* eslint-disable no-param-reassign */
        const rDetails = bbRequest.getRouteDetails();
        const route = bbRequest.getRoute();
        const addKeys = {};

        this._updateValidSites();
        const routes = getRoutesFn(redisKeys, this._validSites);

        // first validate healthcheck routes or prom routes since they do not
        // have rDetails set
        if (route === 'healthcheck') {
            if (bbRequest.getHTTPMethod() !== 'GET') {
                return errors.MethodNotAllowed;
            }
            // TODO: this logic changes with addition of deep healthcheck
            const healthcheckRoute = routes.filter(r =>
                r.category === 'healthcheck')[0];
            bbRequest.setMatchedRoute(healthcheckRoute);
            return null;
        }
        if (route === 'monitoring/metrics') {
            if (bbRequest.getHTTPMethod() !== 'GET') {
                return errors.MethodNotAllowed;
            }
            const promRoute = routes.filter(r =>
                r.category === 'monitoring')[0];
            bbRequest.setMatchedRoute(promRoute);
            return null;
        }

        // All routes at this point have extensions property in use
        // Match http verb and match extension name
        let filteredRoutes = routes.filter(r => (
            bbRequest.getHTTPMethod() === r.httpMethod &&
            r.extensions[rDetails.extension]
        ));
        // if rDetails has a category property. Currently only metrics
        if (rDetails.category) {
            filteredRoutes = filteredRoutes.filter(r =>
                rDetails.category === r.category);
        }
        // if rDetails has a status property
        if (rDetails.status) {
            if (rDetails.status === 'failed') {
                const { extension, marker, bucket, key, versionId } = rDetails;
                const type = (bucket && key && versionId) ? 'specific' : 'all';
                filteredRoutes = filteredRoutes.filter(r => {
                    const extList = r.extensions[extension];
                    return extList.includes('failed') && r.type === type;
                });

                if (type === 'specific') {
                    addKeys.bucket = bucket;
                    addKeys.key = key;
                    addKeys.versionId = versionId;
                }
                if (marker) {
                    // this is optional, so doesn't matter if set or not
                    addKeys.marker = marker;
                }
            } else {
                // currently only pause/resume
                filteredRoutes = filteredRoutes.filter(r =>
                    rDetails.status === r.type);
            }
        }
        // if rDetails has a type property
        if (rDetails.type) {
            filteredRoutes = filteredRoutes.filter(r =>
                rDetails.type === r.type);
        }

        // if rDetails has a site property. Should only have 1 matched route
        // at this point, or else there is an error
        if (rDetails.site && filteredRoutes.length === 1) {
            filteredRoutes = filteredRoutes.filter(r => {
                const list = r.extensions[rDetails.extension];
                return list.includes(rDetails.site);
            });
            // add optional site
            addKeys.site = rDetails.site;
        }

        if (filteredRoutes.length !== 1) {
            return errors.RouteNotFound.customizeDescription(
                `path ${bbRequest.getRoute()} does not exist`);
        }

        // Matching route found. Set on request object so we do not have to
        // re-match later
        const matchedRoute = Object.assign({}, filteredRoutes[0], addKeys);
        bbRequest.setMatchedRoute(matchedRoute);

        return null;
        /* eslint-enable no-param-reassign */
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
     * @param {object} details - route details
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
     * @param {Object} details - route details
     * @param {Function} cb = callback(error, data)
     * @returns {undefined}
     */
    monitoringHandler(details, cb) {
        const promMetrics = monitoringClient.register.metrics();
        return cb(null, promMetrics);
    }

    /**
     * Retrieves object metadata from the source cloud server and instantiates
     * a new ObjectQueueEntry for queuing.
     * @param {ObjectFailureEntry} entry - The entry created from the Redis key
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    _getObjectQueueEntry(entry, cb) {
        const log = this._logger.newRequestLogger();
        const { error } =
            this._backbeatMetadataProxy.setupSourceRole(entry, log);
        if (error) {
            return setImmediate(() => cb(error));
        }
        const params = {
            logInfo: entry.getLogInfo(),
            bucket: entry.getBucket(),
            objectKey: entry.getObjectKey(),
            encodedVersionId: entry.getEncodedVersionId(),
        };
        return this._backbeatMetadataProxy
            .setSourceClient(log)
            .getMetadata(params, log, (err, res) => {
                if (err) {
                    this._logger.error('could not retrieve object metadata ' +
                        'from source during replication retry', { error: err });
                    return cb(err);
                }
                const bucket = entry.getBucket();
                const objectKey = entry.getObjectKey();
                const mdObj = JSON.parse(res.Body);
                const queueEntry =
                    new ObjectQueueEntry(bucket, objectKey, mdObj);
                queueEntry.setSite(entry.getSite());
                return cb(null, queueEntry);
            });
    }

    /**
     * Builds the failed CRR response.
     * @param {String} cursor - The Redis HSCAN cursor
     * @param {Array} keys - The collection of Redis keys for the iteration
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    _getFailedCRRResponse(cursor, keys, cb) {
        const response = {
            IsTruncated: Number.parseInt(cursor, 10) !== 0,
            Versions: [],
        };
        if (response.IsTruncated) {
            response.NextMarker = Number.parseInt(cursor, 10);
        }
        return async.eachLimit(keys, 10, (key, next) =>
            this._redisClient.batch([['get', key]], (err, res) => {
                if (err) {
                    return next(err);
                }
                const [cmdErr, role] = res[0];
                if (cmdErr) {
                    return next(cmdErr);
                }
                // The key did not exist.
                if (role === null) {
                    return next();
                }
                const entry = new ObjectFailureEntry(key, role);
                return this._getObjectQueueEntry(entry, (err, queueEntry) => {
                    if (err) {
                        // If we cannot retrieve the source object, then we no
                        // longer can consider it a failed operation. Delete the
                        // key used to monitor the failure.
                        return this._deleteFailedCRRKey(key, next);
                    }
                    response.Versions.push({
                        Bucket: queueEntry.getBucket(),
                        Key: queueEntry.getObjectKey(),
                        VersionId: queueEntry.getEncodedVersionId(),
                        StorageClass: entry.getSite(),
                        Size: queueEntry.getContentLength(),
                        LastModified: queueEntry.getLastModified(),
                    });
                    return next();
                });
            }), err => cb(err, response));
    }

    /**
     * Recursively scan all existing keys with a count of 1000. Call callback if
     * the response is greater or equal to 1000 keys, or we have scanned all
     * keys (i.e. when the cursor is 0).
     * @param {String} pattern - The key pattern to match
     * @param {Number} marker - The cursor to start scanning from
     * @param {Array} allKeys - The collection of all matching keys found
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    _scanAllKeys(pattern, marker, allKeys, cb) {
        const cmd = ['scan', marker, 'MATCH', pattern, 'COUNT', 1000];
        this._redisClient.batch([cmd], (err, res) => {
            if (err) {
                return cb(err);
            }
            const [cmdErr, collection] = res[0];
            if (cmdErr) {
                return cb(cmdErr);
            }
            const [cursor, keys] = collection;
            allKeys.push(...keys);
            if (allKeys.length >= 1000 || Number.parseInt(cursor, 10) === 0) {
                return cb(null, cursor, allKeys);
            }
            return this._scanAllKeys(pattern, cursor, allKeys, cb);
        });
    }

    /**
     * Find all failed CRR operations that match the bucket, key, and versionID.
     * @param {Object} details - The route details
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    getFailedCRR(details, cb) {
        const { bucket, key } = details;
        let { versionId } = details;
        // If the original CRR was on a bucket without `versioning` enabled
        // (i.e. an NFS bucket), maintain the Redis key schema by using and
        // empty string.
        versionId = versionId === undefined ? '' : versionId;
        const { failedCRR } = redisKeys;
        const pattern = `${failedCRR}:${bucket}:${key}:${versionId}:*`;
        return this._scanAllKeys(pattern, 0, [], (err, cursor, keys) =>
            this._getFailedCRRResponse(cursor, keys, cb));
    }

    /**
     * Get all CRR operations that have failed.
     * @param {Object} details - The route details
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    getAllFailedCRR(details, cb) {
        const marker = Number.parseInt(details.marker, 10) || 0;
        const pattern = `${redisKeys.failedCRR}:*`;
        return this._scanAllKeys(pattern, marker, [], (err, cursor, keys) =>
            this._getFailedCRRResponse(cursor, keys, cb));
    }

    /**
     * For the given queue entry's site, send an entry with PENDING status to
     * the replication status topic, then send an entry to the replication topic
     * so that the queue processor re-attempts replication.
     * @param {QueueEntry} queueEntry - The queue entry constructed from the
     * failed kafka entry that was stored as a Redis key value.
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
        ], err => {
            if (err) {
                this._logger.error('error pushing to kafka topics', {
                    method: 'BackbeatAPI._pushToCRRRetryKafkaTopics',
                });
            }
            return cb(err);
        });
    }

    /**
     * Delete the failed CRR Redis key.
     * @param {String} key - The key to delete
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    _deleteFailedCRRKey(key, cb) {
        const cmd = ['del', key];
        return this._redisClient.batch([cmd], (err, res) => {
            if (err) {
                this._logger.error('error deleting redis key', {
                    method: 'BackbeatAPI._deleteFailedCRRKey',
                    key,
                    error: err,
                });
                return cb(err);
            }
            const [cmdErr] = res[0];
            if (cmdErr) {
                this._logger.error('error deleting redis key', {
                    method: 'BackbeatAPI._deleteFailedCRRKey',
                    key,
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
        return async.eachLimit(entries, 10, (entry, next) =>
            // Get the object metadata from the source cloud server.
            this._getObjectQueueEntry(entry, (err, queueEntry) => {
                if (err) {
                    // If we cannot retrieve the source object, then we no
                    // longer can consider it a failed operation. Delete the
                    // key used to monitor the failure.
                    return this._deleteFailedCRRKey(entry.getRedisKey(), next);
                }
                return async.waterfall([
                    // Push Kafka entries to respective topics to initiate retry
                    done => this._pushToCRRRetryKafkaTopics(queueEntry, err => {
                        if (err) {
                            return done(err);
                        }
                        response.push({
                            Bucket: queueEntry.getBucket(),
                            Key: queueEntry.getObjectKey(),
                            VersionId: queueEntry.getEncodedVersionId(),
                            StorageClass: entry.getSite(),
                            Size: queueEntry.getContentLength(),
                            LastModified: queueEntry.getLastModified(),
                            ReplicationStatus: 'PENDING',
                        });
                        return done();
                    }),
                    // Delete the Redis key from the prior failure.
                    done => this._deleteFailedCRRKey(entry.getRedisKey(), done),
                ], next);
            }),
        err => cb(err, response));
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
        const entries = [];
        return async.eachLimit(reqBody, 10, (o, next) => {
            const { Bucket, Key, VersionId, StorageClass } = o;
            const key = getFailedCRRKey(Bucket, Key, VersionId, StorageClass);
            const cmd = ['get', key];
            return this._redisClient.batch([cmd], (err, res) => {
                if (err) {
                    return next(err);
                }
                const [cmdErr, sourceRole] = res[0];
                // If the key did not exist, value is `null`.
                if (sourceRole !== null) {
                    entries.push(new ObjectFailureEntry(key, sourceRole));
                }
                return next(cmdErr);
            });
        }, err => {
            if (err) {
                return cb(err);
            }
            return this._processFailedKafkaEntries(entries, cb);
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
            const requiredProperties = ['Bucket', 'Key', 'StorageClass'];
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
     * Pause CRR operations for given site(s)
     * @param {Object} details - The route details
     * @param {String} body - The POST request body string
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    pauseCRRService(details, body, cb) {
        let sites;
        if (details.site === 'all') {
            sites = details.extensions.crr.filter(s => s !== 'all');
        } else {
            sites = [details.site];
        }
        sites.forEach(site => {
            const channel = `${this._crrTopic}-${site}`;
            const message = JSON.stringify({ action: 'pauseService' });
            this._redisPublisher.publish(channel, message);
        });
        this._logger.info(`replication service paused for locations: ${sites}`);
        return cb(null, {});
    }

    /**
     * Resume CRR operations for given site(s)
     * @param {Object} details - The route details
     * @param {String} body - The POST request body string
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    resumeCRRService(details, body, cb) {
        let sites;
        if (details.site === 'all') {
            sites = details.extensions.crr.filter(s => s !== 'all');
        } else {
            sites = [details.site];
        }
        sites.forEach(site => {
            const channel = `${this._crrTopic}-${site}`;
            const message = JSON.stringify({ action: 'resumeService' });
            this._redisPublisher.publish(channel, message);
        });
        this._logger.info(`replication service resumed for locations ${sites}`);
        return cb(null, {});
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
