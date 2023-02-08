'use strict'; // eslint-disable-line strict

const async = require('async');
const Redis = require('ioredis');
const { MongoClient } = require('mongodb');

const { errors } = require('arsenal');
const { RedisClient, StatsModel, ZenkoMetrics } = require('arsenal').metrics;

const zookeeper = require('../clients/zookeeper');
const BackbeatProducer = require('../BackbeatProducer');
const ObjectQueueEntry =
    require('../../lib/models/ObjectQueueEntry');
const ObjectFailureEntry =
    require('../../extensions/replication/utils/ObjectFailureEntry');
const BackbeatMetadataProxy = require('../BackbeatMetadataProxy');
const Healthcheck = require('./Healthcheck');
const { getSortedSetKey, getSortedSetMember } =
    require('../util/sortedSetHelper');
const Metrics = require('./Metrics');
const getRoutesFn = require('./routes');
const locations = require('../../conf/locationConfig.json') || {};
const { constructConnectionString } = require('../../extensions/utils/MongoUtils');
const LocationStatusManager = require('../util/LocationStatusManager');

const {
    redisKeys: crrRedisKeys,
    zookeeperNamespace: zookeeperReplicationNamespace,
    zkStatePath: zkReplicationStatePath,
} = require('../../extensions/replication/constants');
const {
    redisKeys: ingestionRedisKeys,
    zookeeperNamespace: zookeeperIngestionNamespace,
    zkStatePath: zkIngestionStatePath,
} = require('../../extensions/ingestion/constants');

const { applyBucketReplicationWorkflows } = require('../../extensions/replication/management');
const { applyBucketLifecycleWorkflows } = require('../../extensions/lifecycle/management');
const { createServiceState } = require('../../lib/management');

// StatsClient constant defaults
// TODO: This should be moved to constants file
const INTERVAL = 300; // 5 minutes
const EXPIRY = 86400; // 24 hours.

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
        this._config = config;
        this._zkConfig = config.zookeeper;
        this._kafkaConfig = config.kafka;
        this._repConfig = config.extensions.replication;
        this._ingestionConfig = config.extensions.ingestion;
        this._lifecycleConfig = config.extensions.lifecycle;
        this._queuePopulator = config.queuePopulator;
        this._kafkaHost = config.kafka.hosts;
        this._redisConfig = config.redis;
        this._logger = logger;

        // topics
        this._crrTopic = this._repConfig.topic;
        this._crrStatusTopic = this._repConfig.replicationStatusTopic;
        this._ingestionTopic = this._ingestionConfig.topic;
        this._metricsTopic = config.metrics.topic;

        this._updateConfigSites();

        this._crrProducer = null;
        this._crrStatusProducer = null;
        this._metricProducer = null;
        this._healthcheck = null;
        this._zkClient = null;

        this._lifecycleSites = [];

        this._mongoClient = null;
        this._locationStatusManager = null;

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
        const { transport, s3, auth } = this._repConfig.source;
        this._backbeatMetadataProxy = new BackbeatMetadataProxy(
            `${transport}://${s3.host}:${s3.port}`, auth);
        // Redis instance is used for getting/setting keys
        this._redisClient = new RedisClient(this._redisConfig, this._logger);
        // Redis expiry increased by an additional interval so we can reference
        // the immediate older data for average throughput calculation
        this._statsClient = new StatsModel(this._redisClient, INTERVAL, EXPIRY);
        const metricsConfig = {
            redisConfig: this._redisConfig,
            crrSites: this._crrSites,
            ingestionSites: this._ingestionSites,
            internalStart: this._internalStart,
        };
        const metrics = new Metrics(metricsConfig, logger);
        Object.assign(this, {
            _queryStats: metrics._queryStats,
            _getData: metrics._getData,
            _hasGlobalKey: metrics._hasGlobalKey,
            _getMaxUptime: metrics._getMaxUptime,
            getBacklog: metrics.getBacklog,
            getCompletions: metrics.getCompletions,
            _sendCompletionsResponseReplication:
                metrics._sendCompletionsResponseReplication,
            _sendCompletionsResponseIngestion:
                metrics._sendCompletionsResponseIngestion,
            getObjectThroughput: metrics.getObjectThroughput,
            getObjectProgress: metrics.getObjectProgress,
            getThroughput: metrics.getThroughput,
            _sendThroughputResponseReplication:
                metrics._sendThroughputResponseReplication,
            _sendThroughputResponseIngestion:
                metrics._sendThroughputResponseIngestion,
            getAllMetrics: metrics.getAllMetrics,
            getPending: metrics.getPending,
            _sendPendingResponseReplication:
                metrics._sendPendingResponseReplication,
            _sendPendingResponseIngestion:
                metrics._sendPendingResponseIngestion,
        });
    }

    /**
     * Validate possible query strings.
     * @param {BackbeatRequest} bbRequest - Relevant data about request
     * @return {Object|null} - The error object or `null` if no error
     */
    validateQuery(bbRequest) {
        const { marker, sitename } = bbRequest.getRouteDetails();
        if (marker !== undefined && Number.isNaN(Number.parseInt(marker, 10))) {
            return errors.InvalidQueryParameter
                .customizeDescription('marker must be a number');
        }
        if (sitename !== undefined && sitename === '') {
            return errors.InvalidQueryParameter
                .customizeDescription('must be a non-empty string');
        }
        return null;
    }

    /**
     * update valid sites per service (i.e. replication, ingestion)
     * @return {undefined}
     */
    _updateConfigSites() {
        const bootstrapList = this._repConfig.destination.bootstrapList;
        const sites = bootstrapList.map(item => item.site);
        const ingestionBuckets = this._config.getIngestionBuckets();
        const allSites = Object.keys(locations);

        this._crrSites = sites;
        this._lifecycleSites = allSites;
        this._ingestionSites =
            [...new Set(ingestionBuckets.map(b => b.locationConstraint))];
    }

    /**
     * Maps the data points to their respective redis keys
     * @param {String} service - service name, i.e. 'crr', 'ingestion'
     * @param {Array} dataPoints - array of strings
     * @return {Array} array of redis keys
     */
    _getRedisKeySchemas(service, dataPoints) {
        if (service === 'ingestion') {
            return dataPoints.map(dp => ingestionRedisKeys[dp]);
        }
        if (service === 'crr') {
            return dataPoints.map(dp => crrRedisKeys[dp]);
        }
        return [];
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

        this._updateConfigSites();
        const routes = getRoutesFn({
            crr: this._crrSites,
            ingestion: this._ingestionSites,
            lifecycle: this._lifecycleSites,
        });

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
            bbRequest.setContentType(ZenkoMetrics.asPrometheusContentType());
            return null;
        }
        if (route === 'configuration/workflows') {
            if (bbRequest.getHTTPMethod() !== 'POST') {
                return errors.MethodNotAllowed;
            }
            const configRoute = routes.filter(r =>
                r.category === 'configuration')[0];
            bbRequest.setMatchedRoute(configRoute);
            bbRequest.setContentType('application/json');
            return null;
        }

        // If giving the bucket name, the user must also provide an object key
        // and version ID.
        const hasGranularity = rDetails.bucketName ?
            rDetails.objectKey && rDetails.versionId : true;
        if (!hasGranularity) {
            return errors.RouteNotFound.customizeDescription(
                'must provide object key and version ID in route: ' +
                `${bbRequest.getRoute()}`);
        }

        // All routes at this point have extensions property in use
        // Match http verb and match extension name
        let filteredRoutes = routes.filter(r => {
            const isMatchingMethod = bbRequest.getHTTPMethod() === r.httpMethod;
            const isMatchingExtension = r.extensions[rDetails.extension];
            const isMatchingLevel = rDetails.objectKey ?
                r.level === 'object' :
                r.level === 'site' || r.level === undefined;
            return isMatchingMethod && isMatchingExtension && isMatchingLevel;
        });

        // if rDetails has an extension property, i.e. 'crr', 'ingestion', 'lifecycle'
        if (rDetails.extension) {
            addKeys.service = rDetails.extension;
        }
        // if rDetails has a category property. Currently only metrics
        if (rDetails.category) {
            filteredRoutes = filteredRoutes.filter(r =>
                rDetails.category === r.category);
        }
        // if rDetails has a status property
        if (rDetails.status) {
            if (rDetails.status === 'failed') {
                const { extension, marker, sitename, bucket, key, versionId } =
                    rDetails;
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
                if (sitename) {
                    // this is optional for backward compatibility
                    addKeys.sitename = sitename;
                }
            } else {
                // currently only pause/resume (for both crr and ingestion)
                filteredRoutes = filteredRoutes.filter(r =>
                    rDetails.status === r.type);
                if (rDetails.schedule) {
                    addKeys.schedule = rDetails.schedule;
                }
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

        if (rDetails.versionId) {
            addKeys.objectKey = rDetails.objectKey;
            addKeys.bucketName = rDetails.bucketName;
            addKeys.versionId = rDetails.versionId;
        }

        const expectedRoute = filteredRoutes[0];
        // convert dataPoint to relevant redis keys
        if (expectedRoute.dataPoints) {
            const dataPointKeys = this._getRedisKeySchemas(rDetails.extension,
                expectedRoute.dataPoints);
            if (dataPointKeys.length === 0) {
                // this would be a developer error, either wrong redis key map
                // or missing constants in redis keys, so return InternalError
                this._logger.error('error getting metrics redis keys', {
                    path: bbRequest.getRoute(),
                    dataPoints: expectedRoute.dataPoints,
                    extension: rDetails.extension,
                });
                return errors.InternalError.customizeDescription(
                    `error for ${bbRequest.getRoute()}, check error logs`);
            }
            expectedRoute.dataPoints = dataPointKeys;
        }

        // Matching route found. Set on request object so we do not have to
        // re-match later
        const matchedRoute = Object.assign({}, expectedRoute, addKeys);
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
        return this._healthcheck.getHealthcheck(this._logger, cb);
    }

    /**
     * Collects metrics from Prometheus variables and register for response
     * @param {Object} details - route details
     * @param {Function} cb = callback(error, data)
     * @returns {undefined}
     */
    monitoringHandler(details, cb) {
        const promMetrics = ZenkoMetrics.asPrometheus();
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
        const params = {
            bucket: entry.getBucket(),
            objectKey: entry.getObjectKey(),
            versionId: entry.getEncodedVersionId(),
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
     * @param {Number} nextMarkerScore - The score used for the NextMarker in
     * the response
     * @param {Array} entries - Array of ObjectFailureEntry instances
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    _getFailedCRRResponse(nextMarkerScore, entries, cb) {
        const filteredEntries = this._filterFailureEntries(entries);
        return async.mapLimit(filteredEntries, 10, (entry, next) =>
            this._getObjectQueueEntry(entry, (err, queueEntry) => {
                if (err) {
                    // If we cannot retrieve the source object, then we no
                    // longer can consider it a failed operation. Skip the
                    // retry and filter it in the response.
                    return next();
                }
                return next(null, {
                    Bucket: queueEntry.getBucket(),
                    Key: queueEntry.getObjectKey(),
                    VersionId: queueEntry.getEncodedVersionId(),
                    StorageClass: entry.getSite(),
                    Size: queueEntry.getContentLength(),
                    LastModified: queueEntry.getLastModified(),
                });
            }),
        (err, results) => {
            if (err) {
                return cb(err);
            }
            const response = {
                IsTruncated: nextMarkerScore !== undefined,
                Versions: results.filter(result => (result !== undefined)),
            };
            if (response.IsTruncated) {
                response.NextMarker = Number.parseInt(nextMarkerScore, 10);
            }
            return cb(null, response);
        });
    }

    /**
     * Helper method to get the sorted set keys by the given site.
     * @param {String} site - Site to get sorted set keys for
     * @return {Array} - The sorted set keys
     */
    _getSortedSetKeys(site) {
        const timestamps = this._statsClient.getSortedSetHours(Date.now());
        return timestamps.map(timestamp => getSortedSetKey(site, timestamp));
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
        const member = getSortedSetMember(bucket, key, versionId);
        return this._getEntriesAcrossSites(member, (err, entries) =>
            this._getFailedCRRResponse(undefined, entries, cb));
    }

    /**
     * Get all CRR operations that have failed by site.
     * @param {Object} details - The route details
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    getSiteFailedCRR(details, cb) {
        const { marker, sitename } = details;
        const score = Number.parseInt(marker, 10) || 0;
        const keys = this._getSortedSetKeys(sitename);
        return this._getEntriesBySite(score, sitename, keys,
            (err, nextMarker, entries) =>
                this._getFailedCRRResponse(nextMarker, entries, cb));
    }

    /**
     * Get all entries across all site sorted sets.
     * @param {String} member - The member in the sorted set to find
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _getEntriesAcrossSites(member, cb) {
        const entries = [];
        return async.eachLimit(this._crrSites, 10, (site, next) => {
            const keys = this._getSortedSetKeys(site);
            return async.eachLimit(keys, 10, (key, done) => {
                this._redisClient.zscore(key, member, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    if (res !== null) {
                        const entry = new ObjectFailureEntry(member, site);
                        entries.push(entry);
                    }
                    return done();
                });
            }, next);
        }, err => {
            if (err) {
                return cb(err);
            }
            return cb(null, entries);
        });
    }

    /**
     * Use the current score to find all sorted sets that are >= to the score.
     * @param {Number} score - The score to use for reduction
     * @param {Array} keys - All the possible site keys
     * @return {Array} - The reduced ke
     */
    _reduceSortedSetKeys(score, keys) {
        const date = new Date(score);
        const startHour = this._statsClient.normalizeTimestampByHour(date);
        const index = keys.findIndex(key => key.includes(startHour));
        return index < 0 ? [] : keys.slice(index);
    }

    /**
     * Get all entries by site sorted sets.
     * @param {Number} marker - The score to begin a listing from
     * @param {String} sitename - The site name to get failures by
     * @param {Array} allKeys - All the possible site sorted set keys
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    _getEntriesBySite(marker, sitename, allKeys, cb) {
        let score = marker;
        const keys = score ?
            this._reduceSortedSetKeys(score, allKeys) : allKeys;
        if (keys.length === 0) {
            // If a given marker did not match a sorted set key.
            return process.nextTick(() => cb(null, undefined, []));
        }
        const listingLimit = 100; // Don't exceed 100 entries in the response.
        const entries = [];
        let nextMarker;
        let shouldContinue;
        let i = 0;
        return async.doWhilst(next => {
            const key = keys[i++];
            async.waterfall([
                done => {
                    if (score) {
                        // If the request included a next marker we want to list
                        // the sorted set from the member with that score.
                        return this._redisClient
                            .zrangebyscore(key, '-inf', score, (err, res) => {
                                if (err) {
                                    return done(err);
                                }
                                // Don't use the score in subsequent sorted set
                                // range requests.
                                score = undefined;
                                return done(null, res);
                            });
                    }
                    return this._redisClient.zrange(key, 0, -1, done);
                },
                (results, done) => {
                    const totalMembers = results.length + entries.length;
                    if (totalMembers >= listingLimit) {
                        const index = totalMembers - listingLimit;
                        const member = results[index - 1];
                        if (member === undefined) {
                            // If the sorted set doesn't include the final
                            // member who's score will be used as the next
                            // marker, go on to check subsequent sorted sets.
                            return done(null, results, index, false);
                        }
                        return this._redisClient.zscore(key, member,
                            (err, score) => {
                                if (err) {
                                    return done(err);
                                }
                                nextMarker = score;
                                return done(null, results, index, true);
                            });
                    }
                    return done(null, results, undefined, true);
                },
                (results, index, hasLastMember, done) => {
                    const slice = results.slice(index);
                    slice.forEach(member =>
                        entries.push(new ObjectFailureEntry(member, sitename)));
                    // If the listing result didn't include the last member, we
                    // should continue checking if there is one in subsequent
                    // sorted sets to retrieve a next marker value. Otherwise we
                    // only continue if this isn't the last sorted set to check.
                    shouldContinue = hasLastMember ?
                        i < keys.length && entries.length < listingLimit :
                        i < keys.length;
                    return done();
                },
            ], next);
        }, () => shouldContinue, err => cb(err, nextMarker, entries));
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
                return cb(err);
            }
            const opsPendingKey = `${site}:${crrRedisKeys.opsPending}`;
            const bytesPendingkey = `${site}:${crrRedisKeys.bytesPending}`;
            const bytes = queueEntry.getContentLength();
            this._statsClient.incrementKey(opsPendingKey, 1);
            this._statsClient.incrementKey(bytesPendingkey, bytes);
            return cb();
        });
    }

    /**
     * Delete the given member from a site sorted set.
     * @param {String} member - The sorted set member to delete
     * @param {String} site - The site name
     * @param {Function} cb - Callback to call
     * @return {undefined}
     */
    _findAndDeleteMember(member, site, cb) {
        const keys = this._getSortedSetKeys(site);
        let i = 0;
        let shouldContinue = true;
        return async.doWhilst(next => {
            const key = keys[i++];
            this._redisClient.zscore(key, member, (err, res) => {
                if (err) {
                    return next(err);
                }
                if (res !== null) {
                    shouldContinue = false;
                    return this._redisClient.zrem(key, member, next);
                }
                return next();
            });
        }, () => (shouldContinue && i < keys.length), cb);
    }

    /**
     * Delete a Redis key.
     * @param {String} key - The key to delete
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    _deleteKey(key, cb) {
        const cmd = ['del', key];
        return this._redisClient.batch([cmd], (err, res) => {
            if (err) {
                this._logger.error('error deleting redis key', {
                    method: 'BackbeatAPI._deleteKey',
                    key,
                    error: err,
                });
                return cb(err);
            }
            const [cmdErr] = res[0];
            if (cmdErr) {
                this._logger.error('error deleting redis key', {
                    method: 'BackbeatAPI._deleteKey',
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
        return async.eachLimit(entries, 10, (entry, next) => {
            const { key, member, StorageClass } = entry;
            const failureEntry = new ObjectFailureEntry(member, StorageClass);
            // Get the object metadata from the source cloud server.
            return this._getObjectQueueEntry(failureEntry,
            (err, queueEntry) => {
                if (err) {
                    // If we cannot retrieve the source object, then we no
                    // longer can consider it a failed operation. Delete the
                    // member used to monitor the failure.
                    if (key) {
                        return this._redisClient.zrem(key, member, next);
                    }
                    // If it is a forced retry we do not remove the member.
                    return next();
                }
                const site = failureEntry.getSite();
                const contentLength = queueEntry.getContentLength();
                return async.series([
                    // Push Kafka entries to respective topics to initiate retry
                    done => this._pushToCRRRetryKafkaTopics(queueEntry, err => {
                        if (err) {
                            return done(err);
                        }
                        const responseObject = {
                            Bucket: queueEntry.getBucket(),
                            Key: queueEntry.getObjectKey(),
                            VersionId: queueEntry.getEncodedVersionId(),
                            StorageClass: site,
                            Size: contentLength,
                            LastModified: queueEntry.getLastModified(),
                            ReplicationStatus: 'PENDING',
                        };
                        if (this._repConfig.source.auth.type === 'role') {
                            responseObject.Role =
                                queueEntry.getReplicationRoles().split(',')[0];
                        }
                        response.push(responseObject);
                        return done();
                    }),
                    // Delete the Redis member from the prior failure.
                    done => {
                        if (key) {
                            return this._redisClient.zrem(key, member, done);
                        }
                        // If it is a forced retry we do not remove the member.
                        return done();
                    },
                    // Delete the Redis lock key
                    done => {
                        const bucket = failureEntry.getBucket();
                        const key = failureEntry.getObjectKey();
                        const versionId = failureEntry.getEncodedVersionId();
                        const storageClass = failureEntry.getSite();
                        const lockKey = `${bucket}:${key}:${versionId}:` +
                            `${storageClass}:processingFailures`;
                        return this._deleteKey(lockKey, done);
                    },
                ], next);
            });
        },
        err => cb(err, response));
    }

    /**
     * Tempfix to correct failure/pending metrics on concurrency issue
     * @param {array} entries - an array of redis failure entries to remove.
     *   Schema:
     *   bb:crr:failed:<bucket>:<key>:<versionID>:<storageClass>:<timestamp>
     * @return {undefined}
     */
    _normalizeFailEntries(entries) {
        async.eachLimit(entries, 10, (entry, next) =>
            async.waterfall([
                done => this._getObjectQueueEntry(entry, done),
                (queueEntry, done) => {
                    const member = entry.getMember();
                    const site = entry.getSite();
                    this._findAndDeleteMember(member, site, err => {
                        if (err) {
                            return done(err);
                        }
                        return done(null, queueEntry);
                    });
                },
            ], (err, queueEntry) => {
                if (err) {
                    this._logger.debug('failed to delete duplicate object ' +
                    'fail key', {
                        method: 'BackbeatAPI._normalizeFailMetrics',
                        error: err,
                    });
                    return next();
                }
                const contentLength = queueEntry.getContentLength();
                const site = entry.getSite();
                const opsPendingKey = `${site}:${crrRedisKeys.opsPending}`;
                const bytesPendingkey = `${site}:${crrRedisKeys.bytesPending}`;
                // decrement key as a duplicate failure object key was found.
                // If removing a duplicate failure object key, we also need to
                // normalize pending metrics by decrementing
                this._statsClient.decrementKey(opsPendingKey, 1);
                this._statsClient.decrementKey(bytesPendingkey, contentLength);
                return next();
            }));
    }

    /* eslint-disable */
    /**
     * Use failed object sorted set lists to get failure metrics
     * @param {object} details - route details from lib/backbeat/routes.js
     * @param {function} cb - callback(error, data)
     * @param {array} data - optional field providing already fetched data in
     *   order of dataPoints mentioned for each route in lib/backbeat/routes.js
     * @return {undefined}
     */
    getFailedMetrics(details, cb, data) {
        const { failedCRR } = crrRedisKeys;
        const sites = details.site === 'all' ?
            this._crrSites : [details.site];
        const hourTimestamps = this._statsClient.getSortedSetHours(Date.now());
        // reduce by hourly timestamp
        return async.reduce(hourTimestamps, 0, (totalSum, timestamp, done) => {
            const keys = sites.map(s => `${failedCRR}:${s}:${timestamp}`);
            // reduce by each site for a given hour
            return async.reduce(keys, 0, (hourlySum, key, next) => {
                return this._redisClient.zcard(key, (err, count) => {
                    if (err) {
                        this._logger.error('error on Redis zcard', {
                            error: err,
                            method: 'BackbeatAPI.getFailedMetrics',
                        });
                        return next(err);
                    }
                    return next(null, hourlySum + count);
                });
            }, (err, hourlyTotal) => {
                if (err) {
                    return done(err);
                }
                return done(null, totalSum + hourlyTotal);
            });
        }, (err, totalCount) => {
            if (err) {
                this._logger.error('error getting metrics: failures', {
                    error: err,
                    method: 'BackbeatAPI.getFailedMetrics',
                });
                return cb(errors.InternalError);
            }
            // TODO: for now, not returning failures bytes. Instead, rely on
            // retry to return obj sizes
            const bytesFail = 0;
            const uptime = this._getMaxUptime(EXPIRY);
            const response = {
                failures: {
                    description: 'Number of failed replication operations ' +
                        '(count) and bytes (size) in the last ' +
                        `${Math.floor(uptime)} seconds`,
                    results: {
                        count: totalCount,
                        size: bytesFail,
                    },
                },
            };
            return cb(null, response);
        });
    }
    /* eslint-enable */

    /**
     * Filter the failed entries retrieved from the sorted set.
     * @param {Array} entries - An array of ObjectFailureEntry instances
     * @return {Array} The Array of filtered entries
     */
    _filterFailureEntries(entries) {
        const filteredEntries = [];
        const uniqueObjs = {};
        const duplicateEntries = [];
        entries.forEach(entry => {
            const bucket = entry.getBucket();
            const objectKey = entry.getObjectKey();
            const encodedVersionId = entry.getEncodedVersionId();
            const site = entry.getSite();
            const hash = `${bucket}:${objectKey}:${encodedVersionId}:${site}`;
            // if the key already exists, debug log because an error occurred
            // somewhere
            if (uniqueObjs[hash]) {
                this._logger.debug('duplicate version id was found in failed ' +
                'object redis members', {
                    method: 'BackbeatAPI._filterFailureEntries',
                });
                duplicateEntries.push(entry);
            } else {
                uniqueObjs[hash] = true;
                filteredEntries.push(entry);
            }
        });
        // attempt to remove without blocking request
        this._normalizeFailEntries(duplicateEntries);
        return filteredEntries;
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
            const { Bucket, Key, VersionId, StorageClass, ForceRetry } = o;
            const member = getSortedSetMember(Bucket, Key, VersionId);
            // The `ForceRetry` field allows for queueing a retry regardless of
            // whether it is failed.
            if (ForceRetry) {
                const lockKey = `${Bucket}:${Key}:${VersionId}:` +
                    `${StorageClass}:processingFailures`;
                return this._applyRetryLockKey(lockKey, (err, lockApplied) => {
                    if (err) {
                        return next(err);
                    }
                    if (lockApplied) {
                        entries.push({ member, StorageClass });
                    }
                    return next();
                });
            }
            const keys = this._getSortedSetKeys(StorageClass);
            return async.eachLimit(keys, 10, (key, done) => {
                this._redisClient.zscore(key, member, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    if (res) {
                        const lockKey = `${Bucket}:${Key}:${VersionId}:` +
                            `${StorageClass}:processingFailures`;
                        return this._applyRetryLockKey(lockKey,
                            (err, lockApplied) => {
                                if (err) {
                                    return done(err);
                                }
                                if (lockApplied) {
                                    entries.push({ key, member, StorageClass });
                                }
                                return done();
                            });
                    }
                    return done();
                });
            }, next);
        }, err => {
            if (err) {
                return cb(err);
            }
            return this._processFailedKafkaEntries(entries, cb);
        });
    }

    /**
     * Apply a retry lock for a given request if none already applied
     * @param {String} key - lock key
     * @param {Function} cb - callback(err, boolean)
     *   Where if boolean is true, a lock has been applied
     *   Where if boolean is false, a lock already exists
     * @return {undefined}
     */
    _applyRetryLockKey(key, cb) {
        this._redisClient.batch([['get', key]], (err, res) => {
            if (err) {
                return cb(err);
            }
            const [cmdErr, lock] = res[0];
            if (cmdErr) {
                return cb(cmdErr);
            }
            if (!lock) {
                // add lock and continue processing
                return this._redisClient.batch([['set', key, 'true']], err => {
                    if (err) {
                        return cb(err);
                    }
                    return cb(null, true);
                });
            }
            // lock already exists
            return cb(null, false);
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
        // Bucket, Key, VersionId, StorageClass
        // filter duplicate version ids if any
        const uniqueObjs = {};
        reqBody = reqBody.filter(entry => {
            const { Bucket, Key, VersionId, StorageClass } = entry;
            const hash = `${Bucket}:${Key}:${VersionId}:${StorageClass}`;
            if (!uniqueObjs[hash]) {
                uniqueObjs[hash] = entry;
                return true;
            }
            return false;
        });

        return { reqBody };
    }

    /**
     * Get list of requested locations
     * @param {Object} details request details
     * @param {Object} details.extensions object containing locations per service
     * @param {string[]} details.extensions.crr replication locations
     * @param {string[]} details.extensions.ingetsion ingestion locations
     * @param {string[]} details.extensions.lifecycle lifecycle locations
     * @returns {string[]} list of site names
     */
    getRequestedSites(details) {
        if (details.site === 'all') {
            return details.extensions[details.service].filter(s => s !== 'all');
        }
        return [details.site];
    }

    /**
     * Pause operations for given site(s)
     * @param {Object} details - The route details
     * @param {String} body - The POST request body string
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    pauseService(details, body, cb) {
        const service = details.service;
        const locations = this.getRequestedSites(details);
        return this._locationStatusManager.pauseService(service, locations, cb);
    }

    /**
     * Resume operations for given site(s)
     * @param {Object} details - The route details
     * @param {String} body - The POST request body string
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    resumeService(details, body, cb) {
        const service = details.service;
        const sites = this.getRequestedSites(details);
        return this._locationStatusManager.resumeService(service, sites, details.schedule, body, cb);
    }

    /**
     * Remove scheduled resume for given site(s)
     * @param {Object} details - The route details
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    deleteScheduledResumeService(details, cb) {
        const service = details.service;
        const sites = this.getRequestedSites(details);
        return this._locationStatusManager.deleteScheduledResumeService(service, sites, cb);
    }

    /**
     * Get Scheduled Resume jobs
     * @param {Object} details - The route details
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    getResumeSchedule(details, cb) {
        const service = details.service;
        const sites = this.getRequestedSites(details);
        return this._locationStatusManager.getResumeSchedule(service, sites, cb);
    }

    /**
     * Get Service Status (Pause/Resume)
     * @param {Object} details - The route details
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    getServiceStatus(details, cb) {
        const service = details.service;
        const sites = this.getRequestedSites(details);
        return this._locationStatusManager.getServiceStatus(service, sites, cb);
    }

    /**
     * Apply Workflow configuration
     * @param {Object} details - The route details
     * @param {String} body - The POST request body string
     * @param {Function} cb - The callback to call
     * @return {undefined}
     */
    applyWorkflowConfiguration(details, body, cb) {
        let overlay;
        try {
            overlay = JSON.parse(body);
        } catch (e) {
            const err = errors.MalformedPOSTRequest.customizeDescription(
                'invalid JSON');
            return cb(err);
        }
        this._logger.debug('applying, parsed json', { overlay });

        const serviceStateJobs = [
            {
                serviceName: 'replication',
                serviceAccount: this._repConfig.source.auth.account,
                applyBucketWorkflows: applyBucketReplicationWorkflows,
            },
            {
                serviceName: 'lifecycle',
                serviceAccount: this._lifecycleConfig.auth.account,
                applyBucketWorkflows: applyBucketLifecycleWorkflows,
            },
        ];

        return async.each(
            serviceStateJobs,
            (jobParams, done) => {
                this._logger.info('applying', { jobParams });
                const serviceState = createServiceState(jobParams.serviceName, this._zkClient);
                serviceState.apply(overlay, jobParams, done);
            },
            err => {
                if (err) {
                    this._logger.error('could not apply service state', {
                        error: err,
                    });
                    return cb(errors.InternalError, {});
                }
                return cb(null, {});
            }
        );
    }

    /**
     * Setup internals
     * @param {function} cb - callback(error)
     * @return {undefined}
     */
    setupInternals(cb) {
        async.parallel([
            done => this._setZookeeper(done),
            done => this._setupMongoClient(done),
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
                this._logger.error('error setting up internal clients', {
                    method: 'BackbeatAPI.setupInternals',
                    error: err.message,
                });
                return cb(err);
            }
            this._healthcheck = new Healthcheck(this._repConfig, this._zkClient,
                this._crrProducer, this._crrStatusProducer,
                this._metricProducer);
            this._locationStatusManager = new LocationStatusManager(
                this._mongoClient,
                this._zkClient,
                this._redisPublisher,
                {
                    crr: {
                        namespace: zookeeperReplicationNamespace,
                        statePath: zkReplicationStatePath,
                        topic: this._crrTopic,
                        isMongo: false,
                    },
                    ingestion: {
                        namespace: zookeeperIngestionNamespace,
                        statePath: zkIngestionStatePath,
                        topic: this._ingestionTopic,
                        isMongo: false,
                    },
                    lifecycle: {
                        isMongo: true,
                    }
                },
                this._logger,
            );
            return this._locationStatusManager._setupLocationStatusStore(err => {
                if (err) {
                    this._logger.error('error setting up location status manager', {
                        method: 'BackbeatAPI.setupInternals',
                        error: err.message,
                    });
                    return cb(err);
                }
                this._logger.info('BackbeatAPI setup ready');
                return cb();
            });
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
        const { connectionString, autoCreateNamespace } = this._zkConfig;
        const zkClient = zookeeper.createClient(connectionString, {
            autoCreateNamespace,
        });
        zkClient.connect();

        zkClient.once('error', cb);
        zkClient.once('connected', () => {
            zkClient.removeAllListeners('error');
            this._zkClient = zkClient;
            return cb();
        });
    }

    /**
     * Connects to MongoDB using and retreives
     * the location status collection
     * @param {function} cb callback
     * @returns {undefined} undefined
     */
    _setupMongoClient(cb) {
        const mongoConfig = this._config.queuePopulator.mongo;
        const mongoUrl = constructConnectionString(mongoConfig);
        MongoClient.connect(mongoUrl, {
                replicaSet: mongoConfig.replicaSet,
                useNewUrlParser: true,
                useUnifiedTopology: true,
        }, (err, client) => {
            if (err) {
                this._logger.error('Could not connect to MongoDB', {
                    method: 'BackbeatAPI._setupMongoClient',
                    error: err.message,
                });
                return cb(err);
            }
            // connect to metadata DB
            this._mongoClient = client.db(mongoConfig.database, {
                ignoreUndefined: true,
            });
            this._logger.info('Connected to MongoDB', {
                method: 'BackbeatAPI._setupMongoClient',
            });
            return cb();
        });
    }
}

module.exports = BackbeatAPI;
