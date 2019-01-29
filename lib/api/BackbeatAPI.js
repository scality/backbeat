'use strict'; // eslint-disable-line strict

const async = require('async');
const zookeeper = require('node-zookeeper-client');

const { errors } = require('arsenal');
const { RedisClient } = require('arsenal').metrics;

const { StatsModel } = require('arsenal').metrics;
const BackbeatProducer = require('../BackbeatProducer');
const ObjectQueueEntry =
    require('../../extensions/replication/utils/ObjectQueueEntry');
const ObjectFailureEntry =
    require('../../extensions/replication/utils/ObjectFailureEntry');
const BackbeatMetadataProxy =
    require('../../extensions/replication/utils/BackbeatMetadataProxy');
const Healthcheck = require('./Healthcheck');
const routes = require('./routes');
const { getSortedSetKey, getSortedSetMember } =
    require('../util/sortedSetHelper');

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
        this._backbeatMetadataProxy =
            new BackbeatMetadataProxy(this._repConfig.source);
        // Redis instance is used for getting/setting keys
        this._redisClient = new RedisClient(this._redisConfig, this._logger);
        // Redis expiry increased by an additional interval so we can reference
        // the immediate older data for average throughput calculation
        this._statsClient = new StatsModel(this._redisClient, INTERVAL,
            (EXPIRY + INTERVAL));
    }

    /**
     * Validate possible query strings.
     * @param {BackbeatRequest} bbRequest - Relevant data about request
     * @return {Object|null} - The error object or `null` if no error
     */
    validateQuery(bbRequest) {
        const { marker, sitename, role } = bbRequest.getRouteDetails();
        if (marker !== undefined && (marker === '' || isNaN(marker))) {
            return errors.InvalidQueryParameter
                .customizeDescription('marker must be a number');
        }
        if (sitename !== undefined && sitename === '') {
            return errors.InvalidQueryParameter
                .customizeDescription('sitename must be a non-empty string');
        }
        if (role !== undefined && role === '') {
            return errors.InvalidQueryParameter
                .customizeDescription('role must be a non-empty string');
        }
        return null;
    }

    /**
     * Find valid route from list of routes
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

        // if rDetails has a category property. Currently only metrics
        if (rDetails.category) {
            filteredRoutes = filteredRoutes.filter(r =>
                rDetails.category === r.category);
        }
        // if rDetails has a status property
        if (rDetails.status) {
            if (rDetails.status === 'failed') {
                const { extension, marker, sitename, bucket, key, versionId,
                    role } = rDetails;
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
                if (role) {
                    // this is optional, and only applies for development/7.4
                    addKeys.role = role;
                }
            }
        }
        // if rDetails has a type property
        if (rDetails.type) {
            filteredRoutes = filteredRoutes.filter(r =>
                rDetails.type === r.type);
        }

        // TODO: site level metrics is not currently supported in 7.4
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
            const site = details.site;
            return this._queryStats(dataPoints, site, cb);
        }
        return cb(null, data);
    }

    /**
     * Get replication backlog in ops count and size in bytes
     * @param {object} details - route details from lib/api/routes.js
     * @param {function} cb - callback(error, data)
     * @param {array} data - optional field providing already fetched data
     *   in order of dataPoints mentioned for each route in lib/api/routes.js
     * @return {undefined}
     */
    getBacklog(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting metrics: backlog', {
                    method: 'BackbeatAPI.getBacklog',
                });
                return cb(errors.InternalError);
            }
            const d = res.map(r => (
                r.requests.slice(0, 3).reduce((acc, i) => acc + i)
            ));

            let opsBacklog = d[0] - d[1];
            if (opsBacklog < 0) opsBacklog = 0;
            let bytesBacklog = d[2] - d[3];
            if (bytesBacklog < 0) bytesBacklog = 0;
            const response = {
                backlog: {
                    description: 'Number of incomplete replication ' +
                        'operations (count) and number of incomplete bytes ' +
                        'transferred (size)',
                    results: {
                        count: opsBacklog,
                        size: bytesBacklog,
                    },
                },
            };
            return cb(null, response);
        });
    }

    /**
     * Get completed replicated stats by ops count and size in bytes
     * @param {object} details - route details from lib/api/routes.js
     * @param {function} cb - callback(error, data)
     * @param {array} data - optional field providing already fetched data
     *   in order of dataPoints mentioned for each route in lib/api/routes.js
     * @return {undefined}
     */
    getCompletions(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting metrics: completions', {
                    method: 'BackbeatAPI.getCompletions',
                });
                return cb(errors.InternalError);
            }

            // Find if time since start is less than EXPIRY time
            const timeSinceStart = (Date.now() - this._internalStart) / 1000;
            // Seconds up to a max of EXPIRY seconds
            const timeDisplay = timeSinceStart < EXPIRY ?
                (timeSinceStart || 1) : EXPIRY;
            const numOfIntervals = Math.ceil(timeDisplay / INTERVAL);

            const d = res.map(r => (
                r.requests.slice(0, numOfIntervals).reduce((acc, i) =>
                    acc + i, 0)
            ));

            const response = {
                completions: {
                    description: 'Number of completed replication operations ' +
                        '(count) and number of bytes transferred (size) in ' +
                        `the last ${Math.floor(timeDisplay)} seconds`,
                    results: {
                        count: d[0],
                        size: d[1],
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
                this._logger.error('error getting metrics: throughput', {
                    method: 'BackbeatAPI.getThroughput',
                });
                return cb(errors.InternalError);
            }

            const now = new Date();
            const timeSinceStart = (now - this._internalStart) / 1000;
            // Seconds up to a max of EXPIRY seconds
            const timeDisplay = timeSinceStart < EXPIRY ?
                (timeSinceStart || 1) : EXPIRY;
            const numOfIntervals = Math.ceil(timeDisplay / INTERVAL);

            const [opsThroughput, bytesThroughput] = res.map(r => {
                let total = r.requests.slice(0, numOfIntervals).reduce(
                    (acc, i) => acc + i, 0);

                // if timeDisplay !== EXPIRY, use interval timer and do not
                // include the extra 4th interval
                if (timeDisplay === EXPIRY) {
                    // all intervals apply, including 4th interval
                    const lastInterval =
                        this._statsClient._normalizeTimestamp(new Date(now));
                    // in seconds
                    const diff = (now - lastInterval) / 1000;

                    // Get average for last interval depending on time
                    // surpassed so far for newest interval
                    total += ((INTERVAL - diff) / INTERVAL) *
                        r.requests[numOfIntervals];
                }

                // Divide total by timeDisplay to determine data per second
                return (total / timeDisplay);
            });

            const response = {
                throughput: {
                    description: 'Current throughput for replication ' +
                        'operations in ops/sec (count) and MB/sec (size) ' +
                        `in the last ${Math.floor(timeDisplay)} seconds`,
                    results: {
                        count: opsThroughput.toFixed(2),
                        size: bytesThroughput.toFixed(2),
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
            encodedVersionId: entry.getEncodedVersionId(),
        };
        const backbeatMetadataProxy = this._backbeatMetadataProxy;
        if (this._repConfig.source.auth.type === 'role') {
            backbeatMetadataProxy.setupSourceRole(entry, log);
        }
        return backbeatMetadataProxy.setSourceClient(log)
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
                const response = {
                    Bucket: queueEntry.getBucket(),
                    Key: queueEntry.getObjectKey(),
                    VersionId: queueEntry.getEncodedVersionId(),
                    StorageClass: entry.getSite(),
                    Size: queueEntry.getContentLength(),
                    LastModified: queueEntry.getLastModified(),
                };
                if (this._repConfig.source.auth.type === 'role') {
                    response.Role =
                        queueEntry.getReplicationRoles().split(',')[0];
                }
                return next(null, response);
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
        const { bucket, key, role } = details;
        let { versionId } = details;
        // If the original CRR was on a bucket without `versioning` enabled
        // (i.e. an NFS bucket), maintain the Redis key schema by using and
        // empty string.
        versionId = versionId === undefined ? '' : versionId;
        const member = getSortedSetMember(bucket, key, versionId, role);
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
        return async.eachLimit(this._validSites, 10, (site, next) => {
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
            }
            return cb(err);
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
                    return this._redisClient.zrem(key, member, next);
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
                    done => this._redisClient.zrem(key, member, done),
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
            ], err => {
                if (err) {
                    this._logger.debug('failed to delete duplicate object ' +
                    'fail key', {
                        method: 'BackbeatAPI._normalizeFailMetrics',
                        error: err,
                    });
                    return next();
                }
                return next();
            }));
    }

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
            const { Bucket, Key, VersionId, StorageClass, Role } = o;
            const member = getSortedSetMember(Bucket, Key, VersionId, Role);
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
            const requiredProperties =
                ['Bucket', 'Key', 'VersionId', 'StorageClass'];
            if (this._repConfig.source.auth.type === 'role') {
                requiredProperties.push('Role');
            }
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
     * @param {string} site - site name
     * @param {function} cb - callback(err, res)
     * @return {undefined}
     */
    _queryStats(ops, site, cb) {
        // TODO: Querying by site level here will change in future
        //   For now, since only 'all' sites is supported, query all
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
