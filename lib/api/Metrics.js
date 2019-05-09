const async = require('async');

const errors = require('arsenal').errors;
const { RedisClient, StatsModel } = require('arsenal').metrics;

const INTERVAL = 300; // 5 minutes
const EXPIRY = 86400; // 24 hours
const THROUGHPUT_EXPIRY = 900; // 15 minutes
const isTest = process.env.CI === 'true';

class Metrics {
    constructor(config, logger) {
        const { redisConfig, crrSites, ingestionSites, internalStart } = config;
        this._logger = logger;
        this._redisClient = new RedisClient(redisConfig, this._logger);
        // Redis expiry increased by an additional interval so we can reference
        // the immediate older data for average throughput calculation
        this._statsClient = new StatsModel(this._redisClient, INTERVAL, EXPIRY);
        this._crrSites = crrSites;
        this._ingestionSites = ingestionSites;
        this._internalStart = internalStart;
    }

    /**
     * Query StatsClient for all ops given
     * @param {array} ops - array of redis key names to query
     * @param {string} site - site name or '*' wildcard
     * @param {string} bucketName - the name of the bucket
     * @param {string} objectKey - the object key name
     * @param {string} versionId - the object version ID
     * @param {string} service - service name (i.e. crr, ingestion)
     * @param {function} cb - callback(err, res)
     * @return {undefined}
     */
    _queryStats(ops, site, bucketName, objectKey, versionId, service, cb) {
        return async.map(ops, (op, done) => {
            // an op represents a given services redis key schema for a given
            // data point.
            // it is possible for an op to be undefined for a given service
            if (!op) {
                return done();
            }
            const hasGlobalKey = this._hasGlobalKey(op);
            const sites = service === 'crr' ?
                          this._crrSites : this._ingestionSites;
            if (site === 'all') {
                const queryStrings = sites.map(s => {
                    if (bucketName && objectKey && versionId) {
                        return `${s}:${bucketName}:${objectKey}:` +
                               `${versionId}:${op}`;
                    }
                    return `${s}:${op}`;
                });
                if (hasGlobalKey) {
                    return this._statsClient.getAllGlobalStats(queryStrings,
                        this._logger, done);
                }
                return this._statsClient.getAllStats(this._logger, queryStrings,
                    done);
            }
            // Query only a single given site or storage class
            // First, validate the site or storage class
            if (!sites.includes(site)) {
                // escalate error to log later
                return done({
                    message: 'invalid site name provided',
                    type: errors.RouteNotFound,
                    method: 'Metrics._queryStats',
                });
            }
            let queryString;
            if (bucketName && objectKey && versionId) {
                queryString =
                    `${site}:${bucketName}:${objectKey}:${versionId}:${op}`;
            } else {
                queryString = `${site}:${op}`;
            }
            if (hasGlobalKey) {
                return this._redisClient.get(queryString, (err, res) => {
                    if (err) {
                        return done({
                            message: `Redis error: ${err.message}`,
                            type: errors.InternalError,
                            method: 'Metrics._queryStats',
                        });
                    }
                    return done(null, { requests: [res || 0] });
                });
            }
            return this._statsClient.getStats(this._logger, queryString, done);
        }, cb);
    }

    /**
     * Determines whether the Redis op uses a global counter or interval key.
     * @param {String} op - The Redis operation
     * @return {Boolean} true if a global counter, false otherwise
     */
    _hasGlobalKey(op) {
        if (isTest) {
            return op.includes('test:bb:bytespending') ||
                op.includes('test:bb:opspending');
        }
        return op.includes('bb:crr:bytespending') ||
            op.includes('bb:crr:opspending') ||
            op.includes('bb:ingestion:opspending');
    }

    /**
     * Get data points which are the keys used to query Redis
     * @param {object} details - route details from lib/backbeat/routes.js
     * @param {array} data - provides already fetched data in order of
     *   dataPoints mentioned for each route in lib/backbeat/routes.js. This can
     *   be undefined.
     * @param {function} cb - callback(error, data), where data returns
     *   data stored in Redis.
     * @return {array} dataPoints array defined in lib/backbeat/routes.js
     */
    _getData(details, data, cb) {
        if (!data) {
            const { dataPoints, site, bucketName, objectKey,
                versionId, service } = details;
            return this._queryStats(dataPoints, site, bucketName, objectKey,
                versionId, service, cb);
        }
        return cb(null, data);
    }

    /**
     * Uptime of server based on this._internalStart up to max of expiry
     * @param {number} expiry - max expiry
     * @return {number} uptime of server up to expiry time
     */
    _getMaxUptime(expiry) {
        let secondsSinceStart = (Date.now() - this._internalStart) / 1000;
        // allow only a minimum value of 1 for uptime
        if (secondsSinceStart < 1) {
            secondsSinceStart = 1;
        }
        return secondsSinceStart < expiry ? secondsSinceStart : expiry;
    }

    /**
     * Get backlog in ops count and size in bytes
     * @param {object} details - route details from lib/backbeat/routes.js
     * @param {function} cb - callback(error, data)
     * @param {array} data - optional field providing already fetched data in
     *   order of dataPoints mentioned for each route in lib/backbeat/routes.js
     * @return {undefined}
     */
    getBacklog(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err && err.type) {
                this._logger.error('error getting metric: backlog', {
                    origin: err.method,
                    method: 'Metrics.getBacklog',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting metrics: backlog', {
                    method: 'Metrics.getBacklog',
                });
                return cb(errors.InternalError);
            }
            const count = Number.parseInt(res[0].requests, 10);
            const size = Number.parseInt(res[1].requests, 10);
            const response = {
                backlog: {
                    description: 'Number of incomplete replication ' +
                        'operations (count) and number of incomplete bytes ' +
                        'transferred (size)',
                    results: {
                        count: count < 0 ? 0 : count,
                        size: size < 0 ? 0 : size,
                    },
                },
            };
            return cb(null, response);
        });
    }

    /**
     * Get completed stats by ops count and size in bytes
     * @param {object} details - route details from lib/backbeat/routes.js
     * @param {function} cb - callback(error, data)
     * @param {array} data - optional field providing already fetched data in
     *   order of dataPoints mentioned for each route in lib/backbeat/routes.js
     * @return {undefined}
     */
    getCompletions(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err && err.type) {
                this._logger.error('error getting metric: completions', {
                    origin: err.method,
                    method: 'Metrics.getCompletions',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting metrics: completions', {
                    method: 'Metrics.getCompletions',
                });
                return cb(errors.InternalError);
            }
            const uptime = this._getMaxUptime(EXPIRY);
            const numOfIntervals = Math.ceil(uptime / INTERVAL);
            const [opsDone, bytesDone] = res.map(r => (
                r.requests.slice(0, numOfIntervals).reduce((acc, i) =>
                    acc + i, 0)
            ));

            const { service } = details;
            if (service === 'crr') {
                return this._sendCompletionsResponseReplication(uptime, opsDone,
                    bytesDone, cb);
            }
            if (service === 'ingestion') {
                return this._sendCompletionsResponseIngestion(uptime, opsDone,
                    cb);
            }
            return cb(errors.InternalError.customizeDescription(
                `unknown service ${service} when getting completions metric`));
        });
    }

    _sendCompletionsResponseReplication(uptime, ops, bytes, cb) {
        const results = {
            count: ops,
            size: bytes,
        };
        const response = {
            completions: {
                description: 'Number of completed replication operations ' +
                    '(count) and number of bytes transferred (size) in the ' +
                    `last ${Math.floor(uptime)} seconds`,
                results,
            },
        };
        return cb(null, response);
    }

    _sendCompletionsResponseIngestion(uptime, ops, cb) {
        const results = {
            count: ops,
        };
        const response = {
            completions: {
                description: 'Number of completed ingestion operations ' +
                    `(count) in the last ${Math.floor(uptime)} seconds`,
                results,
            },
        };
        return cb(null, response);
    }

    /**
     * Get failed stats by ops count and size in bytes
     * @param {object} details - route details from lib/backbeat/routes.js
     * @param {function} cb - callback(error, data)
     * @param {array} data - optional field providing already fetched data in
     *   order of dataPoints mentioned for each route in lib/backbeat/routes.js
     * @return {undefined}
     */
    getFailedMetrics(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err && err.type) {
                this._logger.error('error getting metric: failures', {
                    origin: err.emthod,
                    method: 'Metrics.getFailedMetrics',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting metrics: failures', {
                    method: 'Metrics.getFailedMetrics',
                });
                return cb(errors.InternalError);
            }
            const uptime = this._getMaxUptime(EXPIRY);
            const numOfIntervals = Math.ceil(uptime / INTERVAL);
            const [opsFail, bytesFail] = res.map(r => (
                r.requests.slice(0, numOfIntervals).reduce((acc, i) =>
                    acc + i, 0)
            ));

            const response = {
                failures: {
                    description: 'Number of failed replication operations ' +
                        '(count) and bytes (size) in the last ' +
                        `${Math.floor(uptime)} seconds`,
                    results: {
                        count: opsFail,
                        size: bytesFail,
                    },
                },
            };
            return cb(null, response);
        });
    }

    /**
     * Get current throughput in ops/sec and bytes/sec up to max of 15 minutes
     * Throughput is the number of units processed in a given time
     * @param {object} details - route details from lib/backbeat/routes.js
     * @param {function} cb - callback(error, data)
     * @param {array} data - optional field providing already fetched data in
     *   order of dataPoints mentioned for each route in lib/backbeat/routes.js
     * @return {undefined}
     */
    getThroughput(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err && err.type) {
                this._logger.error('error getting metric: throughput', {
                    origin: err.method,
                    method: 'Metrics.getThroughput',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err) {
                this._logger.error('error getting metrics: throughput', {
                    method: 'Metrics.getThroughput',
                });
                return cb(errors.InternalError);
            }
            const now = new Date();
            const uptime = this._getMaxUptime(THROUGHPUT_EXPIRY);
            const numOfIntervals = Math.ceil(uptime / INTERVAL);
            const [opsThroughput, bytesThroughput] = res.map(r => {
                let total = r.requests.slice(0, numOfIntervals).reduce(
                    (acc, i) => acc + i, 0);

                // if uptime !== THROUGHPUT_EXPIRY, use internal timer and
                // do not include the extra 4th interval
                if (uptime === THROUGHPUT_EXPIRY) {
                    // all intervals apply, including 4th interval
                    const lastInterval =
                        this._statsClient._normalizeTimestamp(now);
                    // in seconds
                    const diff = (now - lastInterval) / 1000;
                    // Get average for last interval depending on time
                    // surpassed so far for newest interval
                    total += ((INTERVAL - diff) / INTERVAL) *
                        r.requests[numOfIntervals];
                }

                // Divide total by uptime to determine data per second
                return (total / uptime);
            });

            const { service } = details;
            if (service === 'crr') {
                return this._sendThroughputResponseReplication(uptime,
                    opsThroughput, bytesThroughput, cb);
            }
            if (service === 'ingestion') {
                return this._sendThroughputResponseIngestion(uptime,
                    opsThroughput, cb);
            }
            return cb(errors.InternalError.customizeDescription(
                `unknown service ${service} when getting throughput metric`));
        });
    }

    _sendThroughputResponseReplication(uptime, ops, bytes, cb) {
        const results = {
            count: ops.toFixed(2),
            size: bytes.toFixed(2),
        };
        const response = {
            throughput: {
                description: 'Current throughput for replication operations ' +
                    'in ops/sec (count) and bytes/sec (size) in the last ' +
                    `${Math.floor(uptime)} seconds`,
                results,
            },
        };
        return cb(null, response);
    }

    _sendThroughputResponseIngestion(uptime, ops, cb) {
        const results = {
            count: ops.toFixed(2),
        };
        const response = {
            throughput: {
                description: 'Current throughput for ingestion operations in ' +
                    `ops/sec (count) in the last ${Math.floor(uptime)} seconds`,
                results,
            },
        };
        return cb(null, response);
    }

    /**
     * Get current throughput for an object in bytes/sec. Throughput is the
     * number of bytes transferred in a given time.
     * @param {object} details - route details from lib/api/routes.js
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    getObjectThroughput(details, cb) {
        this._getData(details, undefined, (err, res) => {
            if (err && err.type) {
                this._logger.error('error getting metric: object throughput', {
                    origin: err.method,
                    method: 'Metrics.getObjectThroughput',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err) {
                this._logger.error('error getting metrics: object throughput', {
                    method: 'Metrics.getObjectThroughput',
                    error: err.message,
                });
                return cb(errors.InternalError);
            }
            const now = new Date();
            const uptime = this._getMaxUptime(THROUGHPUT_EXPIRY);
            const numOfIntervals = Math.ceil(uptime / INTERVAL);
            const { requests } = res[0]; // Bytes done
            let total = requests.slice(0, numOfIntervals)
                .reduce((acc, i) => acc + i, 0);
            // if uptime !== THROUGHPUT_EXPIRY, use internal timer
            // and do not include the extra 4th interval
            if (uptime === THROUGHPUT_EXPIRY) {
                // all intervals apply, including 4th interval
                const lastInterval =
                    this._statsClient._normalizeTimestamp(now);
                // in seconds
                const diff = (now - lastInterval) / 1000;
                // Get average for last interval depending on time passed so
                // far for newest interval
                total += ((INTERVAL - diff) / INTERVAL) *
                    requests[numOfIntervals];
            }
            // Divide total by timeDisplay to determine data per second
            const response = {
                description: 'Current throughput for object replication in ' +
                    'bytes/sec (throughput)',
                throughput: (total / uptime).toFixed(2),
            };
            return cb(null, response);
        });
    }

    /**
     * Get CRR progress for an object in bytes. Progress is the percentage of
     * the object that has completed replication.
     * @param {object} details - route details from lib/api/routes.js
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    getObjectProgress(details, cb) {
        this._getData(details, undefined, (err, res) => {
            if (err && err.type) {
                this._logger.error('error getting metric: object progress', {
                    origin: err.method,
                    method: 'Metrics.getObjectProgress',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting metrics: object progress', {
                    method: 'Metrics.getObjectProgress',
                    error: err.message,
                });
                return cb(errors.InternalError);
            }
            // Find if time since start is less than EXPIRY time
            const uptime = this._getMaxUptime(EXPIRY);
            const numOfIntervals = Math.ceil(uptime / INTERVAL);
            const [totalBytesToComplete, bytesComplete] = res.map(r => (
                r.requests.slice(0, numOfIntervals).reduce((acc, i) =>
                    acc + i, 0)
            ));
            const ratio = totalBytesToComplete === 0 ? 0 :
                bytesComplete / totalBytesToComplete;
            const percentage = (ratio * 100).toFixed();
            const response = {
                description: 'Number of bytes to be replicated ' +
                    '(pending), number of bytes transferred to the ' +
                    'destination (completed), and percentage of the ' +
                    'object that has completed replication (progress)',
                pending: totalBytesToComplete - bytesComplete,
                completed: bytesComplete,
                progress: `${percentage}%`,
            };
            return cb(null, response);
        });
    }

    /**
     * Get pending stats by ops count and size in bytes
     * @param {object} details - route details from lib/backbeat/routes.js
     * @param {function} cb - callback(error, data)
     * @param {array} data - optional field providing already fetched data in
     *   order of dataPoints mentioned for each route in lib/backbeat/routes.js
     * @return {undefined}
     */
    getPending(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err && err.type) {
                this._logger.error('error getting metric: pending', {
                    origin: err.method,
                    method: 'Metrics.getPending',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            const { dataPoints } = details;
            if (err || res.length !== dataPoints.length) {
                this._logger.error('error getting metrics: pending', {
                    method: 'Metrics.getPending',
                    error: err,
                    dataPoints,
                    res,
                });
                return cb(errors.InternalError
                    .customizeDescription('error getting metrics: pending'));
            }
            const count = Number.parseInt(res[0].requests, 10);
            const size = Number.parseInt(res[1].requests, 10);

            const { service } = details;
            if (service === 'crr') {
                return this._sendPendingResponseReplication(count, size, cb);
            }
            if (service === 'ingestion') {
                return this._sendPendingResponseIngestion(count, cb);
            }
            return cb(errors.InternalError.customizeDescription(
                `unknown service ${service} when getting pending metric`));
        });
    }

    _sendPendingResponseReplication(ops, bytes, cb) {
        const results = {
            count: ops,
            size: bytes,
        };
        const response = {
            pending: {
                description: 'Number of pending replication operations ' +
                    '(count) and bytes (size)',
                results,
            },
        };
        return cb(null, response);
    }

    _sendPendingResponseIngestion(ops, cb) {
        const results = {
            count: ops < 0 ? 0 : ops,
        };
        const response = {
            pending: {
                description: 'Number of pending ingestion operations (count)',
                results,
            },
        };
        return cb(null, response);
    }

    /**
     * Get all metrics
     * @param {object} details - route details from lib/backbeat/routes.js
     * @param {function} cb = callback(error, data)
     * @param {array} data - optional field providing already fetched data in
     *   order of dataPoints mentioned for each route in lib/backbeat/routes.js
     * @return {undefined}
     */
    getAllMetrics(details, cb, data) {
        function _adjustedDetails(details, size) {
            const dataPointLength = size >= 0 ? size : 2;
            return Object.assign({},
                                 details,
                                 { dataPoints: new Array(dataPointLength) });
        }

        this._getData(details, data, (err, res) => {
            if (err && err.type) {
                this._logger.error('error getting metric: all', {
                    origin: err.method,
                    method: 'Metrics.getAllMetrics',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting metrics: all', {
                    method: 'Metrics.getAllMetrics',
                });
                return cb(errors.InternalError);
            }
            const { service } = details;
            // res = [ ops, ops_done, ops_fail, bytes, bytes_done, bytes_fail,
            // opsPending, bytesPending ]
            return async.parallel([
                done => {
                    if (service === 'ingestion') {
                        return done();
                    }
                    return this.getBacklog(_adjustedDetails(details), done,
                        [res[6], res[7]]);
                },
                done => this.getCompletions(_adjustedDetails(details), done,
                    [res[1], res[4]]),
                done => {
                    if (service === 'ingestion') {
                        return done();
                    }
                    return this.getFailedMetrics(_adjustedDetails(details),
                        done, [res[2], res[5]]);
                },
                done => this.getThroughput(_adjustedDetails(details), done,
                    [res[1], res[4]]),
                done => this.getPending(_adjustedDetails(details), done,
                    [res[6], res[7]]),
            ], (err, results) => {
                if (err) {
                    this._logger.error('error getting metrics: all', {
                        method: 'Metrics.getAllMetrics',
                        service,
                    });
                    return cb(errors.InternalError);
                }
                const values = results.filter(r => r !== undefined);
                const store = Object.assign({}, ...values);
                return cb(null, store);
            });
        });
    }

    /**
     * Close redis client
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    disconnect(cb) {
        return this._redisClient.disconnect(cb);
    }

    /**
     * Retrieve the list of redis client connectiosn
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    listClients(cb) {
        return this._redisClient.listClients(cb);
    }
}

module.exports = Metrics;
