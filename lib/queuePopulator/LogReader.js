const async = require('async');

const jsutil = require('arsenal').jsutil;

const config = require('../Config');
const BackbeatProducer = require('../BackbeatProducer');
const ReplicationQueuePopulator =
    require('../../extensions/replication/ReplicationQueuePopulator');

const { metricsExtension, metricsTypeQueued } =
    require('../../extensions/replication/constants');
const { transformKey } = require('../util/entry');

class LogReader {

    /**
     * Create a log reader object to populate kafka topics from a
     * single log source (e.g. one raft session)
     *
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.zkClient - zookeeper client object
     * @param {Object} params.kafkaConfig - kafka configuration object
     * @param {Object} [params.logConsumer] - log consumer object
     * @param {function} params.logConsumer.readRecords - function
     *   that fetches records from the log, called by LogReader
     * @param {String} params.logId - log source unique identifier
     * @param {Logger} params.logger - logger object
     * @param {QueuePopulatorExtension[]} params.extensions - array of
     *   queue populator extension modules
     * @param {MetricsProducer} params.metricsProducer - instance of metrics
     *   producer
     * @param {MetricsHandler} params.metricsHandler - instance of metrics
     *   handler
     * @param {ZkMetricsHandler} params.zkMetricsHandler - instance of zookeeper
     *   metrics handler
     */
    constructor(params) {
        this.zkClient = params.zkClient;
        this.kafkaConfig = params.kafkaConfig;
        this.logConsumer = params.logConsumer;
        this.pathToLogOffset = `${config.queuePopulator.zookeeperPath}/` +
            `logState/${params.logId}/logOffset`;
        this.logOffset = null;
        this.log = params.logger;
        this._producers = {};
        this._extensions = params.extensions;
        this._mProducer = params.metricsProducer;

        // internal variable to carry over a tailable cursor across batches
        this._openLog = null;
        this._metricsHandler = params.metricsHandler;
        // TODO: use a common handler for zk metrics from all extensions
        this._zkMetricsHandler = params.zkMetricsHandler;
    }

    _setEntryBatch(entryBatch) {
        this._extensions.forEach(ext => ext.setBatch(entryBatch));
    }

    _unsetEntryBatch() {
        this._extensions.forEach(ext => ext.unsetBatch());
    }

    setLogConsumer(logConsumer) {
        this.logConsumer = logConsumer;
    }

    /**
     * prepare the log reader before starting to populate entries
     *
     * This function may be overriden by subclasses, if source log
     * initialization is needed, in which case they must still call
     * LogReader.setup().
     *
     * @param {function} done - callback function
     * @return {undefined}
     */
    setup(done) {
        this.log.debug('setting up log source',
            {
                method: 'LogReader.setup',
                logSource: this.getLogInfo(),
            });
        this._readLogOffset((err, offset) => {
            if (err) {
                this.log.error('log source setup failed',
                    {
                        method: 'LogReader.setup',
                        logSource: this.getLogInfo(),
                    });
                return done(err);
            }
            this.logOffset = offset;
            this.log.info('log reader is ready to populate backbeat queue(s)',
                {
                    logSource: this.getLogInfo(),
                    logOffset: this.logOffset
                });
            return done();
        });
    }

    /**
     * close the log reader, free any resources opened during the
     * LogReader lifetime
     *
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        async.each(
            Object.values(this._producers),
            (producer, done) => producer.close(done),
            cb);
    }

    /**
     * get the next offset to fetch from source log (aka. log sequence
     * number)
     *
     * @return {string} the next log offset to fetch
     */
    getLogOffset() {
        return this.logOffset;
    }

    _readLogOffset(done) {
        const pathToLogOffset = this.pathToLogOffset;
        this.zkClient.getData(pathToLogOffset, (err, data) => {
            if (err) {
                this._updateZkMetrics('read', 'error');
                if (err.name !== 'NO_NODE') {
                    this.log.error(
                        'Could not fetch log offset',
                        {
                            method: 'LogReader._readLogOffset',
                            error: err,
                        });
                    return done(err);
                }
                return this.zkClient.mkdirp(pathToLogOffset, err => {
                    if (err) {
                        this.log.error(
                            'Could not pre-create path in zookeeper',
                            {
                                method: 'LogReader._readLogOffset',
                                zkPath: pathToLogOffset,
                                error: err,
                            });
                        this._updateZkMetrics('write', 'error');
                        return done(err);
                    }
                    this._updateZkMetrics('write', 'success');
                    return this._initializeLogOffset(done);
                });
            }
            if (data) {
                this._updateZkMetrics('read', 'success');
                let logOffset;
                if (config.queuePopulator.logSource === 'mongo') {
                    logOffset = data.toString();
                } else {
                    const logOffsetNumber = Number.parseInt(data, 10);
                    if (Number.isNaN(logOffsetNumber)) {
                        this.log.error(
                            'invalid stored log offset',
                            {
                                method: 'LogReader._readLogOffset',
                                zkPath: pathToLogOffset,
                                logOffset: data.toString()
                            });
                        return done(null, 1);
                    }
                    logOffset = logOffsetNumber.toString();
                }
                this.log.debug(
                    'fetched current log offset successfully',
                    {
                        method: 'LogReader._readLogOffset',
                        zkPath: pathToLogOffset,
                        logOffset,
                    });
                return done(null, logOffset);
            }
            return this._initializeLogOffset(done);
        });
    }

    _initializeLogOffset(done) {
        // TODO start at latest log offset on non-bucketd backends (mongodb, dmd)
        if (config.queuePopulator.logSource !== 'bucketd') {
            return done(null, 1);
        }
        const pathToLogOffset = this.pathToLogOffset;
        this.log.debug('initializing log offset', {
            method: 'LogReader._initializeLogOffset',
            zkPath: pathToLogOffset,
        });
        return this.logConsumer.readRecords({ limit: 1 }, (err, res) => {
            if (err) {
                // FIXME: getRaftLog metadata route returns 500 when
                // its cache does not contain the queried raft
                // session. For the sake of simplicity, in order to
                // allow the populator to make progress, we choose to
                // accept errors fetching the current offset during
                // setup phase and fallback to starting from offset
                // 1. It would be better to have metadata return
                // special success statuses in such case.
                this.log.warn(
                    'error reading initial log offset, ' +
                        'default to initial offset 1', {
                            method: 'LogReader._initializeLogOffset',
                            zkPath: pathToLogOffset,
                            logOffset: 1,
                            error: err,
                        });
                return done(null, 1);
            }
            const logOffset = res.info.cseq + 1;
            this.log.info('starting after latest log sequence', {
                method: 'LogReader._initializeLogOffset',
                zkPath: pathToLogOffset,
                logOffset,
            });
            return done(null, logOffset);
        });
    }

    _writeLogOffset(logger, done) {
        const pathToLogOffset = this.pathToLogOffset;
        const offsetString = this.logOffset.toString();
        this.zkClient.setData(
            pathToLogOffset,
            Buffer.from(offsetString), -1,
            err => {
                if (err) {
                    logger.error(
                        'error saving log offset',
                        {
                            method: 'LogReader._writeLogOffset',
                            zkPath: pathToLogOffset,
                            logOffset: this.logOffset,
                        });
                    this._updateZkMetrics('write', 'error');
                    return done(err);
                }
                this._updateZkMetrics('write', 'success');
                logger.debug(
                    'saved log offset',
                    {
                        method: 'LogReader._writeLogOffset',
                        zkPath: pathToLogOffset,
                        logOffset: this.logOffset,
                    });
                return done();
            }
        );
    }

    /**
     * Process log entries, up to the maximum defined in params or
     * until the timeout is reached waiting for new entries to come
     * (if tailable oplog)
     *
     * @param {Object} [params] - parameters object
     * @param {Number} [params.maxRead] - max number of records to process
     *   from the log. Records may contain multiple entries and all entries
     *   are not queued, so the number of queued entries is not directly
     *   related to this number.
     * @param {Number} [params.timeoutMs] - timeout after which the
     *   batch will be stopped if still running (default 10000ms)
     * @param {function} done - callback when done processing the
     *   entries - done(error) or done(null, {boolean} hasMoreLog:
     *   true if there is more log to read)
     * @return {undefined}
     */
    processLogEntries(params, done) {
        const batchState = {
            logRes: null,
            logStats: {
                nbLogRecordsRead: 0,
                nbLogEntriesRead: 0,
                hasMoreLog: false,
            },
            entriesToPublish: {},
            publishedEntries: {},
            currentRecords: [],
            maxRead: params.maxRead,
            startTime: Date.now(),
            timeoutMs: params.timeoutMs,
            logger: this.log.newRequestLogger(),
        };
        async.waterfall([
            next => this._processReadRecords(params, batchState, next),
            next => this._processPrepareEntries(batchState, next),
            next => this._processFilterEntries(batchState, next),
            next => this._processPublishEntries(batchState, next),
            next => this._processSaveLogOffset(batchState, next),
        ],
            err => {
                if (err) {
                    return done(err);
                }
                const queuedEntries = {};
                const { publishedEntries } = batchState;
                Object.keys(publishedEntries).forEach(topic => {
                    queuedEntries[topic] = publishedEntries[topic].length;
                });
                const stats = {
                    readRecords: batchState.logStats.nbLogRecordsRead,
                    readEntries: batchState.logStats.nbLogEntriesRead,
                    skippedRecords: batchState.logStats.skippedRecords,
                    hasMoreLog: batchState.logStats.hasMoreLog,
                    queuedEntries,
                };
                // Use heuristics to log when:
                // - at least one entry is pushed to any topic
                // - many records have been skipped (e.g. when starting up
                //   consumption from mongo oplog)
                // - the batch took a significant time to complete.
                const useInfoLevel =
                    Object.keys(stats.queuedEntries).length > 0 ||
                    stats.skippedRecords > 1000;
                const endLog = batchState.logger.end();
                const logFunc = (useInfoLevel ? endLog.info : endLog.debug)
                    .bind(endLog);
                logFunc('batch completed', {
                    stats,
                    logSource: this.getLogInfo(),
                    logOffset: this.getLogOffset(),
                });
                return done(null, stats.hasMoreLog);
            });
        return undefined;
    }

    /* eslint-disable no-param-reassign */

    _processReadRecords(params, batchState, done) {
        if (this._openLog) {
            // an open tailable cursor already exists: reuse it
            batchState.logRes = this._openLog;
            return done();
        }
        const { logger } = batchState;
        const readOptions = {};
        if (this.logOffset !== undefined) {
            readOptions.startSeq = this.logOffset;
        }
        if (params && params.maxRead !== undefined) {
            // only has effect for non-tailable oplog
            readOptions.limit = params.maxRead;
        }
        logger.debug('reading records', { readOptions });
        return this.logConsumer.readRecords(readOptions, (err, res) => {
            if (err) {
                logger.error(
                    'error while reading log records',
                    {
                        method: 'LogReader._processReadRecords',
                        params, error: err,
                    });
                return done(err);
            }
            logger.debug(
                'readRecords callback',
                {
                    method: 'LogReader._processReadRecords',
                    params
                });
            batchState.logRes = res;
            if (res.tailable) {
                // carry tailable cursor over for future batches
                this._openLog = res;
            }
            return done();
        });
    }

    _processPrepareEntries(batchState, done) {
        const { logRes, logStats, logger } = batchState;

        // for raft log only
        if (logRes.info && logRes.info.start === null) {
            return done(null);
        }

        let shipBatchCb;
        const dataEventHandler = record => {
            logger.debug('received log data', {
                nbEntries: record.entries.length,
                timestamp: record.timestamp,
            });
            logStats.nbLogRecordsRead += 1;

            if (record.timestamp) {
                const timestamp = new Date(record.timestamp).getTime() / 1000;
                this._metricsHandler.logTimestamp(this.getMetricLabels(), timestamp);
            }
            batchState.currentRecords.push(record);
            // Pause tailable streams when reaching the record batch
            // limit, as those streams do not emit 'end' events but
            // continuously send 'data' events as they come.
            if (logRes.tailable &&
                logStats.nbLogRecordsRead >= batchState.maxRead) {
                logger.debug('ending batch', {
                    method: 'LogReader._processPrepareEntries',
                    reason: 'limit on number of read records reached',
                    readRecords: logStats.nbLogRecordsRead,
                });
                return shipBatchCb();
            }
            return undefined;
        };
        const errorEventHandler = err => {
            logger.error('error fetching entries from log', {
                method: 'LogReader._processPrepareEntries',
                error: err,
            });
            return shipBatchCb(err);
        };
        const endEventHandler = () => {
            logger.debug('ending record stream', {
                method: 'LogReader._processPrepareEntries',
            });
            return shipBatchCb();
        };
        logRes.log.on('data', dataEventHandler);
        logRes.log.on('error', errorEventHandler);
        logRes.log.on('end', endEventHandler);

        if (logRes.tailable) {
            //
            // For tailable oplog, set an interval check to timely end
            // and ship the current batch "timeoutMs" after the
            // beginning of the batch.
            //
            // This should guarantee regular updates to the stored
            // offset in zookeeper, while allowing a batch enough time
            // to maximize its rate of consumption of log messages
            // (deemed useful to reduce startup time).
            //
            const checker = setInterval(() => {
                const now = Date.now();
                if (now >= batchState.startTime + batchState.timeoutMs) {
                    logger.debug('ending batch', {
                        method: 'LogReader._processPrepareEntries',
                        reason: 'batch timeout reached',
                        readRecords: logStats.nbLogRecordsRead,
                    });
                    shipBatchCb();
                }
            }, 1000);
            shipBatchCb = jsutil.once(err => {
                logRes.log.pause();
                logRes.log.removeListener('data', dataEventHandler);
                logRes.log.removeListener('error', errorEventHandler);
                logRes.log.removeListener('end', endEventHandler);
                clearInterval(checker);
                done(err);
            });
        } else {
            shipBatchCb = jsutil.once(done);
            // for raft log
            logRes.log.on('info', info => {
                logger.debug('received record stream "info" event', {
                    method: 'LogReader._processPrepareEntries',
                    info,
                });
                logRes.info = info;
            });
        }
        if (logRes.tailable && logRes.log.isPaused()) {
            logger.debug('resuming tailable cursor', {
                method: 'LogReader._processPrepareEntries',
            });
            logRes.log.resume();
        }
        if (logRes.log.getSkipCount) {
            logStats.initialSkipCount = logRes.log.getSkipCount();
        }
        return undefined;
    }

    _processLogEntry(batchState, record, entry, cb) {
        function _executeFilter(ext, entry, cb) {
            if (typeof ext.filterAsync === 'function') {
                ext.filterAsync(entry, cb);
            } else {
                ext.filter(entry);
                process.nextTick(cb);
            }
        }
        async.eachSeries(this._extensions, (ext, next) => {
            /**
             * Delete entries don't have a value field.
             * Value field normally contains the timestamp
             * of the event, which is needed for the notification
             * extension.
             * That gets added here, we use the oplog event timestamp
             */
            if (
                entry.type === 'delete' &&
                ext.constructor.name === 'NotificationQueuePopulator'
            ) {
                entry.value = JSON.stringify({
                    'last-modified': record.timestamp
                });
            // skipping entry for other extensions
            } else if (entry.value === undefined) {
                return next();
            }
            const entryToFilter = {
                type: entry.type,
                bucket: record.db,
                // removing the v1 metadata prefix if it exists
                key: transformKey(entry.key),
                value: entry.value,
                logReader: this,
            };
            return _executeFilter(ext, entryToFilter, next);
        }, cb);
    }

    _filterEntries(batchState, record, cb) {
        const { logStats } = batchState;

        return async.eachSeries(record.entries, (entry, next) => {
            logStats.nbLogEntriesRead += 1;
            return this._processLogEntry(batchState, record, entry, next);
        }, cb);
    }

    _processFilterEntry(batchState, record, done) {
        if (!record || !record.entries) {
            return done();
        }
        const { entriesToPublish } = batchState;
        this._setEntryBatch(entriesToPublish);
        return this._filterEntries(batchState, record, err => {
            this._unsetEntryBatch();
            if (err) {
                this.log.error('error filtering entries from log', {
                    method: 'LogReader._processFilterEntries',
                    error: err,
                });
                return done(err);
            }
            return done();
        });
    }

    _processFilterEntries(batchState, done) {
        const { currentRecords } = batchState;
        if (!currentRecords || !currentRecords.length) {
            return done();
        }
        return async.eachSeries(currentRecords,
            (rec, next) => this._processFilterEntry(batchState, rec, next),
            err => {
                if (err) {
                    this.log.error('error filtering entries from log', {
                        method: 'LogReader._processFilterEntries',
                        error: err,
                    });
                    return done(err);
                }
                return done();
            }
        );
    }

    _setupProducer(topic, done) {
        if (this._producers[topic] !== undefined) {
            return process.nextTick(done);
        }
        const producer = new BackbeatProducer({
            kafka: { hosts: this.kafkaConfig.hosts },
            topic,
        });
        producer.once('error', done);
        producer.once('ready', () => {
            this.log.debug('producer is ready',
                {
                    kafkaConfig: this.kafkaConfig,
                    topic,
                });
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this.log.error('error from backbeat producer',
                    { topic, error: err });
            });
            this._producers[topic] = producer;
            done();
        });
        return undefined;
    }

    _processPublishEntries(batchState, done) {
        const { entriesToPublish, logRes, logStats, logger } = batchState;

        // for raft log only
        if (logRes.info && logRes.info.start === null) {
            return done();
        }

        if (logRes.log.getOffset) {
            batchState.nextLogOffset = logRes.log.getOffset();
        } else {
            batchState.nextLogOffset =
                logRes.info.start + logStats.nbLogRecordsRead;
        }
        if (logStats.initialSkipCount !== undefined) {
            const finalSkipCount = logRes.log.getSkipCount();
            logStats.skippedRecords =
                finalSkipCount - logStats.initialSkipCount;
        }
        return async.each(Object.keys(entriesToPublish), (topic, done) => {
            const topicEntries = entriesToPublish[topic];
            if (topicEntries.length === 0) {
                return done();
            }
            return async.series([
                done => this._setupProducer(topic, done),
                done => this._producers[topic].send(topicEntries, done),
            ], err => {
                if (err) {
                    logger.error(
                        'error publishing entries from log to topic',
                        {
                            method: 'LogReader._processPublishEntries',
                            topic,
                            entryCount: topicEntries.length,
                            error: err,
                        }
                    );
                    const labels = Object.assign({},
                        this.getMetricLabels(),
                        { publishStatus: 'FAILURE' }
                    );
                    this._metricsHandler.messages(labels, topicEntries.length);
                    return done(err);
                }
                logger.debug('entries published successfully to topic',
                    {
                        method: 'LogReader._processPublishEntries',
                        topic, entryCount: topicEntries.length
                    });
                batchState.publishedEntries[topic] = topicEntries;
                const labels = Object.assign({},
                    this.getMetricLabels(),
                    { publishStatus: 'SUCCESS' }
                );
                this._metricsHandler.messages(labels, topicEntries.length);
                return done();
            });
        }, err => {
            // TODO: On error, produce error metrics
            if (err) {
                return done(err);
            }
            // Find the CRR Class extension
            const crrExtension = this._extensions.find(ext => (
                ext instanceof ReplicationQueuePopulator
            ));
            if (crrExtension) {
                const extMetrics = crrExtension.getAndResetMetrics();
                if (Object.keys(extMetrics).length > 0) {
                    this._mProducer.publishMetrics(extMetrics,
                        metricsTypeQueued, metricsExtension, () => { });
                }
            }
            return done();
        });
    }

    _processSaveLogOffset(batchState, done) {
        const { logRes, nextLogOffset, logger } = batchState;
        if (nextLogOffset !== undefined &&
            nextLogOffset !== this.logOffset &&
            (!logRes.log.reachedUnpublishedListing ||
             logRes.log.reachedUnpublishedListing())) {

            this.logOffset = nextLogOffset;

            // Skip metrics if we are not using Raft backend
            if (logRes.info) {
                const logSize = logRes.info.cseq || logRes.info.end;
                this._metricsHandler.logReadOffset(this.getMetricLabels(), this.logOffset);
                this._metricsHandler.logSize(this.getMetricLabels(), logSize);
            }

            return this._writeLogOffset(logger, done);
        }
        return process.nextTick(() => done());
    }

    _updateZkMetrics(op, status) {
        if (this._zkMetricsHandler && this._zkMetricsHandler.onZookeeperOp) {
            this._zkMetricsHandler.onZookeeperOp(op, status);
        }
    }

    /* eslint-enable no-param-reassign */

    /**
     * return an object containing useful info about the log
     * source. The default implementation returns an empty object,
     * subclasses may override to provide source-specific info.
     *
     * @return {object} log info object
     */
    getLogInfo() {
        return {};
    }

    /**
     * return an object to include as part of metric labels
     * @return {object} labels to include
     */
    getMetricLabels() {
        return {};
    }

    /**
     * Return an object of the current status of each producer.
     * @returns {Object<string, Boolean>} map of producer statuses
     */
    getProducerStatus() {
        const statuses = {};
        Object.keys(this._producers).forEach(topic => {
            statuses[topic] = this._producers[topic].isReady();
        });
        return statuses;
    }
}

module.exports = LogReader;
