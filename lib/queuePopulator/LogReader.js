const async = require('async');

const { versioning } = require('arsenal');
const { DbPrefixes } = versioning.VersioningConstants;

const BackbeatProducer = require('../BackbeatProducer');
const ReplicationQueuePopulator =
    require('../../extensions/replication/ReplicationQueuePopulator');
const NotificationQueuePopulator =
    require('../../extensions/notification/NotificationQueuePopulator');

const { metricsExtension, metricsTypeQueued } =
    require('../../extensions/replication/constants');

const BATCH_TIMEOUT_SECONDS = 300;

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
     */
    constructor(params) {
        this.zkClient = params.zkClient;
        this.kafkaConfig = params.kafkaConfig;
        this.logConsumer = params.logConsumer;
        this.pathToLogOffset = `/logState/${params.logId}/logOffset`;
        this.logOffset = null;
        this.log = params.logger;
        this._producers = {};
        this._extensions = params.extensions;
        this._mProducer = params.metricsProducer;
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
            this.log.info('log reader is ready to populate replication ' +
                'queue',
                {
                    logSource: this.getLogInfo(),
                    logOffset: this.logOffset,
                });
            return done();
        });
    }

    /**
     * get the next offset to fetch from source log (aka. log sequence
     * number)
     *
     * @return {number} the next log offset to fetch
     */
    getLogOffset() {
        return this.logOffset;
    }

    _readLogOffset(done) {
        const pathToLogOffset = this.pathToLogOffset;
        this.zkClient.getData(pathToLogOffset, (err, data) => {
            if (err) {
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
                        return done(err);
                    }
                    return done(null, 1);
                });
            }
            if (data) {
                const logOffset = Number.parseInt(data, 10);
                if (Number.isNaN(logOffset)) {
                    this.log.error(
                        'invalid stored log offset',
                        {
                            method: 'LogReader._readLogOffset',
                            zkPath: pathToLogOffset,
                            logOffset: data.toString(),
                        });
                    return done(null, 1);
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
            return done(null, 1);
        });
    }

    _writeLogOffset(done) {
        const pathToLogOffset = this.pathToLogOffset;
        const offsetString = this.logOffset.toString();
        this.zkClient.setData(
            pathToLogOffset,
            Buffer.from(offsetString), -1,
            err => {
                if (err) {
                    this.log.error(
                        'error saving log offset',
                        {
                            method: 'LogReader._writeLogOffset',
                            zkPath: pathToLogOffset,
                            logOffset: this.logOffset,
                        });
                    return done(err);
                }
                this.log.debug(
                    'saved log offset',
                    {
                        method: 'LogReader._writeLogOffset',
                        zkPath: pathToLogOffset,
                        logOffset: this.logOffset,
                    });
                return done();
            });
    }

    /**
     * Process log entries, up to the maximum defined in params
     *
     * @param {Object} [params] - parameters object
     * @param {Number} [params.maxRead] - max number of records to process
     *   from the log. Records may contain multiple entries and all entries
     *   are not queued, so the number of queued entries is not directly
     *   related to this number.
     * @param {function} [params.onTimeout] - optional callback,
     *   called when a single batch times out
     * @param {function} done - callback when done processing the
     *   entries. Called with an error, or null and a statistics object as
     *   second argument. On success, the statistics contain the following:
     *     - readRecords {Number} - number of records read
     *     - readEntries {Number} - number of entries read (records can
     *       hold multiple entries)
     *     - queuedEntries {Object} - number of new entries queued in
     *       various kafka topics
     *     - processedAll {Boolean} - true if the log has no more unread
     *       records
     * @return {undefined}
     */
    processLogEntries(params, done) {
        const batchState = {
            logRes: null,
            logStats: {
                nbLogRecordsRead: 0,
                nbLogEntriesRead: 0,
            },
            entriesToPublish: {},
            publishedEntries: {},
            currentRecords: [],
            debugStep: '[INIT]',
        };

        const batchTimeoutTimer = setTimeout(() => {
            this.log.error('replication batch timeout', {
                logStats: batchState.logStats,
                batchStep: batchState.debugStep,
            });
            if (params.onTimeout) {
                params.onTimeout();
            }
        }, BATCH_TIMEOUT_SECONDS * 1000);
        async.waterfall([
            next => this._processReadRecords(params, batchState, next),
            next => this._processPrepareEntries(batchState, next),
            next => this._processFilterEntries(batchState, next),
            next => this._processPublishEntries(batchState, next),
            next => this._processSaveLogOffset(batchState, next),
        ],
            err => {
                clearTimeout(batchTimeoutTimer);
                if (err) {
                    return done(err);
                }
                const queuedEntries = {};
                const { publishedEntries } = batchState;
                Object.keys(publishedEntries).forEach(topic => {
                    queuedEntries[topic] = publishedEntries[topic].length;
                });
                const processedAll = !params
                    || !params.maxRead
                    || (batchState.logStats.nbLogRecordsRead
                        < params.maxRead);
                // Using this specific single-item array format for
                // logging to keep compatibility with existing ELK stack
                // scraping method
                const counters = [{
                    readRecords: batchState.logStats.nbLogRecordsRead,
                    readEntries: batchState.logStats.nbLogEntriesRead,
                    queuedEntries,
                    processedAll,
                    logSource: this.getLogInfo(),
                    logOffset: this.getLogOffset(),
                }];
                this.log.info('batch finished', {
                    counters,
                });
                batchState.debugStep = '[FINISHED]';
                return done(null, processedAll);
            });
        return undefined;
    }

    /* eslint-disable no-param-reassign */

    _processReadRecords(params, batchState, done) {
        const readOptions = {};
        if (this.logOffset !== undefined) {
            readOptions.startSeq = this.logOffset;
        }
        if (params && params.maxRead !== undefined) {
            readOptions.limit = params.maxRead;
        }
        this.log.debug('reading records', { readOptions });
        this.logConsumer.readRecords(readOptions, (err, res) => {
            if (err) {
                batchState.debugStep = 'read records [ERROR]';
                this.log.error(
                    'error while reading log records',
                    {
                        method: 'LogReader._processReadRecords',
                        params, error: err,
                    });
                return done(err);
            }
            batchState.debugStep = 'read records [END]';
            this.log.debug(
                'readRecords callback',
                {
                    method: 'LogReader._processReadRecords',
                    params,
                    info: res.info,
                });
            batchState.logRes = res;
            return done();
        });
    }

    _processLogEntry(batchState, record, entry, cb) {
        function _transformKey(key) {
            if (!key) {
                return undefined;
            }
            if (key.startsWith(DbPrefixes.Master)) {
                return key.slice(DbPrefixes.Master.length);
            }
            if (key.startsWith(DbPrefixes.Version)) {
                return key.slice(DbPrefixes.Version.length);
            }
            return key;
        }

        function _executeFilter(ext, entry, cb) {
            if (typeof ext.filterAsync === 'function') {
                ext.filterAsync(entry, cb);
            } else {
                ext.filter(entry);
                process.nextTick(cb);
            }
        }

        async.eachSeries(this._extensions, (ext, next) => {
            let entryValue = entry.value;
            const notifExtension = ext instanceof NotificationQueuePopulator;
            if (notifExtension) {
                entryValue = entry.value || '{}';
            }
            if (!notifExtension &&
                (entry.key === undefined || entry.value === undefined)) {
                return next();
            }
            const entryToFilter = {
                type: entry.type,
                bucket: record.db,
                key: _transformKey(entry.key),
                value: entryValue,
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

    _processPrepareEntries(batchState, done) {
        const { logRes, logStats } = batchState;

        if (logRes.info.start === null) {
            batchState.debugStep = 'prepare entries [END]';
            return done(null);
        }

        logRes.log.on('data', record => {
            batchState.debugStep = 'prepare entries [DATA]';
            this.log.debug('received log data',
                {
                    nbEntries: record.entries.length,
                    info: logRes.info,
                });
            logStats.nbLogRecordsRead += 1;
            batchState.currentRecords.push(record);
        });
        logRes.log.on('error', err => {
            batchState.debugStep = 'prepare entries [ERROR]';
            this.log.error('error fetching entries from log',
                {
                    method: 'LogReader._processPrepareEntries',
                    error: err,
                });
            return done(err);
        });
        logRes.log.on('end', () => {
            batchState.debugStep = 'prepare entries [END]';
            this.log.debug('ending record stream', { info: logRes.info });
            return done();
        });
        return undefined;
    }

    _processFilterEntry(batchState, record, done) {
        const { entriesToPublish } = batchState;
        if (!record || !record.entries) {
            return done();
        }
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
        if (!currentRecords || currentRecords.length === 0) {
            return done();
        }
        async.eachSeries(currentRecords,
            (rec, next) => this._processFilterEntry(batchState, rec, next),
            err => {
                batchState.debugStep = `filter entries [${err ? 'ERROR' : 'END'}]`;
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
        return undefined;
    }

    _setupProducer(topic, batchState, done) {
        if (this._producers[topic] !== undefined) {
            return process.nextTick(done);
        }
        const producer = new BackbeatProducer({
            kafka: { hosts: this.kafkaConfig.hosts },
            topic,
        });
        producer.once('error', err => {
            batchState.debugStep = 'setup producer [ERROR]';
            done(err);
        });
        producer.once('ready', () => {
            batchState.debugStep = 'setup producer [READY]';
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
        const { entriesToPublish, logRes, logStats } = batchState;

        if (logRes.info.start === null) {
            return done();
        }
        batchState.nextLogOffset =
            logRes.info.start + logStats.nbLogRecordsRead;

        return async.each(Object.keys(entriesToPublish), (topic, done) => {
            const topicEntries = entriesToPublish[topic];
            if (topicEntries.length === 0) {
                return done();
            }
            return async.series([
                done => this._setupProducer(topic, batchState, done),
                done => this._producers[topic].send(topicEntries, err => {
                    batchState.debugStep = `producer send [${err ? 'ERROR' : 'END'}]`;
                    done(err);
                }),
            ], err => {
                if (err) {
                    this.log.error(
                        'error publishing entries from log to topic',
                        {
                            method: 'LogReader._processPublishEntries',
                            topic,
                            entryCount: topicEntries.length,
                            error: err,
                        });
                    return done(err);
                }
                this.log.debug('entries published successfully to topic',
                    {
                        method: 'LogReader._processPublishEntries',
                        topic,
                        entryCount: topicEntries.length,
                    });
                batchState.publishedEntries[topic] = topicEntries;
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
        if (batchState.nextLogOffset !== undefined &&
            batchState.nextLogOffset !== this.logOffset) {
            this.logOffset = batchState.nextLogOffset;
            return this._writeLogOffset(err => {
                batchState.debugStep = `save log offset [${err ? 'ERROR' : 'END'}]`;
                done(err);
            });
        }
        return process.nextTick(() => done());
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
}

module.exports = LogReader;
