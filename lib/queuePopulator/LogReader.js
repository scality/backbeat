const async = require('async');

const { versioning } = require('arsenal');
const { DbPrefixes } = versioning.VersioningConstants;

const BackbeatProducer = require('../BackbeatProducer');
const ReplicationQueuePopulator =
    require('../../extensions/replication/ReplicationQueuePopulator');

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
     * @param {MetricsHandler} params.metricsHandler - instance of metrics
     *   handler
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
        this._metricsHandler = params.metricsHandler;
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
            { method: 'LogReader.setup',
                logSource: this.getLogInfo() });
        this._readLogOffset((err, offset) => {
            if (err) {
                this.log.error('log source setup failed',
                    { method: 'LogReader.setup',
                        logSource: this.getLogInfo() });
                return done(err);
            }
            this.logOffset = offset;
            this.log.info('log reader is ready to populate replication ' +
                          'queue',
                { logSource: this.getLogInfo(),
                    logOffset: this.logOffset });
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
                        { method: 'LogReader._readLogOffset',
                            error: err });
                    return done(err);
                }
                return this.zkClient.mkdirp(pathToLogOffset, err => {
                    if (err) {
                        this.log.error(
                            'Could not pre-create path in zookeeper',
                            { method: 'LogReader._readLogOffset',
                                zkPath: pathToLogOffset,
                                error: err });
                        return done(err);
                    }
                    return this._initializeLogOffset(done);
                });
            }
            if (data) {
                const logOffset = Number.parseInt(data, 10);
                if (Number.isNaN(logOffset)) {
                    this.log.error(
                        'invalid stored log offset',
                        { method: 'LogReader._readLogOffset',
                            zkPath: pathToLogOffset,
                            logOffset: data.toString() });
                    return done(null, 1);
                }
                this.log.debug(
                    'fetched current log offset successfully',
                    { method: 'LogReader._readLogOffset',
                        zkPath: pathToLogOffset,
                        logOffset });
                return done(null, logOffset);
            }
            return this._initializeLogOffset(done);
        });
    }

    _initializeLogOffset(done) {
        const pathToLogOffset = this.pathToLogOffset;
        this.log.debug('initializing log offset', {
            method: 'LogReader._initializeLogOffset',
            zkPath: pathToLogOffset,
        });
        this.logConsumer.readRecords({ limit: 1 }, (err, res) => {
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
                        { method: 'LogReader._writeLogOffset',
                            zkPath: pathToLogOffset,
                            logOffset: this.logOffset });
                    return done(err);
                }
                this.log.debug(
                    'saved log offset',
                    { method: 'LogReader._writeLogOffset',
                        zkPath: pathToLogOffset,
                        logOffset: this.logOffset });
                return done();
            }
        );
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
                    || (batchState.logStats.nbLogRecordsRead < params.maxRead);
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
                this.log.info('replication batch finished', {
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
                    { method: 'LogReader._processReadRecords',
                        params, error: err });
                return done(err);
            }
            batchState.debugStep = 'read records [END]';
            this.log.debug(
                'readRecords callback',
                { method: 'LogReader._processReadRecords',
                    params, info: res.info });
            batchState.logRes = res;
            return done();
        });
    }

    _processLogEntry(batchState, record, entry) {
        function _transformKey(key) {
            if (key.startsWith(DbPrefixes.Master)) {
                return key.slice(DbPrefixes.Master.length);
            }
            if (key.startsWith(DbPrefixes.Version)) {
                return key.slice(DbPrefixes.Version.length);
            }
            return key;
        }
        if (entry.key === undefined || entry.value === undefined) {
            return;
        }
        console.log('_processLogEntry =>> entry.type!!!', entry.type);
        console.log('_processLogEntry =>> record.db!!!', record.db);
        console.log('_processLogEntry =>> entry.value!!!', entry.value);
        console.log('_processLogEntry =>> entry.key!!!', entry.key);
        console.log('_processLogEntry =>> _transformKey(entry.key)!!!', _transformKey(entry.key));
        this._extensions.forEach(ext => ext.filter({
            type: entry.type,
            bucket: record.db,
            key: _transformKey(entry.key),
            value: entry.value,
            logReader: this,
        }));
    }

    _processPrepareEntries(batchState, done) {
        const { entriesToPublish, logRes, logStats } = batchState;

        if (logRes.info.start === null) {
            batchState.debugStep = 'prepare entries [END]';
            return done(null);
        }

        logRes.log.on('data', record => {
            batchState.debugStep = 'prepare entries [DATA]';
            this.log.debug('received log data',
                           { nbEntries: record.entries.length,
                             info: logRes.info });
            logStats.nbLogRecordsRead += 1;

            this._setEntryBatch(entriesToPublish);
            record.entries.forEach(entry => {
                logStats.nbLogEntriesRead += 1;

                this._processLogEntry(batchState, record, entry);
            });
            this._unsetEntryBatch();
        });
        logRes.log.on('error', err => {
            batchState.debugStep = 'prepare entries [ERROR]';
            this.log.error('error fetching entries from log',
                { method: 'LogReader._processPrepareEntries',
                    error: err });
            return done(err);
        });
        logRes.log.on('end', () => {
            batchState.debugStep = 'prepare entries [END]';
            this.log.debug('ending record stream', { info: logRes.info });
            return done();
        });
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
                           { kafkaConfig: this.kafkaConfig,
                             topic });
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

        console.log('entriesToPublish!!!', entriesToPublish);
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
                        }
                    );
                    const labels = Object.assign({},
                        this.getMetricLabels(),
                        { publishStatus: 'FAILURE' }
                    );
                    this._metricsHandler.messages(labels, topicEntries.length);
                    return done(err);
                }
                this.log.debug('entries published successfully to topic',
                               { method: 'LogReader._processPublishEntries',
                                 topic, entryCount: topicEntries.length });
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
            const extMetrics = crrExtension.getAndResetMetrics();
            if (Object.keys(extMetrics).length > 0) {
                this._mProducer.publishMetrics(extMetrics, metricsTypeQueued,
                    metricsExtension, () => {});
            }
            return done();
        });
    }

    _processSaveLogOffset(batchState, done) {
        if (batchState.nextLogOffset !== undefined &&
            batchState.nextLogOffset !== this.logOffset) {
            // different logs use different fields for tracking the size
            const logSize = batchState.logRes.info.cseq || batchState.logRes.info.end;
            this.logOffset = batchState.nextLogOffset;
            this._metricsHandler.logReadOffset(this.getMetricLabels(), this.logOffset);
            this._metricsHandler.logSize(this.getMetricLabels(), logSize);
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
        for (const [topic, prod] of Object.entries(this._producers)) {
            statuses[topic] = prod.isReady();
        }
        return statuses;
    }
}

module.exports = LogReader;
