const async = require('async');

const BackbeatProducer = require('../BackbeatProducer');

class LogReader {

    /**
     * Create a log reader object to populate kafka topics from a
     * single log source (e.g. one raft session)
     *
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.zkClient - zookeeper client object
     * @param {Object} params.zkConfig - zookeeper configuration object
     * @param {Object} [params.logConsumer] - log consumer object
     * @param {function} params.logConsumer.readRecords - function
     *   that fetches records from the log, called by LogReader
     * @param {String} params.logId - log source unique identifier
     * @param {Logger} params.logger - logger object
     * @param {QueuePopulatorExtension[]} params.extensions - array of
     *   queue populator extension modules
     */
    constructor(params) {
        this.zkClient = params.zkClient;
        this.zkConfig = params.zkConfig;
        this.logConsumer = params.logConsumer;
        this.pathToLogOffset = `/logState/${params.logId}/logOffset`;
        this.logOffset = null;
        this.log = params.logger;
        this._producers = {};
        this._extensions = params.extensions;
    }

    _setEntryBatch(entryBatch) {
        this._extensions.forEach(ext => ext.setBatch(entryBatch));
    }

    _endEntryBatch() {
        this._extensions.forEach(ext => ext.endBatch());
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
                    return done(null, 1);
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
        };

        async.waterfall([
            next => this._processReadRecords(params, batchState, next),
            next => this._processPrepareEntries(batchState, next),
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
                const processedAll = !params
                          || !params.maxRead
                          || (batchState.logStats.nbLogRecordsRead
                              < params.maxRead);
                return done(null, {
                    readRecords: batchState.logStats.nbLogRecordsRead,
                    readEntries: batchState.logStats.nbLogEntriesRead,
                    queuedEntries,
                    processedAll,
                });
            });
        return undefined;
    }

    processAllLogEntries(params, done) {
        const self = this;
        const countersTotal = {
            readRecords: 0,
            readEntries: 0,
            queuedEntries: {},
        };
        function cbProcess(err, counters) {
            if (err) {
                return done(err);
            }
            countersTotal.readRecords += counters.readRecords;
            countersTotal.readEntries += counters.readEntries;
            Object.keys(counters.queuedEntries).forEach(topic => {
                if (countersTotal.queuedEntries[topic] === undefined) {
                    countersTotal.queuedEntries[topic] = 0;
                }
                countersTotal.queuedEntries[topic] +=
                    counters.queuedEntries[topic];
            });
            self.log.debug('process batch finished',
                           { counters, countersTotal });
            if (counters.processedAll) {
                return done(null, countersTotal);
            }
            return self.processLogEntries(params, cbProcess);
        }
        return self.processLogEntries(params, cbProcess);
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
                this.log.error(
                    'error while reading log records',
                    { method: 'LogReader._processReadRecords',
                        params, error: err });
                return done(err);
            }
            this.log.debug(
                'readRecords callback',
                { method: 'LogReader._processReadRecords',
                    params, info: res.info });
            batchState.logRes = res;
            return done();
        });
    }

    _processLogEntry(batchState, record, entry, opIndex) {
        if (entry.key === undefined || entry.value === undefined) {
            return;
        }
        this._extensions.forEach(ext => ext.filter({
            opIndex,
            type: entry.type,
            bucket: record.db,
            key: entry.key,
            value: entry.value,
        }));
    }

    _processPrepareEntries(batchState, done) {
        const { entriesToPublish, logRes, logStats } = batchState;

        if (logRes.info.start === null) {
            return done(null);
        }

        this._setEntryBatch(entriesToPublish);
        // start at -1 since will increment when recieve data event
        let recordNumber = logRes.info.start - 1;
        logRes.log.on('data', record => {
            logStats.nbLogRecordsRead += 1;
            recordNumber++;
            const paddedRecordNumber =
                `000000000000${recordNumber}`.substr(-12);
            record.entries.forEach((entry, index) => {
                const paddedIndex = `000000${index}`.substr(-6);
                const opIndex = `${paddedRecordNumber}_${paddedIndex}`;
                logStats.nbLogEntriesRead += 1;
                this._processLogEntry(batchState, record, entry, opIndex);
            });
        });
        logRes.log.on('error', err => {
            this.log.error('error fetching entries from log',
                { method: 'LogReader._processPrepareEntries',
                    error: err });
            return done(err);
        });
        logRes.log.on('end', () => {
            this.log.debug('ending record stream');
            this._endEntryBatch();
            return done();
        });
        return undefined;
    }

    _setupProducer(topic, done) {
        if (this._producers[topic] !== undefined) {
            return process.nextTick(done);
        }
        const producer = new BackbeatProducer({
            zookeeper: { connectionString: this.zkConfig.connectionString },
            topic,
        });
        producer.once('error', done);
        producer.once('ready', () => {
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
                done => this._setupProducer(topic, done),
                done => this._producers[topic].send(topicEntries, done),
            ], err => {
                if (err) {
                    this.log.error(
                        'error publishing entries from log to topic',
                        { method: 'LogReader._processPublishEntries',
                          topic,
                          entryCount: topicEntries.length,
                          error: err });
                    return done(err);
                }
                this.log.debug('entries published successfully to topic',
                               { method: 'LogReader._processPublishEntries',
                                 topic, entryCount: topicEntries.length });
                batchState.publishedEntries[topic] = topicEntries;
                return done();
            });
        }, done);
    }

    _processSaveLogOffset(batchState, done) {
        if (batchState.nextLogOffset !== undefined &&
            batchState.nextLogOffset !== this.logOffset) {
            this.logOffset = batchState.nextLogOffset;
            return this._writeLogOffset(done);
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
