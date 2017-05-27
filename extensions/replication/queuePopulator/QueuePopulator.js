const async = require('async');
const zookeeper = require('node-zookeeper-client');

const Logger = require('werelogs').Logger;

const arsenal = require('arsenal');
const errors = require('arsenal').errors;
const BucketClient = require('bucketclient').RESTClient;
const { isMasterKey } = require('arsenal/lib/versioning/Version');
const MetadataFileClient = arsenal.storage.metadata.MetadataFileClient;
const LogConsumer = arsenal.storage.metadata.LogConsumer;

const BackbeatProducer = require('../../../lib/BackbeatProducer');

class QueuePopulator {
    /**
     * Create a queue populator object to activate Cross-Region
     * Replication from a source S3 server to a kafka topic dedicated
     * to store replication entries.
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {String} zkConfig.host - zookeeper host
     * @param {Number} zkConfig.port - zookeeper port
     * @param {Object} repConfig - replication configuration object
     * @param {String} repConfig.topic - replication topic name
     * @param {Object} repConfig.source - source configuration
     * @param {String} repConfig.source.logSource - type of source
     *   log: "bucketd" (raft log) or "dmd" (bucketfile)
     * @param {Object} [repConfig.source.bucketd] - bucketd source
     *   configuration (mandatory if logSource is "bucket")
     * @param {Object} [repConfig.source.dmd] - dmd source
     *   configuration (mandatory if logSource is "dmd")
     * @param {Logger} logConfig - logging configuration object
     * @param {String} logConfig.logLevel - logging level
     * @param {Logger} logConfig.dumpLevel - dump level
     */
    constructor(zkConfig, repConfig, logConfig) {
        this.zkConfig = zkConfig;
        this.repConfig = repConfig;
        this.logConfig = logConfig;

        this.logger = new Logger('Backbeat:Replication:QueuePopulator',
                                 { level: logConfig.logLevel,
                                   dump: logConfig.dumpLevel });
        this.log = this.logger.newRequestLogger();
    }

    /**
     * Open the queue populator
     *
     * @param {function} cb - callback function
     * @return {undefined}
     */
    open(cb) {
        const sourceConfig = this.repConfig.source;
        let logIdentifier;

        this.logState = {};
        this.state = {};

        switch (sourceConfig.logSource) {
        case 'bucketd':
            this.logState.raftSession = sourceConfig.bucketd.raftSession;
            logIdentifier = `raft_${this.logState.raftSession}`;
            break;
        case 'dmd':
            this.logState.logName = sourceConfig.dmd.logName;
            logIdentifier = `bucketFile_${this.logState.logName}`;
            break;
        default:
            this.log.error("bad 'logSource' config value: expect 'bucketd'" +
                           `or 'dmd', got '${sourceConfig.logSource}'`);
            return process.nextTick(() => cb(errors.InternalError));
        }
        this.pathToLastProcessedSeq =
            `/logState/${logIdentifier}/lastProcessedSeq`;

        return async.parallel([
            done => this._setupLogSource(done),
            done => this._setupProducer(done),
            done => this._setupZookeeper(done),
        ], err => {
            if (err) {
                this.log.error('Error starting up queuePopulator',
                               { method: 'QueuePopulator.open',
                                 error: err, errorStack: err.stack });
                return cb(err);
            }
            return cb();
        });
    }

    _setupLogSource(done) {
        switch (this.repConfig.source.logSource) {
        case 'bucketd':
            return this._openRaftLog(done);
        case 'dmd':
            return this._openBucketFileLog(done);
        default:
            // not reached
            return undefined;
        }
    }

    _openRaftLog(done) {
        const bucketdConfig = this.repConfig.source.bucketd;
        this.log.info('initializing raft log handle',
                      { method: 'QueuePopulator._openRaftLog',
                        bucketdConfig });
        const { host, port, raftSession } = bucketdConfig;
        const bucketClient = new BucketClient(`${host}:${port}`);
        this.logState.logConsumer = new LogConsumer({ bucketClient,
                                                      raftSession,
                                                      logger: this.log });
        process.nextTick(() => done());
    }

    _openBucketFileLog(done) {
        const dmdConfig = this.repConfig.source.dmd;
        this.log.info('initializing bucketfile log handle',
                      { method: 'QueuePopulator._openBucketFileLog',
                        dmdConfig });
        const mdClient = new MetadataFileClient({
            host: dmdConfig.host,
            port: dmdConfig.port,
            log: this.logConfig,
        });
        const logConsumer = mdClient.openRecordLog({
            logName: dmdConfig.logName,
        }, err => {
            if (err) {
                return done(err);
            }
            this.logState.logConsumer = logConsumer;
            return done();
        });
    }

    _setupProducer(done) {
        const producer = new BackbeatProducer({
            zookeeper: this.zkConfig,
            log: this.logConfig,
            topic: this.repConfig.topic,
        });
        producer.once('error', done);
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this.log.error('error from backbeat producer',
                               { error: err });
            });
            this.producer = producer;
            done();
        });
    }

    _setupZookeeper(done) {
        const zkNamespace = this.repConfig.queuePopulator.zookeeperNamespace;
        const zookeeperUrl =
                  `${this.zkConfig.host}:${this.zkConfig.port}` +
                  `${zkNamespace}`;
        this.log.info('opening zookeeper connection for state ' +
                      'management',
                      { zookeeperUrl });
        const zkClient = zookeeper.createClient(zookeeperUrl);
        zkClient.connect();
        this.zkClient = zkClient;
        this._readLastProcessedSeq((err, seq) => {
            if (err) {
                return done(err);
            }
            this.lastProcessedSeq = seq;
            return done();
        });
    }

    _writeLastProcessedSeq(done) {
        const zkClient = this.zkClient;
        const pathToLastProcessedSeq = this.pathToLastProcessedSeq;
        const lastProcessedSeq = this.lastProcessedSeq;
        zkClient.setData(
            pathToLastProcessedSeq,
            new Buffer(lastProcessedSeq.toString()), -1,
            err => {
                if (err) {
                    this.log.error(
                        'error saving last processed sequence number',
                        { method: 'QueuePopulator._writeLastProcessedSeq',
                          zkPath: pathToLastProcessedSeq,
                          lastProcessedSeq });
                    return done(err);
                }
                this.log.debug(
                    'saved last processed sequence number',
                    { method: 'QueuePopulator._writeLastProcessedSeq',
                      zkPath: pathToLastProcessedSeq,
                      lastProcessedSeq });
                return done();
            });
    }

    _readLastProcessedSeq(done) {
        const zkClient = this.zkClient;
        const pathToLastProcessedSeq = this.pathToLastProcessedSeq;
        this.zkClient.getData(pathToLastProcessedSeq, (err, data) => {
            if (err) {
                if (err.name !== 'NO_NODE') {
                    this.log.error(
                        'Could not fetch latest processed sequence number',
                        { method: 'QueuePopulator._readLastProcessedSeq',
                          error: err,
                          errorStack: err.stack });
                    return done(err);
                }
                return zkClient.mkdirp(pathToLastProcessedSeq, err => {
                    if (err) {
                        this.log.error(
                            'Could not pre-create path in zookeeper',
                            { method: 'QueuePopulator._readLastProcessedSeq',
                              zkPath: pathToLastProcessedSeq,
                              error: err,
                              errorStack: err.stack });
                        return done(err);
                    }
                    return done(null, 0);
                });
            }
            if (data) {
                const lastProcessedSeq = Number.parseInt(data, 10);
                if (isNaN(lastProcessedSeq)) {
                    this.log.error(
                        'invalid latest processed sequence number',
                        { method: 'QueuePopulator._readLastProcessedSeq',
                          zkPath: pathToLastProcessedSeq,
                          lastProcessedSeq: data.toString() });
                    return done(null, 0);
                }
                this.log.debug(
                    'fetched latest processed sequence number',
                    { method: 'QueuePopulator._readLastProcessedSeq',
                      zkPath: pathToLastProcessedSeq,
                      lastProcessedSeq });
                return done(null, lastProcessedSeq);
            }
            return done(null, 0);
        });
    }

    _logEntryToQueueEntry(record, entry) {
        if (entry.type === 'put' &&
            entry.key !== undefined && entry.value !== undefined &&
            ! isMasterKey(entry.key)) {
            const value = JSON.parse(entry.value);
            if (value.replicationInfo &&
                value.replicationInfo.status === 'PENDING') {
                this.log.trace('queueing entry', { entry });
                const queueEntry = {
                    type: entry.type,
                    bucket: record.db,
                    key: entry.key,
                    value: entry.value,
                };
                return {
                    key: entry.key,
                    message: JSON.stringify(queueEntry),
                };
            }
        }
        return null;
    }

    /* eslint-disable no-param-reassign */

    /**
     * Process log entries, up to the maximum defined in params
     *
     * @param {Object} [params] - parameters object
     * @param {Number} [params.maxRead] - max number of records to process
     *   from the log. Records may contain multiple entries and all entries
     *   are not queued, so the number of queued entries is not directly
     *   related to this number.
     * @param {function} cb - callback when done processing the
     *   entries. Called with an error, or null and a statistics object as
     *   second argument. On success, the statistics contain the following:
     *     - readRecords {Number} - number of records read
     *     - readEntries {Number} - number of entries read (records can
     *       hold multiple entries)
     *     - queuedEntries {Number} - number of new entries queued in kafka
     *       topic
     *     - lastProcessedSeq {Number} - last sequence number read and
     *       processed from the log
     *     - processedAll {Boolean} - true if the log has no more unread
     *       records
     * @return {undefined}
     */
    processLogEntries(params, cb) {
        const batchState = {
            logRes: null,
            logStats: {
                nbLogRecordsRead: 0,
                nbLogEntriesRead: 0,
            },
            entriesToPublish: [],
        };
        async.waterfall([
            next => this._processReadRecords(params, batchState, next),
            next => this._processPrepareEntries(batchState, next),
            next => this._processPublishEntries(batchState, next),
            next => this._processSaveLastProcessedSeq(batchState, next),
        ],
            err => {
                if (err) {
                    return cb(err);
                }
                const processedAll = !params
                          || !params.maxRead
                          || (batchState.logStats.nbLogRecordsRead
                              < params.maxRead);
                return cb(null, {
                    readRecords: batchState.logStats.nbLogRecordsRead,
                    readEntries: batchState.logStats.nbLogEntriesRead,
                    queuedEntries: batchState.entriesToPublish.length,
                    lastProcessedSeq: this.lastProcessedSeq,
                    processedAll,
                });
            });
        return undefined;
    }

    _processReadRecords(params, batchState, done) {
        const readOptions = {};
        if (this.lastProcessedSeq !== undefined) {
            readOptions.startSeq = this.lastProcessedSeq + 1;
        }
        if (params && params.maxRead !== undefined) {
            readOptions.limit = params.maxRead;
        }
        this.log.debug('reading records', { readOptions });
        this.logState.logConsumer.readRecords(readOptions, (err, res) => {
            if (err) {
                this.log.error(
                    'error while reading log records',
                    { method: 'QueuePopulator._processReadRecords',
                      params, error: err, errorStack: err.stack });
                return done(err);
            }
            this.log.debug(
                'readRecords callback',
                { method: 'QueuePopulator._processReadRecords',
                  params, info: res.info });
            batchState.logRes = res;
            return done();
        });
    }

    _processPrepareEntries(batchState, done) {
        const { logRes, logStats } = batchState;

        if (logRes.info.end === null) {
            return done(null);
        }
        logRes.log.on('data', record => {
            logStats.nbLogRecordsRead += 1;
            record.entries.forEach(entry => {
                logStats.nbLogEntriesRead += 1;
                const queueEntry = this._logEntryToQueueEntry(record, entry);
                if (queueEntry) {
                    batchState.entriesToPublish.push(queueEntry);
                }
            });
        });
        logRes.log.on('error', err => {
            this.log.error('error fetching entries from log',
                           { method: 'QueuePopulator._processPrepareEntries',
                             error: err });
            return done(err);
        });
        logRes.log.on('end', () => {
            this.log.debug('ending record stream');
            return done();
        });
        return undefined;
    }

    _processPublishEntries(batchState, done) {
        const { entriesToPublish, logRes } = batchState;

        if (entriesToPublish.length === 0) {
            if (logRes.info.end !== null) {
                batchState.nextLastProcessedSeq = logRes.info.end;
            }
            return done();
        }
        return this.producer.send(entriesToPublish, err => {
            if (err) {
                this.log.error(
                    'error publishing entries from log',
                    { method: 'QueuePopulator._processPublishEntries',
                      error: err, errorStack: err.stack });
                return done(err);
            }
            batchState.nextLastProcessedSeq = logRes.info.end;
            this.log.debug(
                'entries published successfully',
                { method: 'QueuePopulator._processPublishEntries',
                  entryCount: entriesToPublish.length,
                  lastProcessedSeq: batchState.nextLastProcessedSeq });
            return done();
        });
    }

    _processSaveLastProcessedSeq(batchState, done) {
        if (batchState.nextLastProcessedSeq !== undefined &&
            batchState.nextLastProcessedSeq !== this.lastProcessedSeq) {
            this.lastProcessedSeq = batchState.nextLastProcessedSeq;
            return this._writeLastProcessedSeq(done);
        }
        return process.nextTick(() => done());
    }

    /* eslint-enable no-param-reassign */

    processAllLogEntries(params, done) {
        const self = this;
        const countersTotal = {
            readRecords: 0,
            readEntries: 0,
            queuedEntries: 0,
            lastProcessedSeq: this.lastProcessedSeq,
        };
        function cbProcess(err, counters) {
            if (err) {
                return done(err);
            }
            countersTotal.readRecords += counters.readRecords;
            countersTotal.readEntries += counters.readEntries;
            countersTotal.queuedEntries += counters.queuedEntries;
            countersTotal.lastProcessedSeq = counters.lastProcessedSeq;
            self.log.debug('process batch finished',
                           { counters, countersTotal });
            if (counters.processedAll) {
                return done(null, countersTotal);
            }
            return self.processLogEntries(params, cbProcess);
        }
        this.processLogEntries(params, cbProcess);
    }
}


module.exports = QueuePopulator;
