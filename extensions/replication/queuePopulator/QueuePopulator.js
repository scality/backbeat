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
const ProvisionDispatcher =
          require('../../../lib/provisioning/ProvisionDispatcher');

class QueuePopulator {
    /**
     * Create a queue populator object to activate Cross-Region
     * Replication from a source S3 server to a kafka topic dedicated
     * to store replication entries.
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {string} zkConfig.endpoint - zookeeper endpoint string
     *   as "host:port[/chroot]"
     * @param {Object} sourceConfig - source configuration
     * @param {String} sourceConfig.logSource - type of source
     *   log: "bucketd" (raft log) or "dmd" (bucketfile)
     * @param {Object} [sourceConfig.bucketd] - bucketd source
     *   configuration (mandatory if logSource is "bucket")
     * @param {Object} [sourceConfig.dmd] - dmd source
     *   configuration (mandatory if logSource is "dmd")
     * @param {Object} repConfig - replication configuration object
     * @param {String} repConfig.topic - replication topic name
     * @param {String} repConfig.queuePopulator - config object
     *   specific to queue populator
     * @param {String} repConfig.queuePopulator.zookeeperPath -
     *   sub-path to use for storing populator state in zookeeper
     * @param {Logger} logConfig - logging configuration object
     * @param {String} logConfig.logLevel - logging level
     * @param {Logger} logConfig.dumpLevel - dump level
     */
    constructor(zkConfig, sourceConfig, repConfig, logConfig) {
        this.zkConfig = zkConfig;
        this.sourceConfig = sourceConfig;
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
        this.logState = null;
        this.updatedLogState = undefined;

        return async.parallel([
            done => this._setupProducer(done),
            done => this._setupZookeeper(done),
        ], err => {
            if (err) {
                this.log.error('error starting up queue populator',
                               { method: 'QueuePopulator.open',
                                 error: err, errorStack: err.stack });
                return cb(err);
            }
            this._setupLogSource();
            return cb();
        });
    }

    /**
     * Close the queue populator
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        return async.parallel([
            done => this._closeLogState(done),
            done => this._closeProducer(done),
        ], cb);
    }

    _setupLogSource() {
        switch (this.sourceConfig.logSource) {
        case 'bucketd':
            // initialization of log source is deferred until the
            // dispatcher notifies us of which raft session we're
            // responsible for
            this._subscribeToRaftSessionDispatcher();
            break;
        case 'dmd':
            this._openBucketFileLog();
            break;
        default:
            throw new Error("bad 'logSource' config value: expect 'bucketd'" +
                            `or 'dmd', got '${this.sourceConfig.logSource}'`);
        }
    }

    _subscribeToRaftSessionDispatcher() {
        const zookeeperUrl =
                  this.zkConfig.endpoint +
                  this.repConfig.queuePopulator.zookeeperPath;
        const zkEndpoint = `${zookeeperUrl}/raft-id-dispatcher`;
        this.raftIdDispatcher =
            new ProvisionDispatcher({ endpoint: zkEndpoint },
                                    this.logConfig);
        this.raftIdDispatcher.subscribe((err, items) => {
            if (err) {
                this.log.error('error when receiving raft ID provision list',
                               { zkEndpoint, error: err });
                return undefined;
            }
            if (items.length === 0) {
                this.log.info('no raft ID provisioned, idling',
                              { zkEndpoint });
                this.updatedLogState = null;
                return undefined;
            }
            if (items.length > 1) {
                this.log.warn('more than one raft ID provisioned, idling',
                              { zkEndpoint, provisionList: items });
                this.updatedLogState = null;
                return undefined;
            }
            return this._openRaftLog(items[0]);
        });
    }

    _openRaftLog(raftId) {
        const bucketdConfig = this.sourceConfig.bucketd;
        this.log.info('initializing raft log handle',
                      { method: 'QueuePopulator._openRaftLog',
                        bucketdConfig, raftId });
        const { host, port } = bucketdConfig;
        const bucketClient = new BucketClient(`${host}:${port}`);
        const logState = {
            raftSession: raftId,
            pathToLogOffset: `/logState/raft_${raftId}/logOffset`,
            logConsumer: new LogConsumer({ bucketClient,
                                           raftSession: raftId,
                                           logger: this.log }),
            logOffset: null,
        };
        this._loadLogState(logState);
    }

    _openBucketFileLog() {
        const dmdConfig = this.sourceConfig.dmd;
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
                this.log.error('error opening record log',
                               { method: 'QueuePopulator._openBucketFileLog',
                                 dmdConfig });
                return undefined;
            }
            const logState = {
                logName: dmdConfig.logName,
                pathToLogOffset:
                `/logState/bucketFile_${dmdConfig.logName}/logOffset`,
                logConsumer,
                logOffset: null,
            };
            return this._loadLogState(logState);
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
        const populatorZkPath = this.repConfig.queuePopulator.zookeeperPath;
        const zookeeperUrl = `${this.zkConfig.endpoint}${populatorZkPath}`;
        this.log.info('opening zookeeper connection for persisting ' +
                      'populator state',
                      { zookeeperUrl });
        const zkClient = zookeeper.createClient(zookeeperUrl);
        zkClient.connect();
        this.zkClient = zkClient;
        done();
    }

    _loadLogState(logState) {
        this._readLogOffset(logState, (err, offset) => {
            if (err) {
                // already logged in _readLogState
                return undefined;
            }
            // eslint-disable-next-line no-param-reassign
            logState.logOffset = offset;
            this.log.info('queue populator is ready to populate ' +
                          'replication queue',
                          { raftId: logState.raftSession,
                            logName: logState.logName,
                            logOffset: logState.logOffset });
            // queue log state for next processing batch
            this.updatedLogState = logState;
            return undefined;
        });
    }

    _writeLogOffset(done) {
        const zkClient = this.zkClient;
        const pathToLogOffset = this.logState.pathToLogOffset;
        zkClient.setData(
            pathToLogOffset, new Buffer(this.logState.logOffset.toString()), -1,
            err => {
                if (err) {
                    this.log.error(
                        'error saving log offset',
                        { method: 'QueuePopulator._writeLogOffset',
                          zkPath: pathToLogOffset,
                          logOffset: this.logState.logOffset });
                    return done(err);
                }
                this.log.debug(
                    'saved log offset',
                    { method: 'QueuePopulator._writeLogOffset',
                      zkPath: pathToLogOffset,
                      logOffset: this.logState.logOffset });
                return done();
            });
    }

    _readLogOffset(logState, done) {
        const zkClient = this.zkClient;
        const pathToLogOffset = logState.pathToLogOffset;
        this.zkClient.getData(pathToLogOffset, (err, data) => {
            if (err) {
                if (err.name !== 'NO_NODE') {
                    this.log.error(
                        'Could not fetch log offset',
                        { method: 'QueuePopulator._readLogOffset',
                          error: err, errorStack: err.stack });
                    return done(err);
                }
                return zkClient.mkdirp(pathToLogOffset, err => {
                    if (err) {
                        this.log.error(
                            'Could not pre-create path in zookeeper',
                            { method: 'QueuePopulator._readLogOffset',
                              zkPath: pathToLogOffset,
                              error: err, errorStack: err.stack });
                        return done(err);
                    }
                    return done(null, 1);
                });
            }
            if (data) {
                const logOffset = Number.parseInt(data, 10);
                if (isNaN(logOffset)) {
                    this.log.error(
                        'invalid stored log offset',
                        { method: 'QueuePopulator._readLogOffset',
                          zkPath: pathToLogOffset,
                          logOffset: data.toString() });
                    return done(null, 1);
                }
                this.log.debug(
                    'fetched current log offset successfully',
                    { method: 'QueuePopulator._readLogOffset',
                      zkPath: pathToLogOffset,
                      logOffset });
                return done(null, logOffset);
            }
            return done(null, 1);
        });
    }

    _closeLogState(done) {
        if (this.raftIdDispatcher !== undefined) {
            return this.raftIdDispatcher.unsubscribe(done);
        }
        return process.nextTick(done);
    }

    _closeProducer(done) {
        this.producer.close(done);
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
     * @param {function} done - callback when done processing the
     *   entries. Called with an error, or null and a statistics object as
     *   second argument. On success, the statistics contain the following:
     *     - readRecords {Number} - number of records read
     *     - readEntries {Number} - number of entries read (records can
     *       hold multiple entries)
     *     - queuedEntries {Number} - number of new entries queued in kafka
     *       topic
     *     - logOffset {Number} - next offset (sequence number) to process
     *       from the log
     *     - processedAll {Boolean} - true if the log has no more unread
     *       records
     * @return {undefined}
     */
    processLogEntries(params, done) {
        if (this.updatedLogState !== undefined) {
            this.logState = this.updatedLogState;
            this.updatedLogState = undefined;
        }
        if (this.logState === null) {
            this.log.debug('queue populator has no configured log source');
            return process.nextTick(() => done(errors.ServiceUnavailable));
        }
        return this._processLogEntries(params, done);
    }

    _processLogEntries(params, done) {
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
            next => this._processSaveLogOffset(batchState, next),
        ],
            err => {
                if (err) {
                    return done(err);
                }
                const processedAll = !params
                          || !params.maxRead
                          || (batchState.logStats.nbLogRecordsRead
                              < params.maxRead);
                return done(null, {
                    raftId: this.logState.raftSession,
                    readRecords: batchState.logStats.nbLogRecordsRead,
                    readEntries: batchState.logStats.nbLogEntriesRead,
                    queuedEntries: batchState.entriesToPublish.length,
                    logOffset: this.logState.logOffset,
                    processedAll,
                });
            });
        return undefined;
    }

    _processReadRecords(params, batchState, done) {
        const readOptions = {};
        if (this.logState.logOffset !== undefined) {
            readOptions.startSeq = this.logState.logOffset;
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

        if (logRes.info.start === null) {
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
        const { entriesToPublish, logRes, logStats } = batchState;

        if (entriesToPublish.length === 0) {
            if (logRes.info.start !== null) {
                batchState.nextLogOffset =
                    logRes.info.start + logStats.nbLogRecordsRead;
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
            batchState.nextLogOffset =
                logRes.info.start + logStats.nbLogRecordsRead;
            this.log.debug(
                'entries published successfully',
                { method: 'QueuePopulator._processPublishEntries',
                  entryCount: entriesToPublish.length,
                  logOffset: batchState.nextLogOffset });
            return done();
        });
    }

    _processSaveLogOffset(batchState, done) {
        if (batchState.nextLogOffset !== undefined &&
            batchState.nextLogOffset !== this.logState.logOffset) {
            this.logState.logOffset = batchState.nextLogOffset;
            return this._writeLogOffset(done);
        }
        return process.nextTick(() => done());
    }

    /* eslint-enable no-param-reassign */

    processAllLogEntries(params, done) {
        if (this.updatedLogState !== undefined) {
            this.logState = this.updatedLogState;
            this.updatedLogState = undefined;
        }
        if (this.logState === null) {
            this.log.debug('queue populator has no configured log source');
            return process.nextTick(() => done(errors.ServiceUnavailable));
        }
        const self = this;
        const countersTotal = {
            raftId: this.logState.raftSession,
            readRecords: 0,
            readEntries: 0,
            queuedEntries: 0,
            logOffset: this.logState.logOffset,
        };
        function cbProcess(err, counters) {
            if (err) {
                return done(err);
            }
            countersTotal.readRecords += counters.readRecords;
            countersTotal.readEntries += counters.readEntries;
            countersTotal.queuedEntries += counters.queuedEntries;
            countersTotal.logOffset = counters.logOffset;
            self.log.debug('process batch finished',
                           { counters, countersTotal });
            if (counters.processedAll) {
                return done(null, countersTotal);
            }
            return self._processLogEntries(params, cbProcess);
        }
        return this._processLogEntries(params, cbProcess);
    }
}


module.exports = QueuePopulator;
