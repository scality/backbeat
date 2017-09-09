const async = require('async');
const zookeeper = require('node-zookeeper-client');

const werelogs = require('werelogs');

const arsenal = require('arsenal');
const errors = require('arsenal').errors;
const BucketClient = require('bucketclient').RESTClient;
const MetadataFileClient = arsenal.storage.metadata.MetadataFileClient;
const LogConsumer = arsenal.storage.metadata.LogConsumer;

const BackbeatProducer = require('../../../lib/BackbeatProducer');

class QueuePopulator {
    /**
     * Create a queue populator object to send metadata log journal
     * entries from a source metadata to a kafka topic dedicated
     * to stream clueso entries.
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
     * @param {Object} cluesoConfig - clueso configuration object
     * @param {String} cluesoConfig.topic - clueso topic name
     * @param {String} cluesoConfig.queuePopulator - config object
     *   specific to queue populator
     * @param {String} cluesoConfig.queuePopulator.zookeeperPath -
     *   sub-path to use for storing populator state in zookeeper
     */
    constructor(zkConfig, sourceConfig, cluesoConfig) {
        this.zkConfig = zkConfig;
        this.sourceConfig = sourceConfig;
        this.cluesoConfig = cluesoConfig;
        this.log = new werelogs.Logger('Backbeat:Clueso:QueuePopulator');
    }

    /**
     * Open the queue populator
     *
     * @param {function} cb - callback function
     * @return {undefined}
     */
    open(cb) {
        let logIdentifier;

        this.logState = {};
        this.state = {};

        switch (this.sourceConfig.logSource) {
        case 'bucketd':
            this.logState.raftSession =
                this.sourceConfig.bucketd.raftSession;
            logIdentifier = `raft_${this.logState.raftSession}`;
            break;
        case 'dmd':
            this.logState.logName = this.sourceConfig.dmd.logName;
            logIdentifier = `bucketFile_${this.logState.logName}`;
            break;
        default:
            this.log.error("bad 'logSource' config value: expect 'bucketd'" +
                           `or 'dmd', got '${this.sourceConfig.logSource}'`);
            return process.nextTick(() => cb(errors.InternalError));
        }
        this.pathToLogOffset = `/logState/${logIdentifier}/logOffset`;

        return async.parallel([
            done => this._setupLogSource(done),
            done => this._setupProducer(done),
            done => this._setupZookeeper(done),
        ], err => {
            if (err) {
                this.log.error('error starting up queue populator',
                               { method: 'QueuePopulator.open',
                                 error: err, errorStack: err.stack });
                return cb(err);
            }
            this.log.info('queue populator is ready to populate ' +
                          'clueso queue',
                          { logOffset: this.logOffset });
            return cb();
        });
    }

    _setupLogSource(done) {
        switch (this.sourceConfig.logSource) {
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
        const bucketdConfig = this.sourceConfig.bucketd;
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
        const dmdConfig = this.sourceConfig.dmd;
        this.log.info('initializing bucketfile log handle',
                      { method: 'QueuePopulator._openBucketFileLog',
                        dmdConfig });
        const mdClient = new MetadataFileClient({
            host: dmdConfig.host,
            port: dmdConfig.port,
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
            topic: this.cluesoConfig.topic,
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
        const populatorZkPath = this.cluesoConfig.queuePopulator.zookeeperPath;
        const zookeeperUrl =
            `${this.zkConfig.connectionString}${populatorZkPath}`;
        this.log.info('opening zookeeper connection for persisting ' +
                      'populator state',
                      { zookeeperUrl });
        this.zkClient = zookeeper.createClient(zookeeperUrl);
        console.log("creating zookeeper client!!", this.zkClient);
        this.zkClient.connect();
        console.log("just called connect!!")
        this._readLogOffset((err, offset) => {
            console.log("in callback for read offset log", err, offset);
            if (err) {
                return done(err);
            }
            this.logOffset = offset;
            return done();
        });
    }

    _writeLogOffset(done) {
        const zkClient = this.zkClient;
        const pathToLogOffset = this.pathToLogOffset;
        zkClient.setData(
            pathToLogOffset, new Buffer(this.logOffset.toString()), -1,
            err => {
                if (err) {
                    this.log.error(
                        'error saving log offset',
                        { method: 'QueuePopulator._writeLogOffset',
                          zkPath: pathToLogOffset,
                          logOffset: this.logOffset });
                    return done(err);
                }
                this.log.debug(
                    'saved log offset',
                    { method: 'QueuePopulator._writeLogOffset',
                      zkPath: pathToLogOffset,
                      logOffset: this.logOffset });
                return done();
            });
    }

    _readLogOffset(done) {
        const zkClient = this.zkClient;
        const pathToLogOffset = this.pathToLogOffset;
        console.log("about to call get data!!")
        this.zkClient.getData(pathToLogOffset, (err, data) => {
            console.log("called zkClient.getDATA!!!", err, data)
            if (err) {
                if (err.name !== 'NO_NODE') {
                    this.log.error(
                        'Could not fetch log offset',
                        { method: 'QueuePopulator._readLogOffset',
                          error: err, errorStack: err.stack });
                    return done(err);
                }
                return zkClient.mkdirp(pathToLogOffset, err => {
                    console.log("in callback for mkdirp!!", err)
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

    _shieldBucket(bucketName) {
        if (bucketName.startsWith('mpuShadowBucket') ||
        bucketName === 'users..bucket' ||
        bucketName === 'METADATA' ||
        bucketName === '__metastore') {
            return true;
        }
        return false;
    }

    _logEntryToQueueEntry(record, entry) {
        if (entry.key !== undefined && entry.value !== undefined &&
            !this._shieldBucket(record.db)) {
            const value = JSON.parse(entry.value);
            value.userMd = {};
            Object.keys(value).forEach(attributeName => {
                if (attributeName.startsWith('x-amz-meta')) {
                    value.userMd[attributeName] =
                    value[attributeName];
                    delete value[attributeName];
                }
            });
            const queueEntry = {
                type: entry.type,
                bucket: record.db,
                key: entry.key,
                value,
            };
            this.log.trace('queueing entry', { queueEntry });
            return {
                key: entry.key,
                message: JSON.stringify(queueEntry),
            };
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
     *     - logOffset {Number} - next offset (sequence number) to process
     *       from the log
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
            next => this._processSaveLogOffset(batchState, next),
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
                    logOffset: this.logOffset,
                    processedAll,
                });
            });
        return undefined;
    }

    _processReadRecords(params, batchState, done) {
        const readOptions = {};
        if (this.logOffset !== undefined) {
            readOptions.startSeq = this.logOffset;
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
            batchState.nextLogOffset !== this.logOffset) {
            this.logOffset = batchState.nextLogOffset;
            return this._writeLogOffset(done);
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
            logOffset: this.logOffset,
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
            return self.processLogEntries(params, cbProcess);
        }
        this.processLogEntries(params, cbProcess);
    }
}


module.exports = QueuePopulator;
