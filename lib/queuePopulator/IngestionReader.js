const async = require('async');
const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;

const IngestionProducer = require('./IngestionProducer');
const LogReader = require('./LogReader');
const { decryptLocationSecret } = require('../management/index');

function _isVersionedLogKey(key) {
    return key.split(VID_SEP)[1];
}

class IngestionReader extends LogReader {
    constructor(params) {
        const { zkClient, kafkaConfig, bucketdConfig, qpConfig,
            logger, extensions, metricsProducer, s3Config } = params;
        super({ zkClient, kafkaConfig, logConsumer: {}, logId: '',
                logger, extensions, metricsProducer });
        this.qpConfig = qpConfig;
        this.s3Config = s3Config;
        this.bucketdConfig = bucketdConfig;
        this.logger = logger;

        // source ingestion bucket
        this.bucket = bucketdConfig.bucket;
        // zenko bucket to ingest to
        this._targetZenkoBucket = bucketdConfig.name;
    }

    _setupIngestionProducer(cb) {
        const encryptedKey = this.bucketdConfig.auth.secretKey;
        return decryptLocationSecret(encryptedKey, this.logger,
        (err, decryptedKey) => {
            if (err) {
                this.logger.error('failed to decrypt source secret key');
                return cb(err);
            }
            // only save decrypted key within IngestionProducer instance
            const updatedAuth = Object.assign({},
                this.bucketdConfig.auth, { secretKey: decryptedKey });
            const bucketdConfig = Object.assign({},
                this.bucketdConfig, { auth: updatedAuth });
            this._iProducer = new IngestionProducer(bucketdConfig,
                this.qpConfig, this.s3Config);
            return cb();
        });
    }

    setup(done) {
        const bucketZkRootPath = `/${this._targetZenkoBucket}/logState`;
        this.bucketInitPath = `${bucketZkRootPath}/init`;

        this._setupIngestionProducer(err => {
            if (err) {
                return done(err);
            }
            return this._iProducer.getRaftId(this.bucket, (err, data) => {
                if (err) {
                    return done(err);
                }
                this.raftId = data;
                this.logId = `raft_${this.raftId}`;
                this.pathToLogOffset =
                    `${bucketZkRootPath}/${this.logId}/logOffset`;

                return super.setup(done);
            });
        });
    }

    /**
     * Get the init (snapshot) state for this given IngestionReader
     * @param {function} done - callback(error, object)
     *   where object.versionMarker is the NextVersionIdMarker
     *   where object.keyMarker is the NextKeyMarker
     * @return {undefined}
     */
    _readInitState(done) {
        const initPathNodes = ['status', 'versionMarker', 'keyMarker'];
        let statusResponse;
        async.mapSeries(initPathNodes, (pathNode, cb) => {
            // if complete, we do not care to get other zookeeper data
            if (statusResponse === 'complete') {
                return process.nextTick(cb);
            }
            const path = `${this.bucketInitPath}/${pathNode}`;
            return this.zkClient.getData(path, (err, data) => {
                if (err) {
                    if (err.name !== 'NO_NODE') {
                        this.logger.error(
                            'Could not fetch ingestion init state',
                            { method: 'IngestionReader._readInitState',
                              zkPath: path,
                              error: err });
                        return cb(err);
                    }
                    return this.zkClient.mkdirp(path, err => {
                        if (err) {
                            this.logger.error(
                                'Could not pre-create path in zookeeper',
                                { method: 'IngestionReader._readInitState',
                                  zkPath: path,
                                  error: err });
                            return cb(err);
                        }
                        return cb();
                    });
                }
                this.logger.debug('fetched ingestion init state node', {
                    method: 'IngestionReader._readInitState',
                    zkPath: path,
                });
                const d = data && data.toString();
                if (pathNode === 'status') {
                    statusResponse = d;
                }
                return cb(null, d);
            });
        }, (err, data) => {
            if (err) {
                return done(err);
            }
            const [status, versionMarker, keyMarker] = data;
            return done(null, {
                status: status || 'incomplete',
                versionMarker,
                keyMarker
            });
        });
    }

    /**
     * Set the init (snapshot) state for this given IngestionReader
     * @param {object} initState - initState (snapshot) for ingestion
     * @param {string} initState.status - complete || incomplete
     * @param {string} [initState.versionMarker] - NextVersionIdMarker
     * @param {string} [initState.keyMarker] - NextKeyMarker
     * @param {Logger.newRequestLogger} logger - request logger object
     * @param {function} done - callback(error)
     * @return {undefined}
     */
    _writeInitState(initState, logger, done) {
        // initState is set by each request of processLogEntries. If undefined,
        // we did not go through snapshot phase
        if (!initState) {
            return process.nextTick(done);
        }
        const initPathNodes = ['status', 'versionMarker', 'keyMarker'];

        return async.each(initPathNodes, (pathNode, cb) => {
            const path = `${this.bucketInitPath}/${pathNode}`;
            const data = (initState[pathNode] || 'null').toString();
            return this.zkClient.setData(path, Buffer.from(data), err => {
                if (err) {
                    logger.error('error saving init state', {
                        method: 'IngestionReader._writeInitState',
                        zkPath: path,
                        error: err,
                    });
                    return cb(err);
                }
                logger.debug('saved init state', {
                    method: 'IngestionReader._writeInitState',
                    zkPath: path,
                });
                return cb();
            });
        }, done);
    }

    /* eslint-disable no-param-reassign */

    _processReadRecords(params, batchState, done) {
        const { logger } = batchState;
        const readOptions = {};
        if (this.logOffset !== undefined) {
            readOptions.startSeq = this.logOffset;
        }
        if (params && params.maxRead !== undefined) {
            readOptions.limit = params.maxRead;
        }
        logger.debug('reading records', { readOptions });

        return async.waterfall([
            next => this._readInitState(next),
            (initState, next) => {
                if (initState.status === 'complete') {
                    return this._iProducer.getRaftLog(this.raftId,
                    readOptions.startSeq, readOptions.limit, false,
                    (err, data) => {
                        if (err) {
                            this.logger.error('Error retrieving logs', { err,
                                raftId: this.raftId, method:
                                'IngestionReader._processReadRecords' });
                            return next(err);
                        }
                        this.logger.debug('readRecords got raft logs', {
                            method: 'IngestionReader._processReadRecords',
                            params });
                        batchState.logRes = data;
                        return next();
                    });
                }
                return this._iProducer.snapshot(this.bucket, initState,
                (err, res) => {
                    if (err) {
                        logger.error('error generating snapshot for ' +
                        'ingestion', { err });
                        return next(err);
                    }
                    batchState.logRes = { info: { start: 1 }, log: res.logRes };
                    batchState.initState = res.initState;
                    return next();
                });
            },
        ], done);
    }

    _processLogEntry(batchState, record, entry) {
        // NOTE: Using zenkoName because should be unique to other entries.

        // for a "del", entry.value will not exist but we still need to
        // pass through the event
        // for a bucket metadata entry from s3Connector, there will be no
        // entry.key but there will be an entry.type and we
        // need to pass this through
        if (entry.key === undefined && entry.type === undefined) {
            return;
        }
        if (!record.db) {
            this._extensions.forEach(ext => ext.filter({
                type: entry.type,
                bucket: entry.bucket,
                key: entry.key,
                value: entry.value,
            }));
        } else {
            if (record.db === 'users..bucket') {
                const keySplit = entry.key.split('..|..');
                entry.key = `${keySplit[0]}..|..${this._targetZenkoBucket}`;
            } else if (record.db === 'metastore') {
                const keySplit = entry.key.split('/');
                entry.key = `${keySplit[0]}/${this._targetZenkoBucket}`;
            } else {
                if (record.db === entry.key) {
                    entry.key = this._targetZenkoBucket;
                }
                record.db = this._targetZenkoBucket;
            }
            this._extensions.forEach(ext => ext.filter({
                type: entry.type,
                bucket: record.db,
                key: entry.key,
                value: entry.value,
            }));
        }
    }

    _processPrepareEntries(batchState, done) {
        const {
            entriesToPublish, logRes, logStats, logger, initState,
        } = batchState;

        this._setEntryBatch(entriesToPublish);

        // if initState, then these current log entries came from a snapshot
        if (initState) {
            logRes.log.forEach(entry => {
                // for snapshot phase, only versioned keys are separate records
                // and non-versioned keys are only considered entries.
                // Doing this for logging only. This won't affect offset in zk
                if (_isVersionedLogKey(entry.key)) {
                    logStats.nbLogRecordsRead += 1;
                }
                logStats.nbLogEntriesRead += 1;
                this._processLogEntry(batchState, entry, entry);
            });
            return done();
        }
        if (logRes.info.start === null || logRes.log === null) {
            return done(null);
        }

        this._setEntryBatch(entriesToPublish);

        logRes.log.on('data', record => {
            logStats.nbLogRecordsRead += 1;
            record.entries.forEach(entry => {
                logStats.nbLogEntriesRead += 1;
                if (record.db === this.bucket) {
                    this._processLogEntry(batchState, record, entry);
                }
            });
        });
        logRes.log.on('error', err => {
            logger.error('error fetching entries from log',
                { method: 'LogReader._processPrepareEntries',
                    error: err });
            return done(err);
        });
        logRes.log.on('end', () => {
            logger.debug('ending record stream');
            return done();
        });
        return undefined;
    }

    _processPublishEntries(batchState, done) {
        const {
            entriesToPublish, logRes, logStats, logger, initState,
        } = batchState;

        // initState.cseq is only fetched at very start of snapshot phase.
        // We want to save cseq right before we started snapshot
        // phase to guarantee we don't miss any new entries while snapshot
        // is in process
        if (initState && initState.cseq) {
            batchState.nextLogOffset = initState.cseq;
        }
        // only set this after snapshot phase is done.
        // `initState` is only set during snapshot phase.
        if (!initState) {
            batchState.nextLogOffset =
            logRes.info.start + logStats.nbLogRecordsRead;
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
                        { method: 'LogReader._processPublishEntries',
                          topic,
                          entryCount: topicEntries.length,
                          error: err });
                    return done(err);
                }
                logger.debug('entries published successfully to topic',
                               { method: 'LogReader._processPublishEntries',
                                 topic, entryCount: topicEntries.length });
                batchState.publishedEntries[topic] = topicEntries;
                return done();
            });
        }, done);
    }

    _processSaveLogOffset(batchState, done) {
        const { initState, logger } = batchState;

        async.series([
            next => this._writeInitState(initState, logger, next),
            next => {
                if (batchState.nextLogOffset !== undefined &&
                    batchState.nextLogOffset !== this.logOffset) {
                    if (batchState.nextLogOffset > this.logOffset) {
                        this.logOffset = batchState.nextLogOffset;
                    }
                    return this._writeLogOffset(logger, done);
                }
                return process.nextTick(next);
            },
        ], done);
    }

    getLogInfo() {
        return { raftId: this.raftId };
    }

    getTargetZenkoBucketName() {
        return this._targetZenkoBucket;
    }
}

module.exports = IngestionReader;
