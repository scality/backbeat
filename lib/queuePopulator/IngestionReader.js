const async = require('async');
const { errors } = require('arsenal');
const VID_SEP = require('arsenal').versioning.VersioningConstants
          .VersionId.Separator;

const IngestionProducer = require('./IngestionProducer');
const LogReader = require('./LogReader');
const { decryptLocationSecret } = require('../management/index');
const {
    metricsExtension,
    metricsTypeQueued
} = require('../../extensions/ingestion/constants');

function _isVersionedLogKey(key) {
    return key.split(VID_SEP)[1] !== undefined;
}

class IngestionReader extends LogReader {
    constructor(params) {
        const { zkClient, ingestionConfig, kafkaConfig, bucketdConfig, qpConfig,
            logger, extensions, producer, metricsProducer, s3Config } = params;
        super({ zkClient, kafkaConfig, logConsumer: {}, logId: '',
                logger, extensions, metricsProducer });
        this._ingestionConfig = ingestionConfig;
        this.qpConfig = qpConfig;
        this.s3Config = s3Config;
        this.bucketdConfig = bucketdConfig;
        this.logger = logger;
        this._producer = producer;

        // source ingestion bucket
        this.bucket = bucketdConfig.bucket;
        // zenko bucket to ingest to
        this._targetZenkoBucket = bucketdConfig.name;

        const ingestionPath = this._ingestionConfig.zookeeperPath;
        this.zkBasePath = `${ingestionPath}/${this._targetZenkoBucket}`;
        this.bucketInitPath = `${this.zkBasePath}/init`;
        this.pathToLogOffset = null;
        this.raftId = null;
        this.logId = null;
        this._batchInProgress = false;
        this._shouldProcessInitState = true;
    }

    /**
     * static method to return a list of ingestion init nodes used in zookeeper
     * @return {Array} - array of ingestion init nodes as strings
     */
    static getInitIngestionNodes() {
        return ['isStatusComplete', 'versionMarker', 'keyMarker'];
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
                    `${this.zkBasePath}/logState/${this.logId}/logOffset`;

                return super.setup(done);
            });
        });
    }

    /**
     * Get the init (snapshot) state for this given IngestionReader
     * @param {Logger.newRequestLogger} logger - request logger object
     * @param {function} done - callback(error, object)
     *   where object.versionMarker is the NextVersionIdMarker
     *   where object.keyMarker is the NextKeyMarker
     * @return {undefined}
     */
    _readInitState(logger, done) {
        const initPathNodes = IngestionReader.getInitIngestionNodes();

        if (this._shouldProcessInitState === false) {
            return done(null, {
                isStatusComplete: true,
            });
        }

        return async.map(initPathNodes, (pathNode, cb) => {
            const path = `${this.bucketInitPath}/${pathNode}`;
            return this.zkClient.getData(path, (err, data) => {
                if (err) {
                    if (err.name !== 'NO_NODE') {
                        logger.error(
                            'Could not fetch ingestion init state',
                            { method: 'IngestionReader._readInitState',
                              zkPath: path,
                              error: err });
                        return cb(err);
                    }
                    return this.zkClient.mkdirp(path, err => {
                        if (err) {
                            logger.error(
                                'Could not pre-create path in zookeeper',
                                { method: 'IngestionReader._readInitState',
                                  zkPath: path,
                                  error: err });
                            return cb(err);
                        }
                        return cb();
                    });
                }
                const d = data && data.toString();
                logger.debug('fetched ingestion init state node', {
                    method: 'IngestionReader._readInitState',
                    zkPath: path,
                    data: d,
                });
                return cb(null, d);
            });
        }, (err, data) => {
            if (err) {
                return done(err);
            }
            const [isStatusComplete, versionMarker, keyMarker] = data;
            return done(null, {
                isStatusComplete: isStatusComplete === 'true',
                versionMarker,
                keyMarker
            });
        });
    }

    /**
     * Set the init (snapshot) state for this given IngestionReader
     * @param {object} initState - initState (snapshot) for ingestion
     * @param {boolean} initState.isStatusComplete - true/false
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
        const initPathNodes = IngestionReader.getInitIngestionNodes();

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
                if (pathNode === 'isStatusComplete' && data[pathNode]) {
                    this._shouldProcessInitState = false;
                }

                return cb();
            });
        }, done);
    }

    processLogEntries(params, done) {
        this._batchInProgress = true;

        super.processLogEntries(params, (err, hasMoreLog) => {
            this._batchInProgress = false;
            if (err) {
                return done(err);
            }
            if (hasMoreLog) {
                // keep ingesting new log without waiting
                return this.processLogEntries(params, done);
            }
            return done();
        });
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
            next => this._readInitState(logger, next),
            (state, next) => {
                if (state.isStatusComplete) {
                    return this._iProducer.getRaftLog(this.raftId,
                    readOptions.startSeq, readOptions.limit, false,
                    (err, data) => {
                        if (err) {
                            logger.error('Error retrieving logs', { err,
                                raftId: this.raftId, method:
                                'IngestionReader._processReadRecords' });
                            return next(err);
                        }
                        logger.debug('readRecords got raft logs', {
                            method: 'IngestionReader._processReadRecords',
                            params });
                        batchState.logRes = data;
                        return next();
                    });
                }
                return this._iProducer.snapshot(this.bucket, state,
                (err, res) => {
                    if (err) {
                        logger.error('error generating snapshot for ' +
                        'ingestion', {
                            error: err,
                            method: 'IngestionReader._processReadRecords',
                        });
                        return next(err);
                    }
                    if (!res) {
                        logger.error('failed to get metadata logs', {
                            method: 'IngestionReader._processReadRecords',
                        });
                        return next(errors.InternalError);
                    }
                    batchState.logRes = { info: { start: 1 }, log: res.logRes };
                    batchState.initState = res.initState;
                    return next();
                });
            },
        ], done);
    }

    _processLogEntry(batchState, record, entry) {
        const {
            entriesToPublish
        } = batchState;
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
            }, entriesToPublish));
        } else {
            let key;
            let db;
            if (record.db === 'users..bucket') {
                const keySplit = entry.key.split('..|..');
                key = `${keySplit[0]}..|..${this._targetZenkoBucket}`;
            } else if (record.db === 'metastore') {
                const keySplit = entry.key.split('/');
                key = `${keySplit[0]}/${this._targetZenkoBucket}`;
            } else {
                if (record.db === entry.key) {
                    key = this._targetZenkoBucket;
                }
                db = this._targetZenkoBucket;
            }
            if (db === undefined) {
                db = record.db;
            }
            if (key === undefined) {
                key = entry.key;
            }

            this._extensions.forEach(ext => ext.filter({
                type: entry.type,
                bucket: db,
                key,
                value: entry.value,
            }, entriesToPublish));
        }
    }

    _processPrepareEntries(batchState, done) {
        const {
            logRes, logStats, logger, initState,
        } = batchState;

        // if logRes.log is empty (empty listObjectVersions listing), skip
        if (!logRes.log) {
            return done();
        }
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
            if (logRes.info.start + logStats.nbLogRecordsRead
                <= logRes.info.cseq) {
                logger.debug('there is more log to read');
                logStats.hasMoreLog = true;
            }
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
            console.log('topicEntries!!!', topicEntries);
            return this._producer.send(topicEntries, err => {
                if (err) {
                    logger.error('error publishing entries from log to topic', {
                        method: 'LogReader._processPublishEntries',
                        topic,
                        entryCount: topicEntries.length,
                        error: err,
                    });
                    return done(err);
                }
                logger.debug('entries published successfully to topic',
                               { method: 'LogReader._processPublishEntries',
                                 topic, entryCount: topicEntries.length });
                batchState.publishedEntries[topic] = topicEntries;
                return done();
            });
        }, err => {
            if (err) {
                return done(err);
            }
            this._publishMetrics();
            return done();
        });
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

    _publishMetrics() {
        // Ingestion extensions is a single IngestionQueuePopulatorExt
        const extension = this._extensions[0];
        const location = this.getLocationConstraint();
        const metric = extension.getAndResetMetrics(this._targetZenkoBucket);
        if (metric && metric.ops > 0) {
            const value = { [location]: metric };
            this._mProducer.publishMetrics(value, metricsTypeQueued,
                metricsExtension, () => {});
        }
    }


    /**
     * Bucket configs have user editable fields: credentials, endpoint
     * This method will detect if a change has occurred. If a change occurred,
     * update relevant instance variables and reinstantiate any clients
     * affected by the change.
     * @param {Object} sourceInfo - latest bucketdConfig information
     * @param {Function} done - callback(error)
     * @return {undefined}
     */
    refresh(sourceInfo, done) {
        const bucketdConfig = this._getEditableFields(this.bucketdConfig);
        const latestBucketdConfig = this._getEditableFields(sourceInfo);
        const updated = bucketdConfig !== latestBucketdConfig;

        if (updated) {
            // update instance variables
            this.bucketdConfig = sourceInfo;
            // update clients
            return this._setupIngestionProducer(done);
        }
        return done();
    }

    /**
     * Helper method to fetch an bucketdConfig object of only editable fields
     * following a specific format.
     * Editable fields: auth.accessKey, auth.secretKey, host, port, https
     * @param {Object} info - bucketdConfig information
     * @return {String} editableInfo as a string
     */
    _getEditableFields(info) {
        return JSON.stringify({
            accessKey: info.auth && info.auth.accessKey,
            secretKey: info.auth && info.auth.secretKey,
            host: info.host,
            port: info.port,
            https: info.https,
        });
    }

    getLogInfo() {
        return { raftId: this.raftId };
    }

    getTargetZenkoBucketName() {
        return this._targetZenkoBucket;
    }

    getLocationConstraint() {
        return this.bucketdConfig.locationConstraint;
    }

    isBatchInProgress() {
        return this._batchInProgress;
    }
}

module.exports = IngestionReader;
