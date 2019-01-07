const async = require('async');

const IngestionProducer = require('./IngestionProducer');
const LogReader = require('./LogReader');
const { decryptLocationSecret } = require('../management/index');

const util = require('util');
const jsutil = require('arsenal').jsutil;

// Temp testing
const RESET = '\x1b[0m';
// blue
const COLORME = '\x1b[35m';

function logMe(str) {
    console.log(COLORME, str, RESET);
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

        this.bucket = bucketdConfig.bucket;
        this.logger = logger;
        this.logger.info('initializing ingestion reader',
            { method: 'IngestionReader.constructor',
                bucketdConfig, raftId: this.raftId });
        this.newIngestion = false;
        this.remoteLogOffset = null;

        // source ingestion bucket
        this.bucket = bucketdConfig.bucket;
        // zenko bucket to ingest to
        this._targetZenkoBucket = bucketdConfig.name;
        // zenko location constraint to ingest to
        this._targetZenkoLocationConstraint = bucketdConfig.prefix;
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
                this.pathToLogOffset = `/${this._targetZenkoBucket}/logState` +
                    `/${this.logId}/logOffset`;

                return super.setup(done);
            });
        });
    }

    /* eslint-disable no-param-reassign */

    _processReadRecords(params, batchState, done) {
        logMe('in _processReadRecords')
        const { logger } = batchState;
        const readOptions = {};
        if (this.logOffset !== undefined) {
            readOptions.startSeq = this.logOffset;
        }
        if (params && params.maxRead !== undefined) {
            readOptions.limit = params.maxRead;
        }
        readOptions.limit = 20;
        logger.info('reading records', { readOptions });
        return async.waterfall([
            next => this._readLogOffset((err, res) => {
                const offset = Number.parseInt(res, 10);
                if (err) {
                    return next(err);
                }
                if (offset === 1) {
                    logMe('--=== NEW INGESTION ===--')
                    // this.newIngestion = true;
                    readOptions.startSeq = 1;
                }
                return next();
            }),
            next => {
                // if (!this.newIngestion) {
                    // logMe('not a new ingestion source..')
                    // logMe('Not a new ingestion, go with stream...')
                    // logMe(`LOGOFFSET: ${this.logOffset}`)
                    // logMe(`readOptions: ${readOptions.startSeq} | ${readOptions.limit}`)
                    return this._iProducer.getRaftLog(this.raftId,
                    readOptions.startSeq, readOptions.limit, false,
                    (err, data) => {
                        if (err) {
                            this.logger.error('Error retrieving logs', { err,
                                raftId: this.raftId, method:
                                'IngestionReader._processReadRecords' });
                            return next(err);
                        }
                        logMe(`raft id: ${this.raftId}, startseq: ${readOptions.startSeq}, limit: ${readOptions.limit}`)
                        this.logger.info('readRecords got raft logs', {
                            method: 'IngestionReader._processReadRecords',
                            params });
                        batchState.logRes = data;
                        return next();
                    });
                // }
                // logMe('Doing snapshot...')
                // return this._iProducer.snapshot(this.bucket, (err, res) => {
                //     if (err) {
                //         logger.error('error generating snapshot for ' +
                //         'ingestion', { err });
                //         return next(err);
                //     }
                //     batchState.logRes = { info: { start: 1 }, log: res };
                //     return next();
                // });
            },
        ], done);
    }

    // TODO: Processor requires the zenko bucket name
    //   (currently stored within source ingestion objects on key: `name`)
    _processLogEntry(batchState, record, entry) {
        // NOTE: Using zenkoName because should be unique to other entries.

        // for a "del", entry.value will not exist but we still need to
        // pass through the event
        // for a bucket metadata entry from s3Connector, there will be no
        // entry.key but there will be an entry.type and we
        // need to pass this through
        if (entry.key === undefined && entry.type === undefined) {
            logMe(`-> key: ${entry.key} | type: ${entry.type}`)
            return;
        }
        if (!record.db) {
            // console.log(entry)
            // console.log(JSON.stringify(entry))
            console.log('====')
            // console.log(util.inspect(record, { depth: 3 }));
            this._extensions.forEach(ext => ext.filter({
                type: entry.type,
                bucket: entry.bucket,
                key: entry.key,
                value: entry.value,
            }));
        } else {
            logMe(`-> RECORD.DB: ${record.db} \nENTRY.KEY: ${entry.key}\nENTRY.TYPE: ${entry.type}`)
            if (record.db === 'users..bucket') {
                const keySplit = entry.key.split('..|..');
                entry.key = `${keySplit[0]}..|..` +
                    `${this._targetZenkoBucket}`;
            } else if (record.db === 'metastore') {
                const keySplit = entry.key.split('/');
                entry.key =
                    `${keySplit[0]}/${this._targetZenkoBucket}`;
            } else {
                if (record.db === entry.key) {
                    entry.key = `${this._targetZenkoBucket}`;
                }
                record.db = `${this._targetZenkoBucket}`;
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
        logMe('in _processPrepareEntries')
        const { entriesToPublish, logRes, logStats, logger } = batchState;

        this._setEntryBatch(entriesToPublish);

        logMe(`IR._prepareEntries entriesToPublish: ${entriesToPublish.length}`)

        if (this.newIngestion) {
            // this._setEntryBatch(entriesToPublish);
            logMe(`NEW INGESTION: ${logRes.log.length}`)
            logRes.log.forEach(entry => {
                logStats.nbLogRecordsRead += 1;
                this._processLogEntry(batchState, entry, entry);
            });
            // this._unsetEntryBatch();
            // logMe('prepareEntries new ingestion..')
            logMe(`count: ${logStats.nbLogRecordsRead}`)
            return done();
        }
        if (logRes.info && logRes.info.start === null || logRes.log === null) {
            // logMe(`prepareEntries null... ${logRes.info.start === null} | ${logRes.log === null}`)
            logMe(`logRes.info.start is null? ${logRes.info.start === null}`)
            logMe(`logRes.log is null? ${logRes.log === null}`)
            return done(null);
        }

        // this._setEntryBatch(entriesToPublish);
        logMe('_processPrepareEntries stream stuff..')

        let shipBatchCb;
        const dataEventHandler = record => {
            logMe('got data..')
            logStats.nbLogRecordsRead += 1;
            // this._setEntryBatch(entriesToPublish);
            logMe(`number of entries: ${record.entries.length}`)
            record.entries.forEach(entry => {
                logStats.nbLogEntriesRead += 1;
                // logMe(JSON.stringify(util.inspect(entry, { depth: 4 })))
                logMe(`STREAM: ${record.db} | ${this.bucket}`)
                if (record.db === this.bucket) {
                    // logMe('STREAM: true')
                    logMe(JSON.stringify(entry))
                    this._processLogEntry(batchState, record, entry);
                }
                logMe(`nbLogEntriesRead: ${logStats.nbLogEntriesRead}`)
                if (logStats.nbLogRecordsRead >= batchState.maxRead) {
                    console.log('forceful stop')
                    shipBatchCb();
                }
            });
        }
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

        // const checker = setInterval(() => {
        //     const now = Date.now();
        //     if (now >= batchState.startTime + batchState.timeoutMs) {
        //         logger.debug('ending batch', {
        //             method: 'LogReader._processPrepareEntries',
        //             reason: 'batch timeout reached',
        //             readRecords: logStats.nbLogRecordsRead,
        //         });
        //         shipBatchCb();
        //     }
        // }, 1000);
        shipBatchCb = jsutil.once(done);
        logRes.log.on('info', info => {
            logger.debug('received record stream "info" event', {
                method: 'LogReader._processPrepareEntries',
                info,
            });
            logRes.info = info;
        });

        // logRes.log.on('data', record => {
        //     logMe('got data..')
        //     logStats.nbLogRecordsRead += 1;
        //     // this._setEntryBatch(entriesToPublish);
        //     logMe(`number of entries: ${record.entries.length}`)
        //     record.entries.forEach(entry => {
        //         logStats.nbLogEntriesRead += 1;
        //         // logMe(JSON.stringify(util.inspect(entry, { depth: 4 })))
        //         logMe(`STREAM: ${record.db} | ${this.bucket}`)
        //         if (record.db === this.bucket) {
        //             // logMe('STREAM: true')
        //             logMe(JSON.stringify(entry))
        //             this._processLogEntry(batchState, record, entry);
        //         }
        //         logMe(`nbLogEntriesRead: ${logStats.nbLogEntriesRead}`)
        //     });
        //     // this._unsetEntryBatch();
        // });
        // logRes.log.on('error', err => {
        //     console.log('AM I GETTING AN ERROR HERE??')
        //     logger.error('error fetching entries from log',
        //         { method: 'LogReader._processPrepareEntries',
        //             error: err });
        //     return done(err);
        // });
        // logRes.log.on('end', () => {
        //     logger.info('ending record stream');
        //     return done();
        // });
        // return undefined;
    }

    _processPublishEntries(batchState, done) {
        logMe('in _processPublishEntries')
        const { entriesToPublish, logRes, logStats, logger } = batchState;

        if (this.remoteLogOffset /* &&
        config.queuePopulator.logSource.indexOf('ingestion') > -1 */) {
            batchState.nextLogOffset = this.remoteLogOffset;
            // return done();
        } else {
            batchState.nextLogOffset =
            logRes.info.start + logStats.nbLogRecordsRead;
        }

        return async.each(Object.keys(entriesToPublish), (topic, done) => {
            const topicEntries = entriesToPublish[topic];
            if (topicEntries.length === 0) {
                return done();
            }
            // console.log('=======')
            // console.log(`INGESTION READER: ${topic}`)
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
        logMe('in _processSaveLogOffset')
        if (batchState.nextLogOffset !== undefined &&
            batchState.nextLogOffset !== this.logOffset) {
            if (batchState.nextLogOffset > this.logOffset) {
                this.logOffset = batchState.nextLogOffset;
            }
            this.newIngestion = false;
            this.remoteLogOffset = null;
            return this._writeLogOffset(batchState.logger, done);
        }
        return process.nextTick(() => done());
    }

    getLogInfo() {
        return { raftId: this.raftId };
    }

    getTargetZenkoBucketName() {
        return this._targetZenkoBucket;
    }
}

module.exports = IngestionReader;
