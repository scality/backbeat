const async = require('async');

const IngestionProducer = require('./IngestionProducer');
const LogReader = require('./LogReader');
const { decryptLocationSecret } = require('../management/index');

const util = require('util');

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
        this.logger.info('initializing ingestion reader',
            { method: 'IngestionReader.constructor',
                bucketdConfig, raftId: this.raftId });
        this.newIngestion = false;
        this.remoteLogOffset = null;

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
            next => this._readLogOffset((err, res) => {
                const offset = Number.parseInt(res, 10);
                if (err) {
                    return next(err);
                }
                if (offset === 1) {
                    this.newIngestion = true;
                }
                return next();
            }),
            next => {
                if (!this.newIngestion) {
                    return this._iProducer.getRaftLog(this.raftId,
                    readOptions.startSeq, readOptions.limit, false,
                    (err, data) => {
                        if (err) {
                            this.log.error('error retrieving logs',
                            { err, raftId: this.raftId, method:
                            'IngestionReader._processReadRecords' });
                            return next(err);
                        }
                        this.log.debug('readRecords got raft logs', {
                            method: 'IngestionReader._processReadRecords',
                            params });
                        batchState.logRes = data;
                        return next();
                    });
                }
                return this._iProducer.snapshot(this.bucket, (err, res) => {
                    if (err) {
                        logger.error('error generating snapshot for ' +
                        'ingestion', { err });
                        return next(err);
                    }
                    batchState.logRes = { info: { start: 1 }, log: res };
                    return next();
                });
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
            if (record.db === 'users..bucket') {
                const keySplit = entry.key.split('..|..');
                entry.key = `${keySplit[0]}..|..` +
                    `${this._targetZenkoBucket}-${keySplit[1]}`;
            } else if (record.db === 'metastore') {
                const keySplit = entry.key.split('/');
                entry.key =
                    `${keySplit[0]}/${this._targetZenkoBucket}-${keySplit[1]}`;
            } else {
                if (record.db === entry.key) {
                    entry.key = `${this._targetZenkoBucket}-${entry.key}`;
                }
                record.db = `${this._targetZenkoBucket}-${record.db}`;
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
        const { entriesToPublish, logRes, logStats, logger } = batchState;

        this._setEntryBatch(entriesToPublish);

        if (this.newIngestion) {
            logRes.log.forEach(entry => {
                logStats.nbLogRecordsRead += 1;
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
                if (entry.bucket === this.bucket) {
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
            console.log('=======')
            console.log(`INGESTION READER: ${topic}`)
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
