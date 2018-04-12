const arsenal = require('arsenal');
const async = require('async');
const LogConsumer = arsenal.storage.metadata.bucketclient.LogConsumer;
const BucketClient = require('bucketclient').RESTClient;
const ReplicationQueuePopulator =
    require('../../extensions/replication/ReplicationQueuePopulator');
const IngestionProducer = require('./IngestionProducer');

const { metricsExtension, metricsTypeQueued } =
    require('../../extensions/replication/constants');

const config = require('../../conf/Config');
const LogReader = require('./LogReader');

class RaftLogReader extends LogReader {
    constructor(params) {
        const { zkClient, kafkaConfig, bucketdConfig,
                raftId, logger, extensions, metricsProducer } = params;
        const { host, port } = bucketdConfig;
        logger.info('initializing raft log reader',
            { method: 'RaftLogReader.constructor',
                bucketdConfig, raftId });
        const bucketClient = new BucketClient(`${host}:${port}`);
        const logConsumer = new LogConsumer({ bucketClient,
            raftSession: raftId,
            logger });
        super({ zkClient, kafkaConfig, logConsumer, logId: `raft_${raftId}`,
                logger, extensions, metricsProducer });
        this.raftId = raftId;
        this._iProducer = new IngestionProducer;
        this.newIngestion = false;
        this.removeLogOffset = null;
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
        console.log('readOptions', readOptions, this.raftId);
        return async.waterfall([
            next => this.logConsumer.readRecords(readOptions, (err, res) => {
                if (err) {
                    this.log.error(
                        'error while reading log records',
                        { method: 'LogReader._processReadRecords',
                            params, error: err });
                    return next(err);
                }
                this.log.debug(
                    'readRecords callback',
                    { method: 'LogReader._processReadRecords',
                        params, info: res.info });
                batchState.logRes = res;
                return next();
            }),
            next => {
                return this._readLogOffset((err, res) => {
                    if (err) {
                        return next(err);
                    }
                    if (res === 1) {
                        this.newIngestion = true;
                    }
                    return next();
                });
            },
            next => {
                console.log('check', config.queuePopulator.logSource.indexOf('ingestion'), this.logOffset);
                if (!this.newIngestion) {
                    console.log('this is a new ingestion', this.newIngestion);
                    return next();
                }
                // if (config.queuePopulator.logSource.indexOf('ingestion') > -1 && this.raftId === '0') {
                this.remoteLogOffset = batchState.logRes.info.cseq + 1;
                console.log('taking initial log snapshot');
                return this._iProducer.snapshot(this.raftId, (err, res) => {
                    if (err) {
                        this.log.error('error generating snapshot for ' +
                        'ingestion', { err });
                        return next(err);
                    }
                    console.log('NUMBER OF THINGS FROM SNAPSHOT', res.length);
                    batchState.logRes.log = res;
                    console.log(batchState.logRes.log);
                    return next();
                });
                // } else if (config.queuePopulator.logSource.indexOf('ingestion') === -1 || this.raftId !== 0) {
                //     console.log('RAFT ID IS', this.raftId);
                //     this.remoteLogOffset = batchState.logRes.info.cseq + 1;
                //     batchState.logRes.log = null;
                //     this.newIngestion = false;
                // }
                // return next();
            },
        ], done);
    }

    _processLogEntry(batchState, record, entry) {
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
            this._extensions.forEach(ext => ext.filter({
                type: entry.type,
                bucket: record.db,
                key: entry.key,
                value: entry.value,
            }));
        }
    }

    _processPrepareEntries(batchState, done) {
        const { entriesToPublish, logRes, logStats } = batchState;

        this._setEntryBatch(entriesToPublish);

        if (this.newIngestion/* && config.queuePopulator.logSource.indexOf('ingestion') > -1*/) {
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

        console.log('logRes.log', logRes.log);

        logRes.log.on('data', record => {
            logStats.nbLogRecordsRead += 1;
            record.entries.forEach(entry => {
                logStats.nbLogEntriesRead += 1;

                this._processLogEntry(batchState, record, entry);
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
            return done();
        });
        return undefined;
    }

    _processPublishEntries(batchState, done) {
        const { entriesToPublish, logRes, logStats } = batchState;

        if (this.remoteLogOffset /* &&
        config.queuePopulator.logSource.indexOf('ingestion') > -1 */) {
            console.log('CHANGING NEXT LOG OFFSET, remotelogoffset');
            batchState.nextLogOffset = this.remoteLogOffset;
            // return done();
        } else {
            console.log('CHANGING NEXT LOG OFFSET, normal logs');
            console.log('info start,', logRes.info.start);
            console.log('nbLogRecordsRead', logStats.nbLogRecordsRead);
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
            console.log('THIS LOGOFFSET', this.logOffset);
            console.log('BATCH STATE NEXT LOG OFFSET', batchState.nextLogOffset);
            if (batchState.nextLogOffset > this.logOffset) {
                this.logOffset = batchState.nextLogOffset;
            }
            this.newIngestion = false;
            this.remoteLogOffset = null;
            return this._writeLogOffset(done);
        }
        return process.nextTick(() => done());
    }

    /* eslint-enable no-param-reassign */

    getLogInfo() {
        return { raftId: this.raftId };
    }
}

module.exports = RaftLogReader;
