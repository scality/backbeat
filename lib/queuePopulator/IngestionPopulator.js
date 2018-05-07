const arsenal = require('arsenal');
const async = require('async');

const LogConsumer = arsenal.storage.metadata.bucketclient.LogConsumer;
const BucketClient = require('bucketclient').RESTClient;
const IngestionProducer = require('./IngestionProducer');
const LogReader = require('./LogReader');

class IngestionPopulator extends LogReader {
    constructor(params, raftId, source) {
        const { zkClient, kafkaConfig, logger, extensions, metricsProducer } = params;
        // const { host, port } = bucketdConfig;
        const host = source.host;
        const port = source.port;
        logger.info('initializing raft log reader',
            { method: 'RaftLogReader.constructor',
                raftId });
        const bucketClient = new BucketClient(`${host}:${port}`);
        const logConsumer = new LogConsumer({ bucketClient,
            raftSession: raftId,
            logger });
        super({ zkClient, kafkaConfig, logConsumer, logId: `raft_${raftId}`,
                logger, extensions, metricsProducer });
        this.raftId = raftId;
        this._iProducer = new IngestionProducer({ host, port });
        this.newIngestion = false;
        this.removeLogOffset = null;
    }

    populateIngestion(params, done) {
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
            next => this._grabSnapshot(params, batchState, next),
            next => this._processPrepareSnapshotEntry(batchState, next),
            next => this._processPublishSnapshotEntries(batchState, next),
            next => this._processSaveSnapshotOffset(batchState, next),
        ], err => {
            return done(err);
        });
    }

    _grabSnapshot(params, batchState, done) {
        return async.waterfall([
            next => this._readLogOffset((err, res) => {
                if (err) {
                    return next(err);
                }
                const offset = Number.parseInt(res, 10);
                if (offset === 1) {
                    this.newIngestion = true;
                }
                return next();
            }),
            next => {
                if (!this.newIngestion) {
                    return next();
                }
                this.remoteLogOffset = batchState.logRes.info.cseq + 1;
                return this._iProducer.snapshot(this.raftId, (err, res) => {
                    if (err) {
                        this.log.error('error generating snapshot for ' +
                        'ingestion', { err });
                        return next(err);
                    }
                    batchState.logRes.log = res;
                    return next();
                });
            },
        ], done);
    }

    _processSnapshotEntry(batchState, record, entry) {
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

    _processPrepareSnapshotEntry(batchState, done) {
        const { entriesToPublish, logRes, logStats } = batchState;

        this._setEntryBatch(entriesToPublish);

        if (this.newIngestion) {
            logRes.log.forEach(entry => {
                logStats.nbLogRecordsRead += 1;
                this._processSnapshotEntry(batchState, entry, entry);
            });
            return done();
        }
        return undefined;
    }

    _processPublishSnapshotEntries(batchState, done) {
        const { entriesToPublish, logRes, logStats } = batchState;
        if (this.remoteLogoffset) {
            batchState.nextLogOffset = this.removeLogOffset;
        }
        return async.each(Object.keys(entriesToPublish), (topic, done) => {
            const topicEntries = entriesToPublish[topic];
            if (topicEntries.length === 0) {
                console.log('THERE ARE NO ENTRIES');
                return done();
            }
            return async.series([
                done => this._setupProducer(topic, done),
                done => this._producers[topic].send(topicEntries, done),
            ], err => {
                if (err) {
                    this.log.error(
                        'error publishing entries from log to topic',
                        { method: 'IngestionPopulator._processPublishEntries',
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
            if (err) {
                return done(err);
            }
            // const crrExtension = this._extensions.find(ext => (
            //     ext instanceof ReplicationQueuePopulator
            // ));
            // const extMetrics = crrExtension.getAndResetMetrics();
            // if (Object.keys(extMetrics).length > 0) {
            //     this._mProducer.publishMetrics(extMetrics, metricsTypeQueued,
            //         metricsExtension, () => {});
            // }
            return done();
        });
    }

    _processSaveSnapshotOffset(batchState, done) {
        if (batchState.nextLogOffset !== undefined &&
            batchState.nextLogOffset !== this.logOffset) {
            if (batchState.nextLogOffset > this.logOffset) {
                this.logOffset = batchState.nextLogOffset;
            }
            this.newIngestion = false;
            this.removeLogOffset = null;
            return this._writeLogOffset(done);
        }
        return process.nextTick(() => done());
    }
}

module.exports = IngestionPopulator;
