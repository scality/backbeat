const assert = require('assert');
const sinon = require('sinon');
const stream = require('stream');

const ZookeeperMock = require('zookeeper-mock');

const { errors } = require('arsenal');

const { Logger } = require('werelogs');

const LogReader = require('../../../../lib/queuePopulator/LogReader');
const KafkaLogReader = require('../../../../lib/queuePopulator/KafkaLogReader');
const BucketFileLogReader = require('../../../../lib/queuePopulator/BucketFileLogReader');
const RaftLogReader = require('../../../../lib/queuePopulator/RaftLogReader');
const MongoLogReader = require('../../../../lib/queuePopulator/MongoLogReader');


class MockLogConsumer {
    constructor(params) {
        this.params = params || {};
    }

    readRecords(params, cb) {
        process.nextTick(() => {
            if (this.params.readRecordsError) {
                cb(this.params.readRecordsError);
            } else {
                cb(null, {});
            }
        });
    }
}

class MockRecordStream extends stream.PassThrough {
    constructor() {
        super({ objectMode: true });
    }
}

describe('LogReader', () => {
    let zkMock;
    let logReader;

    beforeEach(() => {
        zkMock = new ZookeeperMock();
        logReader = new LogReader({
            logId: 'test-log-reader',
            zkClient: zkMock.createClient('localhost:2181'),
            logConsumer: new MockLogConsumer(),
            logger: new Logger('test:LogReader'),
        });
        sinon.restore();
    });

    // Currently the initial offset is set to 1 with mongodb backend,
    // it looks odd considering mongodb uses random IDs as log cursors
    // and could be cleaned up, but this test coming from 7.4 branch
    // with a raft log reader still makes sense to improve coverage in
    // the current code, so keeping it.
    it('should start from offset 1 if no zookeeper log offset', done => {
        logReader.setup(err => {
            assert.ifError(err);
            assert.strictEqual(logReader.logOffset, 1);
            done();
        });
    });

    // TODO there is currently no initialization of log offset with
    // mongodb backend, re-enable when implementing initial log offset
    // fetching with mongodb backend.
    it.skip('should start from offset 1 on log consumer readRecords error', done => {
        const errorLogReader = new LogReader({
            logId: 'test-log-reader',
            zkClient: zkMock.createClient('localhost:2181'),
            logConsumer: new MockLogConsumer({
                readRecordsError: errors.InternalError,
            }),
            logger: new Logger('test:ErrorLogReader'),
        });
        errorLogReader.setup(err => {
            assert.ifError(err);
            assert.strictEqual(errorLogReader.logOffset, 1);
            done();
        });
    });

    it('should strip metadata v1 prefixes from object entries', done => {
        const mockExtension = {
            filter: sinon.spy(),
        };
        const logReaderWithExtension = new LogReader({
            logId: 'test-log-reader',
            zkClient: zkMock.createClient('localhost:2181'),
            logConsumer: new MockLogConsumer(),
            logger: new Logger('test:logReaderWithExtension'),
            extensions: [mockExtension]
        });
        const record = {
            db: 'example-bucket',
            timestamp: Date.now(),
        };
        const masterEntry = {
            type: 'example-type',
            key: '\x7fMexample-key',
            value: 'example-value',
            timestamp: '2023-11-29T15:05:57.065Z',
        };
        const versionEntry = {
            type: 'example-type',
            key: '\x7fVexample-key',
            value: 'example-value',
            timestamp: '2023-11-29T15:05:57.065Z',
        };
        logReaderWithExtension._processLogEntry({}, record, masterEntry);
        logReaderWithExtension._processLogEntry({}, record, versionEntry);
        const expectedArgs = {
            type: 'example-type',
            bucket: 'example-bucket',
            key: 'example-key',
            value: 'example-value',
            logReader: logReaderWithExtension,
            overheadFields: {
                commitTimestamp: record.timestamp,
                opTimestamp: '2023-11-29T15:05:57.065Z',
            },
        };
        assert(mockExtension.filter.firstCall.calledWith(expectedArgs));
        assert(mockExtension.filter.secondCall.calledWith(expectedArgs));
        done();
    });

    it('should not change keys of objects in v0 format', done => {
        const mockExtension = {
            filter: sinon.spy(),
        };
        const logReaderWithExtension = new LogReader({
            logId: 'test-log-reader',
            zkClient: zkMock.createClient('localhost:2181'),
            logConsumer: new MockLogConsumer(),
            logger: new Logger('test:logReaderWithExtension'),
            extensions: [mockExtension]
        });
        const record = {
            db: 'example-bucket',
            timestamp: Date.now(),
        };
        const masterEntry = {
            type: 'example-type',
            key: 'fMexample-key',
            value: 'example-value',
            timestamp: '2023-11-29T15:05:57.065Z',
        };
        const versionEntry = {
            type: 'example-type',
            key: 'fVexample-key',
            value: 'example-value',
            timestamp: '2023-11-29T15:05:57.065Z',
        };
        logReaderWithExtension._processLogEntry({}, record, masterEntry);
        logReaderWithExtension._processLogEntry({}, record, versionEntry);
        const expectedArgs = {
            type: 'example-type',
            bucket: 'example-bucket',
            key: 'fMexample-key',
            value: 'example-value',
            logReader: logReaderWithExtension,
            overheadFields: {
                commitTimestamp: record.timestamp,
                opTimestamp: '2023-11-29T15:05:57.065Z',
            },
        };
        assert(mockExtension.filter.firstCall.calledWith(expectedArgs));
        expectedArgs.key = 'fVexample-key';
        assert(mockExtension.filter.secondCall.calledWith(expectedArgs));
        done();
    });

    describe('_processFilterEntries', () => {
        it('should do nothing if no records where pushed', done => {
            const batchState = {
                currentRecords: [],
            };
            const processFilterEntryStb = sinon.stub(logReader, '_processFilterEntry');
            logReader._processFilterEntries(batchState, err => {
                assert.ifError(err);
                assert(processFilterEntryStb.notCalled);
                return done();
            });
        });

        it('should process all records', done => {
            const batchState = {
                currentRecords: [1, 2],
            };
            const processFilterEntryStb = sinon.stub(logReader, '_processFilterEntry')
                .callsArg(2);
            logReader._processFilterEntries(batchState, err => {
                assert.ifError(err);
                assert(processFilterEntryStb.calledTwice);
                return done();
            });
        });
    });

    describe('_readLogOffset', () => {
        [true, false].forEach(managed => {
            it(`should ${managed ? '' : 'not '}read offset from ` +
                `zookeeper when offset is ${managed ? '' : 'not '}managed`, done => {
                    const offsetManagedStub = sinon.stub(logReader, 'isOffsetManaged').returns(managed);
                    const zkClientStub = sinon.stub(logReader.zkClient, 'getData').yields();
                    logReader._readLogOffset(err => {
                        assert.ifError(err);
                        assert(offsetManagedStub.calledOnce);
                        assert(managed ? zkClientStub.calledOnce : zkClientStub.notCalled);
                        done();
                    });
                });

        });
    });

    describe('_processFilterEntry', () => {
        it('should do nothing if record is empty', done => {
            const batchState = {
                entriesToPublish: {},
            };
            const filterEntriesStb = sinon.stub(logReader, '_filterEntries');
            logReader._processFilterEntry(batchState, {}, err => {
                assert.ifError(err);
                assert(filterEntriesStb.notCalled);
                return done();
            });
        });

        it('should process record', done => {
            const batchState = {
                entriesToPublish: {},
            };
            const record = {
                entries: [1]
            };
            const setEntryBatchStb = sinon.stub(logReader, '_setEntryBatch');
            const unsetEntryBatchStb = sinon.stub(logReader, '_unsetEntryBatch');
            const filterEntriesStb = sinon.stub(logReader, '_filterEntries')
                .callsArg(2);
            logReader._processFilterEntry(batchState, record,  err => {
                assert.ifError(err);
                assert(filterEntriesStb.calledOnce);
                assert(setEntryBatchStb.calledOnce);
                assert(unsetEntryBatchStb.calledOnce);
                return done();
            });
        });
    });

    describe('_filterEntries', () => {
        it('should process all record entries', done => {
            const batchState = {
                logStats: {
                    nbLogEntriesRead: 0,
                },
            };
            const record = {
                entries: [1, 2]
            };
            const processLogEntryStb = sinon.stub(logReader, '_processLogEntry')
                .callsArg(3);
            logReader._filterEntries(batchState, record,  err => {
                assert.ifError(err);
                assert(processLogEntryStb.calledTwice);
                assert.strictEqual(batchState.logStats.nbLogEntriesRead, 2);
                return done();
            });
        });
    });

    describe('_processLogEntry', () => {
        [
            {
                description: 'without overhead fields',
                overhead: null,
            }, {
                description: 'with overhead fields',
                overhead: {
                    versionId: '1234',
                },
            }
        ].forEach(params => {
            it(`should pass the proper fields to the filter method (${params.description})`, done => {
                const date = Date.now();
                const record = {
                    db: 'example-bucket',
                    timestamp: date,
                };
                const entry = {
                    type: 'put',
                    key: 'example-key',
                    timestamp: date,
                    value: null,
                    overhead: params.overhead,
                };
                logReader._extensions = [
                    {
                        filter: sinon.stub().returns(),
                    },
                ];
                logReader._processLogEntry({}, record, entry, err => {
                    assert.ifError(err);
                    assert(logReader._extensions[0].filter.calledWithExactly({
                        type: 'put',
                        bucket: 'example-bucket',
                        key: 'example-key',
                        value: null,
                        logReader,
                        overheadFields: {
                            commitTimestamp: date,
                            opTimestamp: date,
                            ...params.overhead,
                        },
                    }));
                    done();
                });
            });
        });
    });

    describe('processLogEntries', () => {
        it('should shutdown when batch processing is stuck and CRASH_ON_BATCH_TIMEOUT is set', done => {
            process.env.CRASH_ON_BATCH_TIMEOUT = true;
            logReader._batchTimeoutSeconds = 1;
            // logReader will become stuck as _processReadRecords will never
            // call the callback
            sinon.stub(logReader, '_processReadRecords').returns();
            let emmitted = false;
            process.once('SIGTERM', () => {
                emmitted = true;
            });
            logReader.processLogEntries({}, () => {});
            setTimeout(() => {
                assert.strictEqual(emmitted, true);
                delete process.env.CRASH_ON_BATCH_TIMEOUT;
                done();
            }, 2000);
        }).timeout(4000);

        it('should fail healthcheck when batch processing is stuck', done => {
            delete process.env.CRASH_ON_BATCH_TIMEOUT;
            logReader._batchTimeoutSeconds = 1;
            // logReader will become stuck as _processReadRecords will never
            // call the callback
            sinon.stub(logReader, '_processReadRecords').returns();
            let emmitted = false;
            process.once('SIGTERM', () => {
                emmitted = true;
            });
            logReader.processLogEntries({}, () => {});
            setTimeout(() => {
                assert.strictEqual(emmitted, false);
                assert.strictEqual(logReader.batchProcessTimedOut(), true);
                done();
            }, 2000);
        }).timeout(4000);

        it('should not shutdown if timeout not reached', done => {
            process.env.CRASH_ON_BATCH_TIMEOUT = true;
            sinon.stub(logReader, '_processReadRecords').yields();
            sinon.stub(logReader, '_processPrepareEntries').yields();
            sinon.stub(logReader, '_processFilterEntries').yields();
            sinon.stub(logReader, '_processPublishEntries').yields();
            sinon.stub(logReader, '_processSaveLogOffset').yields();
            let emmitted = false;
            process.once('SIGTERM', () => {
                emmitted = true;
            });
            logReader.processLogEntries({}, () => {
                assert.strictEqual(emmitted, false);
                delete process.env.CRASH_ON_BATCH_TIMEOUT;
                done();
            });
        });

        it('should not fail healthcheck if timeout not reached', done => {
            delete process.env.CRASH_ON_BATCH_TIMEOUT;
            sinon.stub(logReader, '_processReadRecords').yields();
            sinon.stub(logReader, '_processPrepareEntries').yields();
            sinon.stub(logReader, '_processFilterEntries').yields();
            sinon.stub(logReader, '_processPublishEntries').yields();
            sinon.stub(logReader, '_processSaveLogOffset').yields();
            logReader.processLogEntries({}, () => {
                assert.strictEqual(logReader.batchProcessTimedOut(), false);
                done();
            });
        });

        it('should ignore operations with method === 10', done => {
            const filteredEntries = [];
            const reader = new LogReader({
                logId: 'test-log-reader',
                zkClient: zkMock.createClient('localhost:2181'),
                logConsumer: new MockLogConsumer(),
                extensions: [{
                    filter: entry => { filteredEntries.push(entry); },
                    setBatch: () => {},
                    unsetBatch: () => {},
                }],
                logger: new Logger('test:logReader'),
            });
            reader._processReadRecords = (params, batchState, done) => done();
            reader._processSaveLogOffset = (batchState, done) => done();
            reader._processPrepareEntries = (batchState, done) => {
                // eslint-disable-next-line no-param-reassign
                batchState.logRes = {
                    info: {
                        cseq: 12345,
                    },
                    log: {},
                };
                batchState.currentRecords.push({
                    db: 'db',
                    method: 8,
                    entries: [
                        {
                            type: 'put',
                            key: 'key',
                            value: '{}',
                        },
                    ],
                    timestamp: 't1',
                });
                batchState.currentRecords.push({
                    db: 'db',
                    method: 10,
                    entries: [
                        {
                            type: 'bucket_migration',
                            value: '{}',
                        },
                    ],
                    timestamp: 't2',
                });
                done();
            };

            reader.processLogEntries({}, () => {
                assert.deepStrictEqual(filteredEntries,
                    [
                        {
                            type: 'put',
                            bucket: 'db',
                            key: 'key',
                            value: '{}',
                            logReader: reader,
                            overheadFields: {
                                opTimestamp: undefined,
                                commitTimestamp: 't1',
                            },
                        },
                    ]
                );
                done();
            });
        });
    });

    describe('_processPrepareEntries', () => {
        it('should consume "batchState.maxRead" logs from tailable stream', done => {
            const batchState = {
                logRes: {
                    log: new MockRecordStream(),
                    tailable: true,
                },
                logStats: {
                    nbLogRecordsRead: 0,
                    nbLogEntriesRead: 0,
                    hasMoreLog: false,
                },
                entriesToPublish: {},
                publishedEntries: {},
                currentRecords: [],
                maxRead: 3,
                startTime: Date.now(),
                timeoutMs: 60000,
                logger: logReader.log,
            };
            for (let i = 0; i < 5; ++i) {
                batchState.logRes.log.write({
                    entries: [],
                });
            }
            logReader._processPrepareEntries(batchState, err => {
                assert.ifError(err);
                assert.strictEqual(batchState.logStats.nbLogRecordsRead, 3);
                assert.strictEqual(batchState.logStats.hasMoreLog, true);
                done();
            });
        });

        it('should consume and return if tailable stream doesn\'t have enough records', done => {
            const batchState = {
                logRes: {
                    log: new MockRecordStream(),
                    tailable: true,
                },
                logStats: {
                    nbLogRecordsRead: 0,
                    nbLogEntriesRead: 0,
                    hasMoreLog: false,
                },
                entriesToPublish: {},
                publishedEntries: {},
                currentRecords: [],
                maxRead: 30,
                startTime: Date.now(),
                timeoutMs: 100,
                logger: logReader.log,
            };
            for (let i = 0; i < 3; ++i) {
                batchState.logRes.log.write({
                    entries: [],
                });
            }
            logReader._processPrepareEntries(batchState, err => {
                assert.ifError(err);
                assert.strictEqual(batchState.logStats.nbLogRecordsRead, 3);
                assert.strictEqual(batchState.logStats.hasMoreLog, false);
                done();
            });
        });

        it('should consume all logs from a non tailable streams', done => {
            const batchState = {
                logRes: {
                    log: new MockRecordStream(),
                    tailable: false,
                },
                logStats: {
                    nbLogRecordsRead: 0,
                    nbLogEntriesRead: 0,
                    hasMoreLog: false,
                },
                entriesToPublish: {},
                publishedEntries: {},
                currentRecords: [],
                maxRead: 3,
                startTime: Date.now(),
                timeoutMs: 60000,
                logger: logReader.log,
            };
            for (let i = 0; i < 3; ++i) {
                batchState.logRes.log.write({
                    entries: [],
                });
            }
            batchState.logRes.log.end();
            logReader._processPrepareEntries(batchState, err => {
                assert.ifError(err);
                assert.strictEqual(batchState.logStats.nbLogRecordsRead, 3);
                assert.strictEqual(batchState.logStats.hasMoreLog, true);
                done();
            });
        });

        it('should set hasMoreLog to false when a non tailable streams doesn\'t have enough records', done => {
            const batchState = {
                logRes: {
                    log: new MockRecordStream(),
                    tailable: false,
                },
                logStats: {
                    nbLogRecordsRead: 0,
                    nbLogEntriesRead: 0,
                    hasMoreLog: false,
                },
                entriesToPublish: {},
                publishedEntries: {},
                currentRecords: [],
                maxRead: 5,
                startTime: Date.now(),
                timeoutMs: 60000,
                logger: logReader.log,
            };
            for (let i = 0; i < 3; ++i) {
                batchState.logRes.log.write({
                    entries: [],
                });
            }
            batchState.logRes.log.end();
            logReader._processPrepareEntries(batchState, err => {
                assert.ifError(err);
                assert.strictEqual(batchState.logStats.nbLogRecordsRead, 3);
                assert.strictEqual(batchState.logStats.hasMoreLog, false);
                done();
            });
        });
    });

    describe('_processSaveLogOffset', () => {
        [
            {
                desc: 'should save the offset when offsets are managed',
                managed: true,
                currentOffset: 0,
                nextLogOffset: 1,
                shouldSave: true,
            },
            {
                desc: 'should not save the offset when the next offset is undefined',
                managed: true,
                currentOffset: 0,
                nextLogOffset: undefined,
                shouldSave: false,
            },
            {
                desc: 'should not save the offset if the offset did not change',
                managed: true,
                currentOffset: 0,
                nextLogOffset: 0,
                shouldSave: false,
            },
            {
                desc: 'should not save the offset if offsets are not managed',
                managed: false,
                currentOffset: 0,
                nextLogOffset: 1,
                shouldSave: false,
            },
        ].forEach(params => {
            it(params.desc, done => {
                const batchState = {
                    logRes: {
                        log: new MockRecordStream(),
                        tailable: true,
                    },
                    logStats: {
                        nbLogRecordsRead: 0,
                        nbLogEntriesRead: 0,
                        hasMoreLog: false,
                    },
                    nextLogOffset: params.nextLogOffset,
                    entriesToPublish: {},
                    publishedEntries: {},
                    currentRecords: [],
                    maxRead: 3,
                    startTime: Date.now(),
                    timeoutMs: 60000,
                    logger: logReader.log,
                };
                sinon.stub(logReader, 'logOffset').value(params.currentOffset);
                const offsetManagedStub = sinon.stub(logReader, 'isOffsetManaged').returns(params.managed);
                const writeLogOffsetStub = sinon.stub(logReader, '_writeLogOffset').yields();
                logReader._processSaveLogOffset(batchState, err => {
                    assert.ifError(err);
                    assert(offsetManagedStub.calledOnce);
                    assert(params.shouldSave ? writeLogOffsetStub.calledOnce : writeLogOffsetStub.notCalled);
                    done();
                });
            });
        });
    });

    describe('getMetricLabels', () => {
        [{
            name: 'KafkaLogReader',
            Reader: KafkaLogReader,
            config: {
                kafkaConfig: {
                    hosts: 'localhost:9092',
                },
                qpKafkaConfig: {
                    logName: 'test-log',
                },
            },
            logName: 'kafka-log',
        }, {
            name: 'BucketFileLogReader',
            Reader: BucketFileLogReader,
            config: {
                dmdConfig: {
                    logName: 'test-log',
                    host: 'localhost',
                    port: 8000,
                },
            },
            logName: 'bucket-file',
        }, {
            name: 'RaftLogReader',
            Reader: RaftLogReader,
            config: {
                raftId: 'test-log',
                bucketdConfig: {
                    host: 'localhost',
                    port: 8000,
                },
            },
            logName: 'raft-log',
        }, {
            name: 'MongoLogReader',
            Reader: MongoLogReader,
            config: {
                mongoConfig: {
                    logName: 'test-log',
                    host: 'localhost',
                    port: 8000,
                },
            },
            logName: 'mongo-log',
        }].forEach(params => {
            it(`should return proper ${params.name} metrics labels`, () => {
                const reader = new params.Reader({
                    ...params.config,
                    logger: new Logger('test:LogReader'),
                    extensionNames: 'replication,lifecycle,notification',
                });
                const expectedLabels = {
                    logId: 'test-log',
                    logName: params.logName,
                    origin: 'replication,lifecycle,notification',
                };
                assert.deepStrictEqual(reader.getMetricLabels(), expectedLabels);
            });
        });
    });

    describe('isOffsetManaged', () => {
        [{
            name: 'KafkaLogReader',
            Reader: KafkaLogReader,
            config: {
                kafkaConfig: {
                    hosts: 'localhost:9092',
                },
                qpKafkaConfig: {
                    logName: 'test-log',
                },
            },
            expected: false,
        }, {
            name: 'BucketFileLogReader',
            Reader: BucketFileLogReader,
            config: {
                dmdConfig: {
                    logName: 'test-log',
                    host: 'localhost',
                    port: 8000,
                },
            },
            expected: true,
        }, {
            name: 'RaftLogReader',
            Reader: RaftLogReader,
            config: {
                raftId: 'test-log',
                bucketdConfig: {
                    host: 'localhost',
                    port: 8000,
                },
            },
            expected: true,
        }, {
            name: 'MongoLogReader',
            Reader: MongoLogReader,
            config: {
                mongoConfig: {
                    logName: 'test-log',
                    host: 'localhost',
                    port: 8000,
                },
            },
            expected: true,
        }].forEach(params => {
            it(`should ${params.expected ? '' : 'not'} manage ${params.name} offsets`, () => {
                const reader = new params.Reader({
                    ...params.config,
                    logger: new Logger('test:LogReader'),
                    extensionNames: 'replication,lifecycle,notification',
                });
                assert.deepStrictEqual(reader.isOffsetManaged(), params.expected);
            });
        });
    });
});
