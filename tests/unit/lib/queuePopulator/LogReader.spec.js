const assert = require('assert');
const sinon = require('sinon');

const ZookeeperMock = require('zookeeper-mock');

const { errors } = require('arsenal');

const { Logger } = require('werelogs');

const LogReader = require('../../../../lib/queuePopulator/LogReader');


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
                versionId: undefined,
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
                versionId: undefined,
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
                            versionId: undefined,
                            ...params.overhead,
                        },
                    }));
                    done();
                });
            });
        });

        it('should shutdown when batch processing is stuck', done => {
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
                done();
            }, 2000);
        }).timeout(4000);

        it('should not shutdown if timeout not reached', done => {
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
                done();
            });
        });
    });
});
