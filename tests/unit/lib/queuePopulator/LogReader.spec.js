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

    it('Should strip metadata v1 prefixes from object entries', done => {
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
        };
        const masterEntry = {
            type: 'example-type',
            key: '\x7fMexample-key',
            value: 'example-value'
        };
        const versionEntry = {
            type: 'example-type',
            key: '\x7fVexample-key',
            value: 'example-value'
        };
        logReaderWithExtension._processLogEntry({}, record, masterEntry);
        logReaderWithExtension._processLogEntry({}, record, versionEntry);
        const expectedArgs = {
            type: 'example-type',
            bucket: 'example-bucket',
            key: 'example-key',
            value: 'example-value',
            logReader: logReaderWithExtension,
        };
        assert(mockExtension.filter.firstCall.calledWith(expectedArgs));
        assert(mockExtension.filter.secondCall.calledWith(expectedArgs));
        done();
    });

    it('Should not change keys of objects in v0 format', done => {
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
        };
        const masterEntry = {
            type: 'example-type',
            key: 'fMexample-key',
            value: 'example-value'
        };
        const versionEntry = {
            type: 'example-type',
            key: 'fVexample-key',
            value: 'example-value'
        };
        logReaderWithExtension._processLogEntry({}, record, masterEntry);
        logReaderWithExtension._processLogEntry({}, record, versionEntry);
        const expectedArgs = {
            type: 'example-type',
            bucket: 'example-bucket',
            key: 'fMexample-key',
            value: 'example-value',
            logReader: logReaderWithExtension,
        };
        assert(mockExtension.filter.firstCall.calledWith(expectedArgs));
        expectedArgs.key = 'fVexample-key';
        assert(mockExtension.filter.secondCall.calledWith(expectedArgs));
        done();
    });

    it('Should add timestamp if got delete event and extension is notification', done => {
        const mockExtension = {
            constructor: {
                name: 'NotificationQueuePopulator'
            },
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
            timestamp: 'YYYY-MM-DD:HH-MM-SS',
            entry: {
                type: 'delete',
                key: 'fMexample-key',
            }
        };
        logReaderWithExtension._processLogEntry({}, record, record.entry);
        const expectedArgs = {
            type: 'delete',
            bucket: 'example-bucket',
            key: 'fMexample-key',
            value: JSON.stringify({
                'last-modified': 'YYYY-MM-DD:HH-MM-SS',
            }),
            logReader: logReaderWithExtension,
        };
        assert(mockExtension.filter.calledWith(expectedArgs));
        done();
    });

    it('Should not modify entry value if already defined', done => {
        const mockExtension = {
            constructor: {
                name: 'NotificationQueuePopulator'
            },
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
            timestamp: 'timestamp-value',
            entry: {
                type: 'example-type',
                key: 'fMexample-key',
                value: {
                    'last-modified': 'YYYY-MM-DD:HH-MM-SS',
                }
            }
        };
        logReaderWithExtension._processLogEntry({}, record, record.entry);
        const expectedArgs = {
            type: 'example-type',
            bucket: 'example-bucket',
            key: 'fMexample-key',
            value: {
                'last-modified': 'YYYY-MM-DD:HH-MM-SS',
            },
            logReader: logReaderWithExtension,
        };
        assert(mockExtension.filter.calledWith(expectedArgs));
        done();
    });

    it('Should skip filtering if value is undefined and extension not notification', done => {
        const mockExtension = {
            constructor: {
                name: 'ReplicationQueuePopulator'
            },
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
            timestamp: 'YYYY-MM-DD:HH-MM-SS',
            entry: {
                type: 'example-type',
                key: 'fMexample-key',
            }
        };
        logReaderWithExtension._processLogEntry({}, record, record.entry);
        assert(mockExtension.filter.notCalled);
        done();
    });

    describe('_processFilterEntries', () => {
        it('Should do nothing if no records where pushed', done => {
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

        it('Should process all records', done => {
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
        it('Should do nothing if record is empty', done => {
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

        it('Should process record', done => {
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
        it('Should process all record entries', done => {
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
});
