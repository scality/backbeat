const assert = require('assert');
const sinon = require('sinon');

const UpdateReplicationStatus =
    require('../../../extensions/replication/tasks/UpdateReplicationStatus');
const ReplicationStatusProcessor =
    require('../../../extensions/replication/replicationStatusProcessor/ReplicationStatusProcessor');
const QueueEntry = require('../../../lib/models/QueueEntry');
const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const { replicationEntry } = require('../../utils/kafkaEntries');

const { Logger } = require('werelogs');
const log = new Logger('test:UpdateReplicationStatus');

const config = require('../../config.json');
const fakeLogger = require('../../utils/fakeLogger');
const { ObjectMD } = require('arsenal/build/lib/models');

function getCompletedEntry() {
    return QueueEntry.createFromKafkaEntry(replicationEntry)
        .toCompletedEntry('sf')
        .toCompletedEntry('replicationaws')
        .setSite('sf')
        .setOriginOp('s3:ObjectCreated:Put');
}

function getRefreshedEntry() {
    return QueueEntry.createFromKafkaEntry(replicationEntry)
        .setSite('sf')
        .setOriginOp('s3:ObjectCreated:Put');
}

function checkReplicationInfo(site, status, updatedSourceEntry) {
    assert.strictEqual(
        updatedSourceEntry.getReplicationSiteStatus(site), status);
}

const metricHandlers = ReplicationStatusProcessor.loadMetricHandlers(
    config.extensions.replication);

const rspMock = {
    getStateVars: () => ({
        repConfig: {
            replicationStatusProcessor: {},
        },
        sourceConfig: {
            auth: {},
            s3: {
                host: 'localhost',
                port: 8000,
            },
            transport: 'http',
        },
    }),
};

describe('UpdateReplicationStatus', () => {

    afterEach(() => {
        sinon.restore();
    });

    describe('_getUpdatedSourceEntry()', () => {
        const updateReplicationStatus = new UpdateReplicationStatus(rspMock, metricHandlers);

        it('should return a COMPLETED entry when site status is PENDING', () => {
            const sourceEntry = getCompletedEntry();
            const refreshedEntry = getRefreshedEntry();
            const updatedSourceEntry = updateReplicationStatus
                  ._getUpdatedSourceEntry({ sourceEntry, refreshedEntry }, log);
            checkReplicationInfo('sf', 'COMPLETED', updatedSourceEntry);
            checkReplicationInfo('replicationaws', 'PENDING', updatedSourceEntry);
            assert.strictEqual(updatedSourceEntry.getOriginOp(), 's3:Replication:OperationCompletedReplication');
        });

        it('should return null when site status is COMPLETED and existing site ' +
        'status is COMPLETED', () => {
            const sourceEntry = getCompletedEntry();
            const refreshedEntry = getCompletedEntry();
            const updatedSourceEntry = updateReplicationStatus
                  ._getUpdatedSourceEntry({ sourceEntry, refreshedEntry }, log);
            assert.strictEqual(updatedSourceEntry, null);
        });

        it('should return null when site status is FAILED and existing site ' +
        'status is COMPLETED', () => {
            const sourceEntry = getCompletedEntry()
                  .toFailedEntry('sf')
                  .setSite('sf');
            const refreshedEntry = getCompletedEntry();
            const updatedSourceEntry = updateReplicationStatus
                  ._getUpdatedSourceEntry({ sourceEntry, refreshedEntry }, log);
            assert.strictEqual(updatedSourceEntry, null);
        });
    });

    describe('_getNFSUpdatedSourceEntry()', () => {
        const updateReplicationStatus = new UpdateReplicationStatus(rspMock, metricHandlers);

        it('should return a COMPLETED entry when metadata has not changed', () => {
            const sourceEntry = getCompletedEntry();
            const refreshedEntry = getRefreshedEntry();
            const updatedSourceEntry = updateReplicationStatus
                ._getNFSUpdatedSourceEntry(sourceEntry, refreshedEntry);
            checkReplicationInfo('sf', 'COMPLETED', updatedSourceEntry);
            checkReplicationInfo('replicationaws', 'PENDING', updatedSourceEntry);
        });

        it('should return a PENDING entry when MD5 mismatch', () => {
            const sourceEntry = getCompletedEntry();
            const refreshedEntry = getRefreshedEntry();
            refreshedEntry.setContentMd5('d41d8cd98f00b204e9800998ecf8427e');
            const updatedSourceEntry = updateReplicationStatus
                ._getNFSUpdatedSourceEntry(sourceEntry, refreshedEntry);
            checkReplicationInfo('sf', 'PENDING', updatedSourceEntry);
            checkReplicationInfo('replicationaws', 'PENDING', updatedSourceEntry);
        });

        it('should return a PENDING entry when tags mismatch', () => {
            const sourceEntry = getCompletedEntry();
            const refreshedEntry = getRefreshedEntry();
            refreshedEntry.setTags({ key: 'value' });
            const updatedSourceEntry = updateReplicationStatus
                ._getNFSUpdatedSourceEntry(sourceEntry, refreshedEntry);
            checkReplicationInfo('sf', 'PENDING', updatedSourceEntry);
            checkReplicationInfo('replicationaws', 'PENDING', updatedSourceEntry);
        });
    });

    describe('_handleFailedReplicationEntry', () => {
        it('should trigger replication failure notification when replay is disabled', done => {
            const task = new UpdateReplicationStatus({
                getStateVars: () => ({
                    repConfig: {
                        replicationStatusProcessor: {},
                        monitorReplicationFailures: false,
                    },
                    sourceConfig: config.extensions.replication.source,
                    destConfig: config.extensions.replication.destination,
                }),
            }, metricHandlers);
            const entry = new ObjectQueueEntry('test', '', new ObjectMD());
            const pushRelayEntryStub = sinon.stub(task, '_pushReplayEntry');
            const getReplayCountStub = sinon.stub(entry, 'getReplayCount');

            const outEntry = task._handleFailedReplicationEntry(entry, entry, 'test-site-2', fakeLogger);
            assert(getReplayCountStub.notCalled);
            assert(pushRelayEntryStub.notCalled);
            assert.strictEqual(outEntry.getOriginOp(), 's3:Replication:OperationFailedReplication');
            return done();
        });

        it('should trigger replication failure notification when replayCount is 0', done => {
            const task = new UpdateReplicationStatus({
                getStateVars: () => ({
                    repConfig: {
                        replicationStatusProcessor: {},
                        monitorReplicationFailures: false,
                    },
                    sourceConfig: config.extensions.replication.source,
                    destConfig: config.extensions.replication.destination,
                    replayTopics: ['replay-topic'],
                }),
            }, metricHandlers);
            const entry = new ObjectQueueEntry('test', '', new ObjectMD());
            const pushRelayEntryStub = sinon.stub(task, '_pushReplayEntry');
            sinon.stub(entry, 'getReplayCount').returns(0);

            const outEntry = task._handleFailedReplicationEntry(entry, entry, 'test-site-2', fakeLogger);
            assert(pushRelayEntryStub.notCalled);
            assert.strictEqual(outEntry.getOriginOp(), 's3:Replication:OperationFailedReplication');
            return done();
        });

        it('should not trigger replication failure notification on first replay', done => {
            const task = new UpdateReplicationStatus({
                getStateVars: () => ({
                    repConfig: {
                        replicationStatusProcessor: {},
                        monitorReplicationFailures: false,
                    },
                    sourceConfig: config.extensions.replication.source,
                    destConfig: config.extensions.replication.destination,
                    replayTopics: ['replay-topic'],
                }),
            }, metricHandlers);
            const entry = new ObjectQueueEntry('test', '', new ObjectMD());
            const pushRelayEntryStub = sinon.stub(task, '_pushReplayEntry');
            sinon.stub(entry, 'getReplayCount').returns(null);

            const outEntry = task._handleFailedReplicationEntry(entry, entry, 'test-site-2', fakeLogger);
            assert(pushRelayEntryStub.calledOnceWith(entry, 'test-site-2', fakeLogger));
            assert(!outEntry);
            return done();
        });

        it('should not trigger replication failure notification on next replay', done => {
            const task = new UpdateReplicationStatus({
                getStateVars: () => ({
                    repConfig: {
                        replicationStatusProcessor: {},
                        monitorReplicationFailures: false,
                    },
                    sourceConfig: config.extensions.replication.source,
                    destConfig: config.extensions.replication.destination,
                    replayTopics: ['replay-topic'],
                }),
            }, metricHandlers);
            const entry = new ObjectQueueEntry('test', '', new ObjectMD());
            const pushRelayEntryStub = sinon.stub(task, '_pushReplayEntry');
            sinon.stub(entry, 'getReplayCount').returns(1);

            const outEntry = task._handleFailedReplicationEntry(entry, entry, 'test-site-2', fakeLogger);
            assert(pushRelayEntryStub.calledOnceWith(entry, 'test-site-2', fakeLogger));
            assert(!outEntry);
            return done();
        });
    });

    describe('_pushFailedEntry', () => {
        [
            {
                description: 'should include the role in the failed entry member',
                replicationInfo: {
                    role: 'replication',
                    status: 'SUCCESS',
                    backends: [{
                            site: 'azure',
                            status: 'SUCCESS',
                            dataStoreVersionId: '123456',
                    }],
                },
                expectedMember: 'test::746573742d6d79764964:replication',
            }, {
                description: 'should include the site in the failed entry member',
                replicationInfo: {
                    status: 'SUCCESS',
                    backends: [{
                            site: 'azure',
                            status: 'SUCCESS',
                            dataStoreVersionId: '123456',
                    }],
                },
                expectedMember: 'test::746573742d6d79764964',
            }
        ].forEach(opts => {
            it(opts.description, () => {
                const publishFailedCRREntry = sinon.stub().returns();
                const currentDate = new Date().getTime();
                const task = new UpdateReplicationStatus({
                    getStateVars: () => ({
                        repConfig: {
                            replicationStatusProcessor: {},
                        },
                        sourceConfig: {
                            transport: 'https',
                            s3: {
                                host: 'localhost',
                                port: 8000,
                            },
                            auth: {},
                        },
                        failedCRRProducer: {
                            publishFailedCRREntry,
                        },
                        statsClient: {
                            getSortedSetCurrentHour: () => currentDate,
                        },
                    }),
                }, metricHandlers);
                const objectMD = {
                    'md-model-version': 2,
                    'owner-display-name': 'Bart',
                    'owner-id': '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                    'x-amz-storage-class': 'STANDARD',
                    'content-length': 542,
                    'content-type': 'text/plain',
                    'last-modified': '2017-07-13T02:44:25.515Z',
                    'content-md5': '01064f35c238bd2b785e34508c3d27f4',
                    'key': 'test',
                    'isDeleteMarker': false,
                    'isNull': false,
                    'dataStoreName': 'STANDARD',
                    'originOp': 's3:ObjectRestore:Post',
                    'versionId': 'test-myvId',
                };
                objectMD.replicationInfo = opts.replicationInfo;
                const entry = new ObjectQueueEntry('test', '', objectMD);
                task._pushFailedEntry(entry);
                const message = JSON.parse(publishFailedCRREntry.getCall(0).args.at(0));
                assert.strictEqual(message.key, `test:bb:crr:failed:null:${currentDate}`);
                assert.strictEqual(message.member, opts.expectedMember);
            });
        });
    });
});
