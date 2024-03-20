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

describe('UpdateReplicationStatus._getUpdatedSourceEntry()', () => {
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

describe('UpdateReplicationStatus._getNFSUpdatedSourceEntry()', () => {
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

describe('UpdateReplicationStatus._handleFailedReplicationEntry', () => {

    afterEach(() => {
        sinon.restore();
    });

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
