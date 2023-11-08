const assert = require('assert');
const sinon = require('sinon');

const UpdateReplicationStatus =
    require('../../../extensions/replication/tasks/UpdateReplicationStatus');
const ReplicationStatusProcessor =
    require('../../../extensions/replication/replicationStatusProcessor/ReplicationStatusProcessor');
const QueueEntry = require('../../../lib/models/QueueEntry');
const { replicationEntry } = require('../../utils/kafkaEntries');
const { sourceEntry } = require('../../utils/mockEntries');

const { Logger } = require('werelogs');
const log = new Logger('test:UpdateReplicationStatus');

const config = require('../../config.json');
const fakeLogger = require('../../utils/fakeLogger');
const configUtil = require('../../../extensions/notification/utils/config');
const { notification } = require('../../config.notification.json').extensions;

function getCompletedEntry() {
    return QueueEntry.createFromKafkaEntry(replicationEntry)
        .toCompletedEntry('sf')
        .toCompletedEntry('replicationaws')
        .setSite('sf');
}

function getRefreshedEntry() {
    return QueueEntry.createFromKafkaEntry(replicationEntry).setSite('sf');
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

    it('should publish replication failure notification when replay is disabled', done => {
        const task = new UpdateReplicationStatus({
            getStateVars: () => ({
                repConfig: {
                    replicationStatusProcessor: {},
                    monitorReplicationFailures: false,
                },
                sourceConfig: config.extensions.replication.source,
                destConfig: config.extensions.replication.destination,
                bucketNotificationConfig: notification,
                notificationConfigManager: {
                    getConfig: () => null
                },
            }),
        }, metricHandlers);
        const publishNotificationStub = sinon.stub(task, '_publishFailedReplicationStatusNotification');
        const pushRelayEntryStub = sinon.stub(task, '_pushReplayEntry');
        const getReplayCountStub = sinon.stub(sourceEntry, 'getReplayCount');

        task._handleFailedReplicationEntry(sourceEntry, sourceEntry, 'test-site-2', fakeLogger);
        assert(getReplayCountStub.notCalled);
        assert(pushRelayEntryStub.notCalled);
        assert(publishNotificationStub.calledOnceWith(sourceEntry, fakeLogger));
        return done();
    });

    it('should publish replication failure notification when replayCount is 0', done => {
        const task = new UpdateReplicationStatus({
            getStateVars: () => ({
                repConfig: {
                    replicationStatusProcessor: {},
                    monitorReplicationFailures: false,
                },
                sourceConfig: config.extensions.replication.source,
                destConfig: config.extensions.replication.destination,
                bucketNotificationConfig: notification,
                notificationConfigManager: {
                    getConfig: () => null
                },
                replayTopics: ['replay-topic'],
            }),
        }, metricHandlers);
        const publishNotificationStub = sinon.stub(task, '_publishFailedReplicationStatusNotification');
        const pushRelayEntryStub = sinon.stub(task, '_pushReplayEntry');
        sinon.stub(sourceEntry, 'getReplayCount').returns(0);

        task._handleFailedReplicationEntry(sourceEntry, sourceEntry, 'test-site-2', fakeLogger);
        assert(pushRelayEntryStub.notCalled);
        assert(publishNotificationStub.calledOnceWith(sourceEntry, fakeLogger));
        return done();
    });

    it('should not publish replication failure notification on first replay', done => {
        const task = new UpdateReplicationStatus({
            getStateVars: () => ({
                repConfig: {
                    replicationStatusProcessor: {},
                    monitorReplicationFailures: false,
                },
                sourceConfig: config.extensions.replication.source,
                destConfig: config.extensions.replication.destination,
                bucketNotificationConfig: notification,
                notificationConfigManager: {
                    getConfig: () => null
                },
                replayTopics: ['replay-topic'],
            }),
        }, metricHandlers);
        const publishNotificationStub = sinon.stub(task, '_publishFailedReplicationStatusNotification');
        const pushRelayEntryStub = sinon.stub(task, '_pushReplayEntry');
        sinon.stub(sourceEntry, 'getReplayCount').returns(null);

        task._handleFailedReplicationEntry(sourceEntry, sourceEntry, 'test-site-2', fakeLogger);
        assert(pushRelayEntryStub.calledOnceWith(sourceEntry, 'test-site-2', fakeLogger));
        assert(publishNotificationStub.notCalled);
        return done();
    });

    it('should not publish replication failure notification on next replay', done => {
        const task = new UpdateReplicationStatus({
            getStateVars: () => ({
                repConfig: {
                    replicationStatusProcessor: {},
                    monitorReplicationFailures: false,
                },
                sourceConfig: config.extensions.replication.source,
                destConfig: config.extensions.replication.destination,
                bucketNotificationConfig: notification,
                notificationConfigManager: {
                    getConfig: () => null
                },
                replayTopics: ['replay-topic'],
            }),
        }, metricHandlers);
        const publishNotificationStub = sinon.stub(task, '_publishFailedReplicationStatusNotification');
        const pushRelayEntryStub = sinon.stub(task, '_pushReplayEntry');
        sinon.stub(sourceEntry, 'getReplayCount').returns(1);

        task._handleFailedReplicationEntry(sourceEntry, sourceEntry, 'test-site-2', fakeLogger);
        assert(pushRelayEntryStub.calledOnceWith(sourceEntry, 'test-site-2', fakeLogger));
        assert(publishNotificationStub.notCalled);
        return done();
    });
});

describe('UpdateReplicationStatus._publishFailedReplicationStatusNotification', () => {

    afterEach(() => {
        sinon.restore();
    });

    it('should publish entry to notification destination topic in correct format', done => {
        const validateStub = sinon.stub(configUtil, 'validateEntry');
        validateStub.onCall(0).returns(true);
        validateStub.onCall(1).returns(false);
        const notificationProducerSend1 = sinon.stub().callsFake((entries, cb) => cb());
        const notificationProducerSend2 = sinon.stub().callsFake((entries, cb) => cb());
        const task = new UpdateReplicationStatus({
            getStateVars: () => ({
                repConfig: {
                    replicationStatusProcessor: {},
                    monitorReplicationFailures: false,
                },
                sourceConfig: config.extensions.replication.source,
                destConfig: config.extensions.replication.destination,
                bucketNotificationConfig: notification,
                notificationProducers: {
                    destination1: {
                        send: notificationProducerSend1
                    },
                    destination2: {
                        send: notificationProducerSend2
                    },
                },
                notificationConfigManager: {
                    getConfig: async () => ({
                        queueConfig: [
                            {
                                events: [
                                    's3:ObjectCreated:*',
                                    's3:ObjectRemoved:*',
                                    's3:Replication:OperationFailedReplication'
                                ],
                                queueArn: 'arn:scality:bucketnotif:::destination1',
                                id: 'NjhiYzg2YTQtZmM3NS00YTJlLWJkMWYtMzcwZjk0MjA5YzAz'
                            },
                            {
                                events: [
                                    's3:ObjectCreated:*',
                                    's3:ObjectRemoved:*',
                                ],
                                queueArn: 'arn:scality:bucketnotif:::destination2',
                                id: 'NjhiYzg2YTQtZmM3NS00YTJlLWJkMWYtMzcwZjkfvjA5YzAz'
                            }
                        ],
                    }),
                }
            }),
        });
        sinon.stub(sourceEntry, 'getObjectKey').returns('example-key');
        sinon.stub(sourceEntry, 'getVersionId').returns('123456');
        sinon.stub(sourceEntry, 'getBucket').returns('example-bucket');
        sinon.stub(sourceEntry, 'getValue').returns({
            'last-modified': '0000',
            'originOp': 'origin-operation',
            'dataStoreName': 'metastore',
            'md-model-version': '1',
            'content-length': '6',
            'versionId': '1234',
        });
        const message = {
            dateTime: '0000',
            eventType: 's3:Replication:OperationFailedReplication',
            region: 'metastore',
            schemaVersion: '1',
            size: '6',
            versionId: '123456',
            bucket: 'example-bucket',
            key: 'example-key',
        };
        const entryPublished = {
            key: 'example-bucket',
            message: JSON.stringify(message),
        };
        task._publishFailedReplicationStatusNotification(sourceEntry, fakeLogger, err => {
            assert.ifError(err);
            assert(validateStub.callCount === 2);
            assert(notificationProducerSend1.calledOnceWith([entryPublished]));
            assert(notificationProducerSend2.notCalled);
            return done();
        });
    });
});
