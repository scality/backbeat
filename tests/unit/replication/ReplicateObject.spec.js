const assert = require('assert');
const errors = require('arsenal').errors;
const sinon = require('sinon');

const config = require('../../config.json');
const { notification } = require('../../config.notification.json').extensions;
const ReplicateObject =
    require('../../../extensions/replication/tasks/ReplicateObject');
const { sourceEntry } = require('../../utils/mockEntries');
const fakeLogger = require('../../utils/fakeLogger');
const { replicationEntry } = require('../../utils/kafkaEntries');
const configUtil = require('../../../extensions/notification/utils/config');

const retryConfig = { scality: { timeoutS: 300 } };

describe('ReplicateObject', function test() {
    this.timeout(5000);

    afterEach(() => {
        sinon.restore();
    });

    describe('::_handleReplicationOutcome', () => {
        it('should publish replication failure status to notification topic', done => {
            const task = new ReplicateObject({
                getStateVars: () => ({
                    repConfig: {
                        queueProcessor: {
                            retry: retryConfig,
                        },
                    },
                    sourceConfig: config.extensions.replication.source,
                    destConfig: config.extensions.replication.destination,
                    site: 'test-site-2',
                    notificationConfig: notification,
                    notificationConfigManager: {
                        getConfig: () => null
                    }
                }),
            });
            const publishNotificationStub = sinon.stub(task, '_publishFailedReplicationStatusNotification')
                .callsArg(2);
            const publishReplicationStatusStub = sinon.stub(task, '_publishReplicationStatus');
            task._handleReplicationOutcome(errors.InternalError, sourceEntry, sourceEntry, replicationEntry, fakeLogger,
                err => {
                assert.ifError(err);
                assert(publishReplicationStatusStub.calledOnceWith(sourceEntry, 'FAILED', {
                    log: fakeLogger,
                    reason: errors.InternalError.description,
                    kafkaEntry: replicationEntry,
                }));
                assert(publishNotificationStub.calledOnceWith(sourceEntry, fakeLogger));
                return done();
            });
        });

        it('should not publish replication failure to notification topic', done => {
            const task = new ReplicateObject({
                getStateVars: () => ({
                    repConfig: {
                        queueProcessor: {
                            retry: retryConfig,
                        },
                    },
                    sourceConfig: config.extensions.replication.source,
                    destConfig: config.extensions.replication.destination,
                    site: 'test-site-2',
                    notificationConfig: notification,
                    notificationConfigManager: {
                        getConfig: () => null
                    }
                }),
            });
            const publishNotificationStub = sinon.stub(task, '_publishFailedReplicationStatusNotification')
            .callsArg(2);
            const publishReplicationStatusStub = sinon.stub(task, '_publishReplicationStatus');
            task._handleReplicationOutcome(null, sourceEntry, sourceEntry, replicationEntry, fakeLogger,
                err => {
                assert.ifError(err);
                assert(publishReplicationStatusStub.calledOnceWith(sourceEntry, 'COMPLETED', {
                    log: fakeLogger,
                    kafkaEntry: replicationEntry,
                }));
                assert(publishNotificationStub.notCalled);
                return done();
            });
        });
    });

    describe('::_publishFailedReplicationStatusNotification', () => {
        it('should publish entry to notification destination topic in correct format', done => {
            const validateStub = sinon.stub(configUtil, 'validateEntry');
            validateStub.onCall(0).returns(true);
            validateStub.onCall(1).returns(false);
            const notificationProducerSend1 = sinon.stub().callsFake((entries, cb) => cb());
            const notificationProducerSend2 = sinon.stub().callsFake((entries, cb) => cb());
            const task = new ReplicateObject({
                getStateVars: () => ({
                    repConfig: {
                        queueProcessor: {
                            retry: retryConfig,
                        },
                    },
                    sourceConfig: config.extensions.replication.source,
                    destConfig: config.extensions.replication.destination,
                    site: 'test-site-2',
                    notificationConfig: notification,
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
                assert(notificationProducerSend1.calledOnceWith([entryPublished]));
                assert(notificationProducerSend2.notCalled);
                return done();
            });
        });
    });
});
