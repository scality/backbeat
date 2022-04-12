const assert = require('assert');
const errors = require('arsenal').errors;
const sinon = require('sinon');

const config = require('../../config.json');
const notificationConfig = require('../../config.notification.json');
const ReplicateObject =
    require('../../../extensions/replication/tasks/ReplicateObject');
const { sourceEntry } = require('../../utils/mockEntries');
const fakeLogger = require('../../utils/fakeLogger');
const { replicationEntry } = require('../../utils/kafkaEntries');
const configUtil = require('../../../extensions/notification/utils/config');

const retryConfig = { scality: { timeoutS: 300 } };

describe('MultipleBackendTask', function test() {
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
                    notificationConfig,
                }),
            });
            const publishNotificationStub = sinon.stub(task, '_publishFailedReplicationStatusNotification');
            const publishReplicationStatusStub = sinon.stub(task, '_publishReplicationStatus');
            task._handleReplicationOutcome(errors.InternalError, sourceEntry, sourceEntry, replicationEntry, fakeLogger,
                (err, data) => {
                assert.ifError(err);
                assert(data);
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
                    notificationConfig,
                }),
            });
            const publishNotificationStub = sinon.stub(task, '_publishFailedReplicationStatusNotification');
            const publishReplicationStatusStub = sinon.stub(task, '_publishReplicationStatus');
            task._handleReplicationOutcome(null, sourceEntry, sourceEntry, replicationEntry, fakeLogger,
                (err, data) => {
                assert.ifError(err);
                assert(data);
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
        it('should publish entry to notification topic in correct format', done => {
            sinon.stub(configUtil, 'validateEntry').returns(true);
            const notificationProducerSend = sinon.stub().callsFake((entries, cb) => cb());
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
                    notificationConfig,
                    notificationProducer: {
                        send: notificationProducerSend,
                    },
                    notificationConfigManager: {
                        getConfig: () => ({
                            key: 'value',
                        }),
                    }
                }),
            });
            sinon.stub(sourceEntry, 'getObjectVersionedKey').returns('example-key\u0000123456');
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
            task._publishFailedReplicationStatusNotification(sourceEntry, fakeLogger);
            const message = {
                dateTime: '0000',
                eventType: 's3:Replication:OperationFailedReplication',
                region: 'metastore',
                schemaVersion: '1',
                size: '6',
                versionId: '123456',
                bucket: 'example-bucket',
                key: 'example-key\u0000123456',
            };
            const entryPublished = {
                key: 'example-bucket',
                message: JSON.stringify(message),
            };
            assert(notificationProducerSend.calledOnceWith([entryPublished]));
            return done();
        });
    });
});
