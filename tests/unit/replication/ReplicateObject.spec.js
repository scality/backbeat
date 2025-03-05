const assert = require('assert');
const sinon = require('sinon');

const QueueEntry = require('../../../lib/models/QueueEntry');
const ReplicateObject = require('../../../extensions/replication/tasks/ReplicateObject');

const { replicationEntry } = require('../../utils/kafkaEntries');
const fakeLogger = require('../../utils/fakeLogger');

describe('ReplicateObject', () => {
    let task;

    beforeEach(() => {
        task = new ReplicateObject({
            getStateVars: () => ({
                site: 'site',
                repConfig: {
                    queueProcessor: {
                        retry: {
                            scality: {
                                maxRetries: 3,
                            }
                        },
                    },
                },
                destConfig: {
                    auth: {
                        site: 'zenko',
                        type: 'assumeRole',
                        sts: {
                            host: 'sts.enpoint.com',
                            port: 80
                        },
                    },
                    bootstrapList: [{
                        site: 'site',
                        servers: ['localhost:9095'],
                    }]
                },
                destHosts: {
                    pickNextHost: () => 'localhost:9095',
                }
            }),
        });
    });

    describe('_setTargetAccountMd', () => {
        it('should skip gettin target account info when auth type is assumeRole', done => {
            sinon.stub(task, '_setupDestClients').returns();
            const setTargetAccountStub = sinon.stub(task, '_setTargetAccountMdOnce').yields();
            task._setTargetAccountMd({}, '', fakeLogger, err => {
                assert.ifError(err);
                assert(setTargetAccountStub.notCalled);
                done();
            });
        });

        it('should get target account info', done => {
            sinon.stub(task, '_setupDestClients').returns();
            const setTargetAccountStub = sinon.stub(task, '_setTargetAccountMdOnce').yields();
            task.destConfig.auth = {
                type: 'service',
                account: 'replication-service',
            };
            task._setTargetAccountMd({ getLogInfo: () => {} }, '', fakeLogger, err => {
                assert.ifError(err);
                assert(setTargetAccountStub.calledOnce);
                done();
            });
        });
    });

    describe('_putMetadataOnce', () => {
        it('should pass extract accountId from role and pass it when using AssumeRole auth', done => {
            sinon.stub(task, '_publishMetadataWriteMetrics').returns();
            const entry = QueueEntry.createFromKafkaEntry(replicationEntry);
            task.backbeatDest = {
                putMetadata: sinon.stub().returns({
                    send: sinon.stub().yields(),
                    on: sinon.stub(),
                }),
            };
            task.targetRole = 'arn:aws:iam::123456789012:role/crr-role';
            task._putMetadataOnce(entry, true, fakeLogger, err => {
                assert.ifError(err);
                assert(task.backbeatDest.putMetadata.calledOnce);
                assert.deepStrictEqual(task.backbeatDest.putMetadata
                    .firstCall.args[0].AccountId, '123456789012');
                done();
            });
        });
        it('should not pass accountId when not in assumeRole', done => {
            sinon.stub(task, '_publishMetadataWriteMetrics').returns();
            const entry = QueueEntry.createFromKafkaEntry(replicationEntry);
            task.backbeatDest = {
                putMetadata: sinon.stub().returns({
                    send: sinon.stub().yields(),
                    on: sinon.stub(),
                }),
            };
            task.targetRole = 'arn:aws:iam::123456789012:role/crr-role';
            sinon.stub(task.destConfig.auth, 'type').value('role');
            task._putMetadataOnce(entry, true, fakeLogger, err => {
                assert.ifError(err);
                assert(task.backbeatDest.putMetadata.calledOnce);
                assert.strictEqual(task.backbeatDest.putMetadata.firstCall.args[0].AccountId, undefined);
                done();
            });
        });
    });
});
