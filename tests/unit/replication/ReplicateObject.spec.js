const assert = require('assert');
const sinon = require('sinon');

const QueueEntry = require('../../../lib/models/QueueEntry');
const ReplicateObject = require('../../../extensions/replication/tasks/ReplicateObject');
const ClientManager = require('../../../lib/clients/ClientManager');
const locations = require('../../../conf/locationConfig.json');

const { replicationEntry } = require('../../utils/kafkaEntries');
const fakeLogger = require('../../utils/fakeLogger');

describe('ReplicateObject', () => {
    let task;

    beforeEach(() => {
        locations.site = {
            details: {
                awsEndpoint: 'https://s3.amazonaws.com',
                bucketMatch: true,
                bucketName: 'ring-bucket-1',
                credentials: {
                    accessKey: 'accessKey1',
                    secretKey: 'verySecretKey1',
                },
            },
            isTransient: false,
            legacyAwsBehavior: false,
            objectId: '06a862b3-fee4-11eb-a6ba-26bd22419be2',
            type: 'aws_s3'
        };
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
                    }],
                    transport: 'http',
                },
                destHosts: {
                    pickNextHost: () => 'localhost:9095',
                    pickHost: () => ({
                        host: 'localhost',
                        port: 9095,
                    }),
                },
                logger: fakeLogger,
            }),
        });
    });

    afterEach(() => {
        sinon.restore();
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

    describe('_setupDestClients', () => {
        it('should setup destination client with proper creds when using assumeRole', () => {
            sinon.stub(ClientManager.prototype, 'initCredentialsManager').returns(null);
            sinon.stub(ClientManager.prototype, 'getBackbeatClient').returns(null);
            task._setupDestClients('arn:aws:iam::123456789012:role/crr-role', fakeLogger);
            assert.deepStrictEqual(task.clientManager._id, '123456789012');
            assert.deepStrictEqual(task.clientManager._authConfig, {
                type: 'assumeRole',
                roleName: 'crr-role',
                sts: {
                    host: 'sts.enpoint.com',
                    port: 80,
                    accessKey: 'accessKey1',
                    secretKey: 'verySecretKey1',
                },
            });
            assert.deepStrictEqual(task.clientManager._s3Config, {
                host: 'localhost',
                port: 9095,
            });
            assert.deepStrictEqual(task.clientManager._transport, 'http');
            assert.deepStrictEqual(task.clientManager._stsConfig.endpoint, 'http://sts.enpoint.com:80');
            assert.deepStrictEqual(task.clientManager._stsConfig.credentials, {
                accessKeyId: 'accessKey1',
                secretAccessKey: 'verySecretKey1',
            });
        });

        it('should setup destination BackbeatClient with proper creds when not in assumeRole', () => {
            task.destConfig.auth = {
                type: 'service',
                account: 'replication-service',
            };
            sinon.stub(task, '_createCredentials').returns({
                accessKeyId: 'accessKeyNoAssumeRole',
                secretAccessKey: 'secretKeyNoAssumeRole',
            });
            task._setupDestClients('arn:aws:iam::123456789012:role/crr-role', fakeLogger);
            assert.strictEqual(task.backbeatDest.config.endpoint, 'http://localhost:9095');
            assert.deepStrictEqual(task.backbeatDest.config.credentials, {
                accessKeyId: 'accessKeyNoAssumeRole',
                secretAccessKey: 'secretKeyNoAssumeRole',
            });
        });
    });

    describe('constructor', () => {
        it('should use retry config of the relevent type', () => {
            const task = new ReplicateObject({
                getStateVars: () => ({
                    repConfig: {
                        queueProcessor: {
                            retry: {
                                scality: {
                                    maxRetries: 5,
                                },
                                azure: {
                                    maxRetries: 4,
                                },
                            },
                        },
                    },
                    destConfig: {
                        replicationEndpoint: {
                            site: 'test-site',
                            type: 'scality',
                        },
                    },
                }),
            });
            assert.strictEqual(task.retryParams.maxRetries, 5);
        });
    });
});
