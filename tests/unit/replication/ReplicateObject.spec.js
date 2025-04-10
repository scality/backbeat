const assert = require('assert');
const sinon = require('sinon');

const QueueEntry = require('../../../lib/models/QueueEntry');
const ReplicateObject = require('../../../extensions/replication/tasks/ReplicateObject');
const ClientsManager = require('../../../lib/clients/ClientsManager');
const locations = require('../../../conf/locationConfig.json');

const { replicationEntry } = require('../../utils/kafkaEntries');
const fakeLogger = require('../../utils/fakeLogger');
const BackbeatClient = require('../../../lib/clients/BackbeatClient');

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
        it('should create assumeRole client when using assumeRole', () => {
            const setupClient = sinon.stub(task, '_setupAssumeRoleDestClient').returns();
            task._setupDestClients('arn:aws:iam::123456789012:role/crr-role', fakeLogger);
            assert(setupClient.calledOnce);
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

    describe('_setupAssumeRoleDestClient', () => {
        it('should create a new client if no client exists', () => {
            task.destBackbeatHost = {
                host: 'localhost',
                port: 9095,
            };
            task.clientsManager = new ClientsManager('test', fakeLogger);
            task._setupAssumeRoleDestClient('arn:aws:iam::123456789012:role/crr-role');
            assert(task.backbeatDest instanceof BackbeatClient);
        });
        it('should reuse old client if it exists', () => {
            task.destBackbeatHost = {
                host: 'host1',
                port: 9095,
            };
            task.clientsManager = new ClientsManager('test', fakeLogger);
            task._setupAssumeRoleDestClient('arn:aws:iam::123456789012:role/crr-role');
            const oldBackbeatClient = task.backbeatDest;
            task._setupAssumeRoleDestClient('arn:aws:iam::123456789012:role/crr-role');
            assert.deepStrictEqual(oldBackbeatClient, task.backbeatDest);
            assert(task.backbeatDest instanceof BackbeatClient);
        });
        it('should create a new client if s3 config changed', () => {
            task.destBackbeatHost = {
                host: 'host1',
                port: 9095,
            };
            task.clientsManager = new ClientsManager('test', fakeLogger);
            task._setupAssumeRoleDestClient('arn:aws:iam::123456789012:role/crr-role');
            const oldBackbeatClient = task.backbeatDest;
            task.destBackbeatHost.host = 'host2';
            task._setupAssumeRoleDestClient('arn:aws:iam::123456789012:role/crr-role');
            assert.notDeepStrictEqual(oldBackbeatClient, task.backbeatDest);
            assert(task.backbeatDest instanceof BackbeatClient);
            assert.strictEqual(task.backbeatDest.endpoint.host, 'host2:9095');
        });
    });
});
