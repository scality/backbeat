const assert = require('assert');
const sinon = require('sinon');

const CopyLocationTask = require('../../../extensions/replication/tasks/CopyLocationTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { errors } = require('arsenal');
const { ObjectMD } = require('arsenal').models;

const fakeLogger = require('../../utils/fakeLogger');

describe('CopyLocationTask', () => {
    describe('_checkObjectState', () => {
        let task;

        beforeEach(() => {
            task = new CopyLocationTask({
                getStateVars: () => ({
                    site: 'test-site',
                    mProducer: {
                        getProducer: () => {},
                    }
                }),
            });
        });

        it('should return invalidState error object has been changed', () => {
            const objMd = new ObjectMD();
            objMd.setContentMd5('1234-9');

            const entry = new ActionQueueEntry({
                target: {
                    eTag: '"156781-9"',
                },
            });

            const res = task._checkObjectState(entry, objMd);
            assert(res.InvalidObjectState);
        });

        it('should return invalidState error when object already transitioned', () => {
            const objMd = new ObjectMD();
            objMd.setDataStoreName('test-site');

            const entry = new ActionQueueEntry({
                target: {},
                toLocation: 'test-site',
            });

            const res = task._checkObjectState(entry, objMd);
            assert(res.InvalidObjectState);
        });

        it('should not return error if object is valid', () => {
            const objMd = new ObjectMD();
            objMd.setDataStoreName('STANDARD');
            objMd.setContentMd5('1234-9');

            const entry = new ActionQueueEntry({
                target: {
                    eTag: '"1234-9"',
                },
                toLocation: 'test-site',
            });

            const res = task._checkObjectState(entry, objMd);
            assert.equal(res, null);
        });
    });

    describe('_publishCopyLocationStatus', () => {
        let task;

        beforeEach(() => {
            task = new CopyLocationTask({
                getStateVars: () => ({
                    site: 'test-site',
                    mProducer: {
                        getProducer: () => {},
                    }
                }),
            });
        });

        it('should skip object if object state is invalid', () => {
            const entry = new ActionQueueEntry({
                target: {
                    key: 'key',
                    eTag: '"1234-9"',
                },
                toLocation: 'test-site',
            });

            task.replicationStatusProducer = sinon.stub().yields();

            const res = task._publishCopyLocationStatus(errors.InvalidObjectState, entry, null, fakeLogger);
            assert.strictEqual(res.committable, true);
            assert(task.replicationStatusProducer.notCalled);
        });
    });

    describe('_initiateMPU', () => {
        let task;
        beforeEach(() => {
            task = new CopyLocationTask({
                getStateVars: () => ({
                    site: 'test-site',
                    mProducer: {
                        getProducer: () => {},
                    },
                }),
            });
        });
        it('should init mpu when location type is not azure', done => {
            const entry = new ActionQueueEntry({
                target: {
                    key: 'key',
                    eTag: '"1234-9"',
                },
                toLocation: 'test-site',
            });
            task.backbeatClient = {
                send: sinon.stub().resolves({}),
            };
            task.destConfig = {
                replicationEndpoint: {
                    site: 'test-site',
                    type: 'aws_s3',
                },
            };
            task._initiateMPU(entry, new ObjectMD(), fakeLogger, err => {
                assert.ifError(err);
                assert(task.backbeatClient.send.calledOnce);
                done();
            });
        });
        it('should not init mpu when location type is azure', done => {
            const entry = new ActionQueueEntry({
                target: {
                    key: 'key',
                    eTag: '"1234-9"',
                },
                toLocation: 'test-site',
            });
            task.backbeatClient = {
                send: sinon.stub().resolves({}),
            };
            task.destConfig = {
                replicationEndpoint: {
                    site: 'test-site',
                    type: 'azure',
                },
            };
            task._initiateMPU(entry, new ObjectMD(), fakeLogger, err => {
                assert.ifError(err);
                assert(task.backbeatClient.send.notCalled);
                done();
            });
        });
    });

    describe('_completeRangedMPU', () => {
        let task;
        beforeEach(() => {
            task = new CopyLocationTask({
                getStateVars: () => ({
                    site: 'test-site',
                    repConfig: {
                        queueProcessor: {
                            mpuPartsConcurrency: 2,
                        },
                    },
                    mProducer: {
                        getProducer: () => {},
                    },
                }),
            });
        });
        it('should abort MPU on part upload error', done => {
            const entry = new ActionQueueEntry({
                target: {
                    key: 'key',
                    eTag: '"1234-9"',
                },
                toLocation: 'test-site',
            });
            const objectMD = new ObjectMD();
            objectMD.setContentLength(200);
            
            sinon.stub(task, '_getRanges').returns([
                { start: 0, end: 100 },
                { start: 101, end: 199 }
            ]);
            
            const putRangeFunc = sinon.stub(task, '_getRangeAndPutMPUPart');
            putRangeFunc.onCall(0).yields(null, {
                partNumber: 0,
                ETag: 'etag1',
            });
            putRangeFunc.onCall(1).yields(new Error('Upload failed'));
            
            const abortMpuFunc = sinon.stub(task, '_multipleBackendAbortMPU').yields();
            const completeMpuFunc = sinon.stub(task, '_completeMPU').yields();
            
            task.destConfig = {
                replicationEndpoint: {
                    site: 'test-site',
                    type: 'aws_s3',
                },
            };
            
            const uploadId = 'test-upload-id';
            task._completeRangedMPU(entry, objectMD, uploadId, fakeLogger, err => {
                assert(err);
                assert.strictEqual(err.message, 'Upload failed');
                assert(abortMpuFunc.calledOnce);
                assert(abortMpuFunc.calledWith(
                    entry, objectMD, uploadId, fakeLogger, sinon.match.func
                ));
                assert(completeMpuFunc.notCalled);
                done();
            });
        });
            
        it('should handle Azure special case for MPU parts', done => {
            const entry = new ActionQueueEntry({
                target: {
                    key: 'key',
                    eTag: '"1234-9"',
                },
                toLocation: 'test-site',
            });
            const objectMD = new ObjectMD();
            objectMD.setContentLength(200);
            
            sinon.stub(task, '_getRanges').returns([
                { start: 0, end: 100 },
                { start: 101, end: 199 }
            ]);
            
            const putRangeFunc = sinon.stub(task, '_getRangeAndPutMPUPart');
            putRangeFunc.onCall(0).yields(null, {
                partNumber: 0,
                ETag: 'etag1',
                numberSubParts: 2
            });
            putRangeFunc.onCall(1).yields(null, {
                partNumber: 1,
                ETag: 'etag2',
                numberSubParts: 1
            });
            
            const completeMpuFunc = sinon.stub(task, '_completeMPU').yields();
            
            task.destConfig = {
                replicationEndpoint: {
                    site: 'test-site',
                    type: 'azure',
                },
            };
            
            task._completeRangedMPU(entry, objectMD, 'test-upload-id', fakeLogger, err => {
                assert.ifError(err);
                assert(completeMpuFunc.calledOnce);
                const completionData = completeMpuFunc.firstCall.args[3];
                assert(completionData[0].NumberSubParts);
                assert(completionData[1].NumberSubParts);
                done();
            });
        });
    });

    describe('constructor', () => {
        it('should use retry config of the relevent type', () => {
            const task = new CopyLocationTask({
                getStateVars: () => ({
                    mProducer: {
                        getProducer: () => {},
                    },
                    repConfig: {
                        queueProcessor: {
                            retry: {
                                scality: {
                                    maxRetries: 13,
                                },
                                azure: {
                                    maxRetries: 5,
                                },
                            },
                        },
                    },
                    destConfig: {
                        replicationEndpoint: {
                            site: 'test-site',
                            type: 'scality',
                        }
                    },
                }),
            });
            assert.strictEqual(task.retryParams.maxRetries, 13);
        });
    });

    describe('_sendGetObject', () => {
        let task;
        let config;

        beforeEach(() => {
            config = require('../../../lib/Config');
            task = new CopyLocationTask({
                getStateVars: () => ({
                    mProducer: { getProducer: () => {} },
                    sourceConfig: { transport: 'http' },
                }),
            });
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should read through Cloudserver when the location is not isCRR', () => {
            sinon.stub(config, 'getLocationConstraint').returns({ locationType: 'location-aws-s3-v1', isCRR: false });
            task.backbeatClient = { send: sinon.stub().resolves({ Body: 'stream' }) };

            const entry = new ActionQueueEntry({
                target: { bucket: 'bucket', key: 'key', version: 'v1' },
            });
            const objMd = new ObjectMD();
            objMd.setDataStoreName('some-location');

            return task._sendGetObject(entry, objMd, undefined, fakeLogger, new AbortController())
                .then(response => {
                    assert.deepStrictEqual(response, { Body: 'stream' });
                    assert(task.backbeatClient.send.calledOnce);
                    const command = task.backbeatClient.send.firstCall.args[0];
                    assert.strictEqual(command.input.Bucket, 'bucket');
                    assert.strictEqual(command.input.Key, 'key');
                    assert.strictEqual(command.input.VersionId, 'v1');
                    assert.strictEqual(command.input.LocationConstraint, 'some-location');
                });
        });

        it('should read directly from the CRR source location when isCRR', () => {
            sinon.stub(config, 'getLocationConstraint').returns({
                locationType: 'location-scality-crr-v1',
                isCRR: true,
                details: {
                    servers: ['production.example.com:443'],
                    transport: 'https',
                    sts: {
                        host: 'sts.production.example.com',
                        port: '443',
                        accessKey: 'AK',
                        secretKey: 'SK',
                    },
                },
            });

            const fakeS3Client = { send: sinon.stub().resolves({ Body: 'remote-stream' }) };
            sinon.stub(task, '_getAssumedRoleS3Client').returns(fakeS3Client);

            const entry = new ActionQueueEntry({
                target: { bucket: 'local-bucket', key: 'key', version: 'v1' },
            });
            const objMd = new ObjectMD();
            objMd.setDataStoreName('source-site');
            objMd.setKey('backups/vm001.vbk');
            objMd.setLocation([{
                key: 'backups/vm001.vbk',
                size: 1048576,
                start: 0,
                dataStoreName: 'source-site',
                dataStoreType: 'aws_s3',
                dataStoreETag: '1:9b2cf535f27731c974343645a3985328',
                dataStoreVersionId: 'aJdO95zrzY5BKLXf9GHFItC0d1CkQ0Ei',
                bucket: 'backup-repo-01',
                role: 'arn:aws:iam::123456789012:role/clean-room-read',
            }]);

            return task._sendGetObject(entry, objMd, undefined, fakeLogger, new AbortController())
                .then(response => {
                    assert.deepStrictEqual(response, { Body: 'remote-stream' });
                    assert(task._getAssumedRoleS3Client.calledOnce);
                    const [locationConfig, roleArn] = task._getAssumedRoleS3Client.firstCall.args;
                    assert.strictEqual(locationConfig.isCRR, true);
                    assert.strictEqual(roleArn, 'arn:aws:iam::123456789012:role/clean-room-read');
                    assert(fakeS3Client.send.calledOnce);
                    const command = fakeS3Client.send.firstCall.args[0];
                    assert.strictEqual(command.input.Bucket, 'backup-repo-01');
                    assert.strictEqual(command.input.Key, 'backups/vm001.vbk');
                    assert.strictEqual(command.input.VersionId, 'aJdO95zrzY5BKLXf9GHFItC0d1CkQ0Ei');
                });
        });

        it('should reject without calling Cloudserver or the remote site when the role is missing', () => {
            sinon.stub(config, 'getLocationConstraint').returns({
                locationType: 'location-scality-crr-v1',
                isCRR: true,
                details: {},
            });
            task.backbeatClient = { send: sinon.stub() };
            sinon.stub(task, '_getAssumedRoleS3Client');

            const entry = new ActionQueueEntry({ target: {} });
            const objMd = new ObjectMD();
            objMd.setDataStoreName('source-site');
            objMd.setLocation([{
                key: 'k',
                bucket: 'b',
                dataStoreName: 'source-site',
                // no role: owner absent from the ownerId->role map
            }]);

            return task._sendGetObject(entry, objMd, undefined, fakeLogger, new AbortController())
                .then(() => assert.fail('expected rejection'))
                .catch(err => {
                    assert(err.AccessDenied);
                    assert.strictEqual(err.retryable, true);
                    assert(task.backbeatClient.send.notCalled);
                    assert(task._getAssumedRoleS3Client.notCalled);
                });
        });
    });

    describe('_getAssumedRoleS3Client', () => {
        let task;
        const locationConfig = {
            details: {
                servers: ['production.example.com:443'],
                transport: 'https',
                sts: {
                    host: 'sts.production.example.com',
                    port: '443',
                    accessKey: 'AK',
                    secretKey: 'SK',
                },
            },
        };
        const roleArn = 'arn:aws:iam::123456789012:role/clean-room-read';

        beforeEach(() => {
            task = new CopyLocationTask({
                getStateVars: () => ({
                    mProducer: { getProducer: () => {} },
                    sourceConfig: { transport: 'http' },
                    assumedRoleCredentialsManager: {
                        getCredentials: sinon.stub().returns({
                            getCredentialsProvider: () => async () => ({}),
                        }),
                    },
                    assumedRoleS3Clients: {},
                }),
            });
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should cache and reuse the S3 client for the same endpoint and role', () => {
            const client1 = task._getAssumedRoleS3Client(locationConfig, roleArn, fakeLogger);
            const client2 = task._getAssumedRoleS3Client(locationConfig, roleArn, fakeLogger);
            assert.strictEqual(client1, client2);
            assert(task.assumedRoleCredentialsManager.getCredentials.calledOnce);
        });

        it('should log and throw a retryable AccessDenied when credentials cannot be obtained', () => {
            task.assumedRoleCredentialsManager.getCredentials.returns(null);
            const logSpy = sinon.spy(fakeLogger, 'error');

            assert.throws(
                () => task._getAssumedRoleS3Client(locationConfig, roleArn, fakeLogger),
                err => err.AccessDenied && err.retryable === true);
            assert(logSpy.calledOnce);
        });

        it('should keep the full role name, including any path, when the role ARN has one', () => {
            const pathedRoleArn = 'arn:aws:iam::123456789012:role/service-role/clean-room-read';

            task._getAssumedRoleS3Client(locationConfig, pathedRoleArn, fakeLogger);

            const params = task.assumedRoleCredentialsManager.getCredentials.firstCall.args[0];
            assert.strictEqual(params.authConfig.roleName, 'service-role/clean-room-read');
        });
    });
});
