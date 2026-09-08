const assert = require('assert');
const sinon = require('sinon');

const CopyLocationTask = require('../../../extensions/replication/tasks/CopyLocationTask');
const ClientManager = require('../../../lib/clients/ClientManager');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { errors } = require('arsenal');
const { ObjectMD } = require('arsenal').models;
const locationConfig = require('../../../conf/locationConfig.json');

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

        // what the operator writes for a location we replicate to over CRR:
        // the servers to reach it, and an STS to assume roles on it
        const remoteSiteDetails = {
            transport: 'https',
            servers: ['production.example.com:443'],
            sts: {
                host: 'sts.production.example.com',
                port: '443',
                accessKey: 'AK',
                secretKey: 'SK',
            },
        };

        const sourcePart = {
            key: 'backups/vm001.vbk',
            size: 1048576,
            start: 0,
            dataStoreName: 'source-site',
            dataStoreType: 'aws_s3',
            dataStoreETag: '1:9b2cf535f27731c974343645a3985328',
            dataStoreVersionId: 'aJdO95zrzY5BKLXf9GHFItC0d1CkQ0Ei',
            bucket: 'backup-repo-01',
            role: 'arn:aws:iam::123456789012:role/clean-room-read',
        };

        function sourceObjectMD(location = [sourcePart]) {
            const objMd = new ObjectMD();
            objMd.setDataStoreName('source-site');
            objMd.setKey('vm001.vbk');
            objMd.setLocation(location);
            return objMd;
        }

        beforeEach(() => {
            locationConfig['source-site'] =
                { type: 'aws_s3', isCRR: true, details: remoteSiteDetails };
            sinon.stub(fakeLogger, 'getSerializedUids').returns('req-uid-1');
            task = new CopyLocationTask({
                getStateVars: () => ({
                    mProducer: { getProducer: () => {} },
                    sourceConfig: { transport: 'http' },
                }),
            });
        });

        afterEach(() => {
            delete locationConfig['source-site'];
            sinon.restore();
        });

        it('should read through Cloudserver when the location is not a CRR location', () => {
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
                    assert.strictEqual(command.input.RequestUids, 'req-uid-1');
                });
        });

        it('should read directly from the source location when it is a CRR location', () => {
            const remoteClient = { send: sinon.stub().resolves({ Body: 'remote-stream' }) };
            sinon.stub(task, '_getAssumedRoleS3Client').returns(remoteClient);

            const entry = new ActionQueueEntry({
                target: { bucket: 'local-bucket', key: 'key', version: 'v1' },
            });

            return task._sendGetObject(entry, sourceObjectMD(), undefined, fakeLogger,
                new AbortController())
                .then(response => {
                    assert.deepStrictEqual(response, { Body: 'remote-stream' });
                    assert(task._getAssumedRoleS3Client.calledOnce);
                    const [siteConfig, roleArn] = task._getAssumedRoleS3Client.firstCall.args;
                    assert.deepStrictEqual(siteConfig, {
                        transport: 'https',
                        endpoint: 'production.example.com:443',
                        sts: remoteSiteDetails.sts,
                    });
                    assert.strictEqual(roleArn, 'arn:aws:iam::123456789012:role/clean-room-read');
                    assert(remoteClient.send.calledOnce);
                    const command = remoteClient.send.firstCall.args[0];
                    // bucket, key and version all describe the data on the
                    // source site, not the object we are copying
                    assert.strictEqual(command.input.Bucket, 'backup-repo-01');
                    assert.strictEqual(command.input.Key, 'backups/vm001.vbk');
                    assert.strictEqual(command.input.VersionId, 'aJdO95zrzY5BKLXf9GHFItC0d1CkQ0Ei');
                    // the source site is Scality too: same command, and the
                    // request uids let us follow the read across both sites
                    assert.strictEqual(command.input.RequestUids, 'req-uid-1');
                    // the data is native there, it must not be redirected
                    assert.strictEqual(command.input.LocationConstraint, undefined);
                });
        });

        it('should pass the range on to the source location', () => {
            const remoteClient = { send: sinon.stub().resolves({}) };
            sinon.stub(task, '_getAssumedRoleS3Client').returns(remoteClient);

            const entry = new ActionQueueEntry({ target: {} });

            return task._sendGetObject(entry, sourceObjectMD(), { start: 0, end: 99 },
                fakeLogger, new AbortController())
                .then(() => {
                    const command = remoteClient.send.firstCall.args[0];
                    assert.strictEqual(command.input.Range, 'bytes=0-99');
                });
        });

        it('should reject when the CRR location has no endpoint to reach it', () => {
            // flagged isCRR, but carrying none of the details telling us
            // where to read the data from
            locationConfig['source-site'] = { type: 'aws_s3', isCRR: true, details: {} };
            task.backbeatClient = { send: sinon.stub() };

            const entry = new ActionQueueEntry({ target: {} });

            return task._sendGetObject(entry, sourceObjectMD(), undefined, fakeLogger,
                new AbortController())
                .then(() => assert.fail('expected rejection'))
                .catch(err => {
                    assert(err.InternalError);
                    assert.strictEqual(err.retryable, true);
                    assert(task.backbeatClient.send.notCalled);
                });
        });

        it('should reject when the source location holds more than one part', () => {
            task.backbeatClient = { send: sinon.stub() };
            sinon.stub(task, '_getAssumedRoleS3Client');

            const entry = new ActionQueueEntry({ target: {} });
            const objMd = sourceObjectMD([sourcePart, { ...sourcePart, start: 1048576 }]);

            return task._sendGetObject(entry, objMd, undefined, fakeLogger, new AbortController())
                .then(() => assert.fail('expected rejection'))
                .catch(err => {
                    assert(err.InternalError);
                    // the metadata will not change: retrying cannot help
                    assert.notStrictEqual(err.retryable, true);
                    assert(task.backbeatClient.send.notCalled);
                    assert(task._getAssumedRoleS3Client.notCalled);
                });
        });

        it('should reject without calling Cloudserver or the remote site when the role is missing', () => {
            task.backbeatClient = { send: sinon.stub() };
            sinon.stub(task, '_getAssumedRoleS3Client');

            const entry = new ActionQueueEntry({ target: {} });
            // no role: owner absent from the ownerId->role map
            const objMd = sourceObjectMD([{ key: 'k', bucket: 'b', dataStoreName: 'source-site' }]);

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

    describe('source version disappearing mid-copy', () => {
        let task;

        function noSuchVersion() {
            const err = new Error('The version does not exist.');
            err.name = 'NoSuchVersion';
            err.$metadata = { httpStatusCode: 404 };
            return err;
        }

        beforeEach(() => {
            task = new CopyLocationTask({
                getStateVars: () => ({
                    site: 'test-site',
                    mProducer: { getProducer: () => {} },
                    sourceConfig: { transport: 'http', s3: {} },
                }),
            });
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should fail the copy, not skip it', done => {
            sinon.stub(task, '_sendGetObject').rejects(noSuchVersion());
            const put = sinon.stub(task, '_sendMultipleBackendPutObject');

            const entry = new ActionQueueEntry({ target: {} });
            const objMd = new ObjectMD();
            objMd.setContentLength(200);

            task._getAndPutObjectOnce(entry, objMd, fakeLogger, err => {
                assert(err);
                // an InvalidObjectState would be committed as a skip, leaving
                // the object flagged in transition for good
                assert(!err.InvalidObjectState);
                assert(put.notCalled);
                done();
            });
        });

        it('should fail the copy, not skip it, while streaming an MPU part', done => {
            sinon.stub(task, '_sendGetObject').rejects(noSuchVersion());
            const putPart = sinon.stub(task, '_putMPUPart');

            const entry = new ActionQueueEntry({ target: {} });
            const objMd = new ObjectMD();

            task._getRangeAndPutMPUPartOnce(entry, objMd, { start: 0, end: 99 }, 1,
                'upload-id', fakeLogger, err => {
                    assert(err);
                    assert(!err.InvalidObjectState);
                    assert(putPart.notCalled);
                    done();
                });
        });

        it('should report the failure so the transition can be reset', () => {
            const entry = new ActionQueueEntry({ target: {} });
            entry.setResultsTopic('backbeat-lifecycle-transition-tasks');
            task.replicationStatusProducer = { sendToTopic: sinon.stub() };
            task.dataMoverConsumer = { onEntryCommittable: sinon.stub() };

            const res = task._publishCopyLocationStatus(
                noSuchVersion(), entry, null, fakeLogger);

            // committing here would drop the entry before the lifecycle side
            // resets transitionInProgress and counts the attempt
            assert.strictEqual(res.committable, false);
            assert(task.replicationStatusProducer.sendToTopic.calledOnce);
            assert.strictEqual(task.replicationStatusProducer.sendToTopic.firstCall.args[0],
                'backbeat-lifecycle-transition-tasks');
        });
    });

    describe('_getAssumedRoleS3Client', () => {
        let task;
        const siteConfig = {
            transport: 'https',
            endpoint: 'production.example.com:443',
            sts: {
                host: 'sts.production.example.com',
                port: '443',
                accessKey: 'AK',
                secretKey: 'SK',
            },
        };
        const roleArn = 'arn:aws:iam::123456789012:role/clean-room-read';

        let getBackbeatClient;

        beforeEach(() => {
            sinon.stub(ClientManager.prototype, 'initSTSConfig');
            sinon.stub(ClientManager.prototype, 'initCredentialsManager');
            getBackbeatClient = sinon.stub(ClientManager.prototype, 'getBackbeatClient')
                .returns({ send: () => {} });
            task = new CopyLocationTask({
                getStateVars: () => ({
                    mProducer: { getProducer: () => {} },
                    sourceConfig: { transport: 'http' },
                    logger: fakeLogger,
                    sourceClientManagers: {},
                }),
            });
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should reuse the client manager for the same endpoint and role', () => {
            const client1 = task._getAssumedRoleS3Client(siteConfig, roleArn, fakeLogger);
            const client2 = task._getAssumedRoleS3Client(siteConfig, roleArn, fakeLogger);

            assert.strictEqual(client1, client2);
            assert.strictEqual(Object.keys(task.sourceClientManagers).length, 1);
            const clientManager = Object.values(task.sourceClientManagers)[0];
            assert.strictEqual(clientManager._transport, 'https');
            assert.deepStrictEqual(clientManager._s3Config,
                { host: 'production.example.com', port: '443' });
            assert.deepStrictEqual(clientManager._authConfig.sts, siteConfig.sts);
            assert(getBackbeatClient.alwaysCalledWith('123456789012'));
        });

        it('should default the port when the location carries none', () => {
            task._getAssumedRoleS3Client(
                { ...siteConfig, endpoint: 'production.example.com' }, roleArn, fakeLogger);

            const clientManager = Object.values(task.sourceClientManagers)[0];
            assert.deepStrictEqual(clientManager._s3Config,
                { host: 'production.example.com', port: 443 });
        });

        it('should log and throw a retryable AccessDenied when credentials cannot be obtained', () => {
            getBackbeatClient.returns(null);
            const logSpy = sinon.spy(fakeLogger, 'error');

            assert.throws(
                () => task._getAssumedRoleS3Client(siteConfig, roleArn, fakeLogger),
                err => err.AccessDenied && err.retryable === true);
            assert(logSpy.calledOnce);
        });

        it('should keep the full role name, including any path, when the role ARN has one', () => {
            const pathedRoleArn = 'arn:aws:iam::123456789012:role/service-role/clean-room-read';

            task._getAssumedRoleS3Client(siteConfig, pathedRoleArn, fakeLogger);

            const clientManager = Object.values(task.sourceClientManagers)[0];
            assert.strictEqual(clientManager._authConfig.roleName, 'service-role/clean-room-read');
        });
    });
});
