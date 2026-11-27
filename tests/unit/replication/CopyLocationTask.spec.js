const assert = require('assert');
const sinon = require('sinon');

const CopyLocationTask = require('../../../extensions/replication/tasks/CopyLocationTask');
const ClientManager = require('../../../lib/clients/ClientManager');
const ClientManagerCache = require('../../../lib/clients/ClientManagerCache');
const VaultClient = require('vaultclient').Client;
const { AccountIdCache } = require('../../../extensions/utils/AccountIdCache');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const { errors } = require('@scality/arsenal');
const { ObjectMD } = require('@scality/arsenal').models;
const locationConfig = require('../../../conf/locationConfig.json');

const fakeLogger = require('../../utils/fakeLogger');

function firstManager(cache) {
    return [...cache._managers.values()][0];
}

describe('CopyLocationTask', () => {
    describe('_checkObjectState', () => {
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
                    },
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
            task.destType = 'aws_s3';
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
            task.destType = 'azure';
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
                { start: 101, end: 199 },
            ]);

            const putRangeFunc = sinon.stub(task, '_getRangeAndPutMPUPart');
            putRangeFunc.onCall(0).yields(null, {
                partNumber: 0,
                ETag: 'etag1',
            });
            putRangeFunc.onCall(1).yields(new Error('Upload failed'));

            const abortMpuFunc = sinon.stub(task, '_multipleBackendAbortMPU').yields();
            const completeMpuFunc = sinon.stub(task, '_completeMPU').yields();

            task.destType = 'aws_s3';

            const uploadId = 'test-upload-id';
            task._completeRangedMPU(entry, objectMD, uploadId, fakeLogger, err => {
                assert(err);
                assert.strictEqual(err.message, 'Upload failed');
                assert(abortMpuFunc.calledOnce);
                assert(abortMpuFunc.calledWith(entry, objectMD, uploadId, fakeLogger, sinon.match.func));
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
                { start: 101, end: 199 },
            ]);

            const putRangeFunc = sinon.stub(task, '_getRangeAndPutMPUPart');
            putRangeFunc.onCall(0).yields(null, {
                partNumber: 0,
                ETag: 'etag1',
                numberSubParts: 2,
            });
            putRangeFunc.onCall(1).yields(null, {
                partNumber: 1,
                ETag: 'etag2',
                numberSubParts: 1,
            });

            const completeMpuFunc = sinon.stub(task, '_completeMPU').yields();

            task.destType = 'azure';

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
                        },
                    },
                }),
            });
            assert.strictEqual(task.retryParams.maxRetries, 13);
        });
    });

    describe('_getDestination', () => {
        const task = new CopyLocationTask({
            getStateVars: () => ({
                site: 'aws-site',
                mProducer: { getProducer: () => {} },
                repConfig: { queueProcessor: { retry: {} } },
                destConfig: { replicationEndpoint: { site: 'aws-site', type: 'aws_s3' } },
            }),
        });

        it('should write a transition to this site, with its endpoint type', () => {
            const entry = new ActionQueueEntry({
                action: 'copyLocation',
                toLocation: 'aws-site',
            });
            assert.deepStrictEqual(task._getDestination(entry), {
                location: 'aws-site',
                storageType: 'aws_s3',
            });
        });

        it('should write a pull replication to its target location, with that location type', () => {
            const entry = new ActionQueueEntry({
                action: 'copyLocation',
                site: 'aws-site',
                toLocation: 'us-east-1',
            });
            assert.deepStrictEqual(task._getDestination(entry), {
                location: 'us-east-1',
                storageType: 'file',
            });
        });
    });

    describe('_sendGetObject', () => {
        let task;

        // what the operator writes for a location we replicate to over CRR:
        // the servers to reach it, and an STS to assume roles on it and to
        // find the account of an object owner
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
            // as the source stores it, not as its S3 API takes it
            dataStoreVersionId: '98501018544962999999RG001  ',
            bucket: 'backup-repo-01',
            ownerId: 'owner-canonical-id',
            roleName: 'svcid-dr-source',
        };

        function sourceObjectMD(location = [sourcePart]) {
            const objMd = new ObjectMD();
            objMd.setDataStoreName('source-site');
            objMd.setKey('vm001.vbk');
            objMd.setLocation(location);
            return objMd;
        }

        // the account lookup on the source's Vault
        let getAccountIds;

        beforeEach(() => {
            locationConfig['source-site'] = { type: 'aws_s3', isCRR: true, details: remoteSiteDetails };
            sinon.stub(fakeLogger, 'getSerializedUids').returns('req-uid-1');
            getAccountIds = sinon
                .stub(VaultClient.prototype, 'getAccountIds')
                .yields(null, { message: { body: { 'owner-canonical-id': '123456789012' } } });
            task = new CopyLocationTask({
                getStateVars: () => ({
                    mProducer: { getProducer: () => {} },
                    sourceConfig: { transport: 'http' },
                    sourceAccountIds: new AccountIdCache(10),
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

            return task._sendGetObject(entry, objMd, undefined, fakeLogger, new AbortController()).then(response => {
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

            return task
                ._sendGetObject(entry, sourceObjectMD(), undefined, fakeLogger, new AbortController())
                .then(response => {
                    assert.deepStrictEqual(response, { Body: 'remote-stream' });
                    // the role of the part, in the account of its owner on the source
                    assert.deepStrictEqual(getAccountIds.firstCall.args[0], ['owner-canonical-id']);
                    assert(task._getAssumedRoleS3Client.calledOnce);
                    const [siteConfig, accountId, roleName] = task._getAssumedRoleS3Client.firstCall.args;
                    assert.deepStrictEqual(siteConfig, {
                        transport: 'https',
                        endpoint: 'production.example.com:443',
                        sts: remoteSiteDetails.sts,
                    });
                    assert.strictEqual(accountId, '123456789012');
                    assert.strictEqual(roleName, 'scality-internal/svcid-dr-source');
                    assert(remoteClient.send.calledOnce);
                    const command = remoteClient.send.firstCall.args[0];
                    // bucket, key and version all describe the data on the
                    // source site, not the object we are copying: the
                    // version encoded, as the S3 API takes it
                    assert.strictEqual(command.input.Bucket, 'backup-repo-01');
                    assert.strictEqual(command.input.Key, 'backups/vm001.vbk');
                    assert.strictEqual(command.input.VersionId, 'aKC1sy5pSf1500000000001I4j3QKItW');
                    // the source site is Scality too: same command, and the
                    // request uids let us follow the read across both sites
                    assert.strictEqual(command.input.RequestUids, 'req-uid-1');
                    // the data is native there, it must not be redirected
                    assert.strictEqual(command.input.LocationConstraint, undefined);
                });
        });

        it('should assume the role the part carries, without looking its owner up', () => {
            const remoteClient = { send: sinon.stub().resolves({}) };
            sinon.stub(task, '_getAssumedRoleS3Client').returns(remoteClient);

            const entry = new ActionQueueEntry({ target: {} });
            const objMd = sourceObjectMD([
                { ...sourcePart, role: 'arn:aws:iam::210987654321:role/scality-internal/svcid-dr-source' },
            ]);

            return task._sendGetObject(entry, objMd, undefined, fakeLogger, new AbortController()).then(() => {
                assert(getAccountIds.notCalled);
                const [, accountId, roleName] = task._getAssumedRoleS3Client.firstCall.args;
                assert.strictEqual(accountId, '210987654321');
                assert.strictEqual(roleName, 'scality-internal/svcid-dr-source');
            });
        });

        it('should read the null version when the part carries no version id', () => {
            const remoteClient = { send: sinon.stub().resolves({}) };
            sinon.stub(task, '_getAssumedRoleS3Client').returns(remoteClient);

            const entry = new ActionQueueEntry({ target: {} });
            const objMd = sourceObjectMD([{ ...sourcePart, dataStoreVersionId: undefined }]);

            return task._sendGetObject(entry, objMd, undefined, fakeLogger, new AbortController()).then(() => {
                const command = remoteClient.send.firstCall.args[0];
                assert.strictEqual(command.input.VersionId, 'null');
            });
        });

        it('should pass the range on to the source location', () => {
            const remoteClient = { send: sinon.stub().resolves({}) };
            sinon.stub(task, '_getAssumedRoleS3Client').returns(remoteClient);

            const entry = new ActionQueueEntry({ target: {} });

            return task
                ._sendGetObject(entry, sourceObjectMD(), { start: 0, end: 99 }, fakeLogger, new AbortController())
                .then(() => {
                    const command = remoteClient.send.firstCall.args[0];
                    assert.strictEqual(command.input.Range, 'bytes=0-99');
                });
        });

        it('should look the account of an owner up once per source', () => {
            sinon.stub(task, '_getAssumedRoleS3Client').returns({ send: sinon.stub().resolves({}) });

            const read = () =>
                task._sendGetObject(
                    new ActionQueueEntry({ target: {} }),
                    sourceObjectMD(),
                    undefined,
                    fakeLogger,
                    new AbortController(),
                );

            return read()
                .then(read)
                .then(() => {
                    assert(getAccountIds.calledOnce);
                    assert(task.sourceAccountIds.has('https://sts.production.example.com:443::owner-canonical-id'));
                });
        });

        it('should reject when the source refuses the owner lookup', () => {
            // what Vault answers for an unknown owner
            getAccountIds.yields({ code: 'AccessDenied', description: 'Access Denied', AccessDenied: true });

            const entry = new ActionQueueEntry({ target: {} });

            return task
                ._sendGetObject(entry, sourceObjectMD(), undefined, fakeLogger, new AbortController())
                .then(() => assert.fail('expected rejection'))
                .catch(err => {
                    assert(err.is.AccessDenied);
                    assert.notStrictEqual(err.retryable, true);
                });
        });

        it('should reject with a retryable error when the source cannot be reached for the owner lookup', () => {
            // what vaultclient gives on a network error
            getAccountIds.yields({ code: 500, description: 'We encountered an internal error.', InternalError: true });

            const entry = new ActionQueueEntry({ target: {} });

            return task
                ._sendGetObject(entry, sourceObjectMD(), undefined, fakeLogger, new AbortController())
                .then(() => assert.fail('expected rejection'))
                .catch(err => {
                    assert(err.is.InternalError);
                    assert.strictEqual(err.retryable, true);
                    assert.strictEqual(task.sourceAccountIds.size, 0);
                });
        });

        it('should reject with a retryable error when the source fails the owner lookup', () => {
            getAccountIds.yields({ code: 'ServiceUnavailable', description: 'Unavailable', ServiceUnavailable: true });

            const entry = new ActionQueueEntry({ target: {} });

            return task
                ._sendGetObject(entry, sourceObjectMD(), undefined, fakeLogger, new AbortController())
                .then(() => assert.fail('expected rejection'))
                .catch(err => {
                    assert(err.is.ServiceUnavailable);
                    assert.strictEqual(err.retryable, true);
                });
        });

        it('should reject when the source answers without the owner', () => {
            getAccountIds.yields(null, { message: { body: {} } });

            const entry = new ActionQueueEntry({ target: {} });

            return task
                ._sendGetObject(entry, sourceObjectMD(), undefined, fakeLogger, new AbortController())
                .then(() => assert.fail('expected rejection'))
                .catch(err => {
                    assert(err.is.InternalError);
                    assert.match(err.description, /returned no account for owner owner-canonical-id/);
                    assert.strictEqual(task.sourceAccountIds.size, 0);
                });
        });

        it('should reject when the CRR location has no endpoint to reach it', () => {
            // flagged isCRR, but carrying none of the details telling us
            // where to read the data from
            locationConfig['source-site'] = { type: 'aws_s3', isCRR: true, details: {} };
            task.backbeatClient = { send: sinon.stub() };

            const entry = new ActionQueueEntry({ target: {} });

            return task
                ._sendGetObject(entry, sourceObjectMD(), undefined, fakeLogger, new AbortController())
                .then(() => assert.fail('expected rejection'))
                .catch(err => {
                    assert(err.InternalError);
                    assert.strictEqual(err.retryable, true);
                    assert(task.backbeatClient.send.notCalled);
                });
        });

        it('should reject when the CRR location says nothing of its transport', () => {
            // guessing would dial http on port 80 at a site listening on https
            locationConfig['source-site'] = {
                type: 'aws_s3',
                isCRR: true,
                details: { ...remoteSiteDetails, transport: undefined },
            };
            task.backbeatClient = { send: sinon.stub() };

            const entry = new ActionQueueEntry({ target: {} });

            return task
                ._sendGetObject(entry, sourceObjectMD(), undefined, fakeLogger, new AbortController())
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

            return task
                ._sendGetObject(entry, objMd, undefined, fakeLogger, new AbortController())
                .then(() => assert.fail('expected rejection'))
                .catch(err => {
                    assert(err.InternalError);
                    // the metadata will not change: retrying cannot help
                    assert.notStrictEqual(err.retryable, true);
                    assert(task.backbeatClient.send.notCalled);
                    assert(task._getAssumedRoleS3Client.notCalled);
                });
        });

        it('should reject without calling Cloudserver or the remote site when the part has no role nor owner', () => {
            task.backbeatClient = { send: sinon.stub() };
            sinon.stub(task, '_getAssumedRoleS3Client');

            const entry = new ActionQueueEntry({ target: {} });
            const objMd = sourceObjectMD([{ key: 'k', bucket: 'b', dataStoreName: 'source-site' }]);

            return task
                ._sendGetObject(entry, objMd, undefined, fakeLogger, new AbortController())
                .then(() => assert.fail('expected rejection'))
                .catch(err => {
                    assert(err.AccessDenied);
                    // the metadata will not change: retrying cannot help
                    assert.notStrictEqual(err.retryable, true);
                    assert(task.backbeatClient.send.notCalled);
                    assert(task._getAssumedRoleS3Client.notCalled);
                    assert(getAccountIds.notCalled);
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

            task._getRangeAndPutMPUPartOnce(entry, objMd, { start: 0, end: 99 }, 1, 'upload-id', fakeLogger, err => {
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

            const res = task._publishCopyLocationStatus(noSuchVersion(), entry, null, fakeLogger);

            // committing here would drop the entry before the lifecycle side
            // resets transitionInProgress and counts the attempt
            assert.strictEqual(res.committable, false);
            assert(task.replicationStatusProducer.sendToTopic.calledOnce);
            assert.strictEqual(
                task.replicationStatusProducer.sendToTopic.firstCall.args[0],
                'backbeat-lifecycle-transition-tasks',
            );
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
        const accountId = '123456789012';
        const roleName = 'scality-internal/clean-room-read';

        let getBackbeatClient;

        beforeEach(() => {
            sinon.stub(ClientManager.prototype, 'initSTSConfig');
            sinon.stub(ClientManager.prototype, 'initCredentialsManager');
            getBackbeatClient = sinon.stub(ClientManager.prototype, 'getBackbeatClient').returns({ send: () => {} });
            task = new CopyLocationTask({
                getStateVars: () => ({
                    mProducer: { getProducer: () => {} },
                    sourceConfig: { transport: 'http' },
                    logger: fakeLogger,
                    sourceClientManagers: new ClientManagerCache(),
                }),
            });
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should reuse the client manager for the same endpoint and role', () => {
            const client1 = task._getAssumedRoleS3Client(siteConfig, accountId, roleName, fakeLogger);
            const client2 = task._getAssumedRoleS3Client(siteConfig, accountId, roleName, fakeLogger);

            assert.strictEqual(client1, client2);
            assert.strictEqual(task.sourceClientManagers.size, 1);
            const clientManager = firstManager(task.sourceClientManagers);
            assert.strictEqual(clientManager._transport, 'https');
            assert.deepStrictEqual(clientManager._s3Config, { host: 'production.example.com', port: '443' });
            assert.deepStrictEqual(clientManager._authConfig.sts, siteConfig.sts);
            assert(getBackbeatClient.alwaysCalledWith('123456789012'));
        });

        it('should reuse the client manager across tasks', () => {
            const sourceClientManagers = task.sourceClientManagers;
            const otherEntry = new CopyLocationTask({
                getStateVars: () => ({
                    mProducer: { getProducer: () => {} },
                    sourceConfig: { transport: 'http' },
                    logger: fakeLogger,
                    sourceClientManagers,
                }),
            });

            const client1 = task._getAssumedRoleS3Client(siteConfig, accountId, roleName, fakeLogger);
            const client2 = otherEntry._getAssumedRoleS3Client(siteConfig, accountId, roleName, fakeLogger);

            assert.strictEqual(client1, client2);
            assert.strictEqual(sourceClientManagers.size, 1);
        });

        it('should default the port when the location carries none', () => {
            task._getAssumedRoleS3Client(
                { ...siteConfig, endpoint: 'production.example.com' },
                accountId,
                roleName,
                fakeLogger,
            );

            const clientManager = firstManager(task.sourceClientManagers);
            assert.deepStrictEqual(clientManager._s3Config, { host: 'production.example.com', port: 443 });
        });

        it('should log and throw a retryable AccessDenied when credentials cannot be obtained', () => {
            getBackbeatClient.returns(null);
            const logSpy = sinon.spy(fakeLogger, 'error');

            assert.throws(
                () => task._getAssumedRoleS3Client(siteConfig, accountId, roleName, fakeLogger),
                err => err.AccessDenied && err.retryable === true,
            );
            assert(logSpy.calledOnce);
        });

        it('should assume the role with its path', () => {
            task._getAssumedRoleS3Client(siteConfig, accountId, roleName, fakeLogger);

            const clientManager = firstManager(task.sourceClientManagers);
            assert.strictEqual(clientManager._authConfig.roleName, 'scality-internal/clean-room-read');
        });
    });
});
