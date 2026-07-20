const assert = require('assert');
const sinon = require('sinon');
const { Readable } = require('stream');

const QueueEntry = require('../../../lib/models/QueueEntry');
const ReplicateObject = require('../../../extensions/replication/tasks/ReplicateObject');
const ClientManager = require('../../../lib/clients/ClientManager');
const locations = require('../../../conf/locationConfig.json');
const { versioning } = require('arsenal');
const { generateVersionId, encode } = versioning.VersionID;
const {
    VersionIdCollisionException,
    StaleMicroVersionIdException,
    MicroVersionIdAlreadyStoredException,
} = require('@scality/cloudserverclient');

const { HttpRequest } = require('@smithy/protocol-http');
const { replicationExpectContinueThreshold } = require('../../../lib/constants');
const { replicationEntry } = require('../../utils/kafkaEntries');
const fakeLogger = require('../../utils/fakeLogger');

describe('ReplicateObject', () => {
    let task;

    beforeEach(() => {
        locations.site = {
            details: {
                servers: ['s3.zenko.local:80'],
                bucketName: 'crr-bucket-1',
                sts: {
                    host: 'sts.zenko.local',
                    port: 80,
                    accessKey: 'accessKey1',
                    secretKey: 'verySecretKey1',
                },
            },
            objectId: '06a862b3-fee4-11eb-a6ba-26bd22419be2',
            type: 'crr'
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
                            host: 'sts.zenko.local',
                            port: 80,
                            accessKey: 'accessKey1',
                            secretKey: 'verySecretKey1',
                        },
                    },
                    bootstrapList: [{
                        site: 'site',
                        servers: ['s3.zenko.local:80'],
                    }],
                    transport: 'http',
                },
                destHosts: {
                    pickNextHost: () => {},
                    pickHost: () => ({
                        host: 's3.zenko.local',
                        port: 80,
                    }),
                },
                logger: fakeLogger,
            }),
        });
    });

    afterEach(() => {
        sinon.restore();
    });

    function makeMicroVersionIds() {
        const a = generateVersionId('test', 'RG001');
        const b = generateVersionId('test', 'RG001');
        const [older, newer] = a > b ? [a, b] : [b, a]; // In case they are generated at the same millisecond
        return { older, newer, olderEncoded: encode(older), newerEncoded: encode(newer) };
    }

    function makeBodyStream() {
        const stream = new Readable({ read() {} });
        process.nextTick(() => stream.push(null));
        return stream;
    }

    function makeSourceEntry(microVersionId) {
        return {
            getBucket: () => 'src-bucket',
            getObjectKey: () => 'key',
            getEncodedVersionId: () => encode(generateVersionId('test', 'RG001')),
            getMicroVersionId: () => microVersionId || null,
            getOwnerId: () => 'canonical-id',
            getLogInfo: () => ({}),
            getLocation: () => [{
                key: 'data-key', size: 10, start: 0,
                dataStoreName: 'file', dataStoreETag: '1:abc',
            }],
            getContentLength: () => 10,
        };
    }

    function makeDestEntry() {
        return {
            getBucket: () => 'dest-bucket',
            getObjectKey: () => 'key',
            getOwnerId: () => 'canonical-id',
            getLogInfo: () => ({}),
            setAmzServerSideEncryption: () => {},
            setAmzEncryptionCustomerAlgorithm: () => {},
            setAmzEncryptionKeyId: () => {},
            setLocation: () => {},
        };
    }

    describe('putData : cascade VersionIdCollisionException handling', () => {
        let part;

        beforeEach(() => {
            part = { key: 'data-key', size: 10, start: 0,
                dataStoreName: 'file', dataStoreETag: '1:abc' };
            sinon.stub(task, '_publishReadMetrics').returns();
            sinon.stub(task, '_publishDataWriteMetrics').returns();
        });

        function mockDataTransfer(destinationMicroVersionId) {
            task.S3source = {
                send: sinon.stub().resolves({
                    Body: makeBodyStream(),
                    ContentLength: 10,
                }),
            };
            const err = new VersionIdCollisionException({
                message: 'version already at destination',
                microVersionId: destinationMicroVersionId,
            });
            task.backbeatDest = {
                send: sinon.stub().rejects(err),
            };
        }

        it('should return collision info on VersionIdCollisionException', done => {
            const { older, olderEncoded } = makeMicroVersionIds();
            mockDataTransfer(olderEncoded);
            task._getAndPutPartOnce(makeSourceEntry(older), makeDestEntry(), part, fakeLogger, (err, result) => {
                assert.ifError(err);
                assert.ok(result && result.isCollision, 'should return collision info object');
                assert.ok('microVersionId' in result,
                    'collision info should include microVersionId');
                sinon.assert.notCalled(task._publishDataWriteMetrics);
                done();
            });
        });

        it('should set data location and publish metrics when no collision', done => {
            task.S3source = {
                send: sinon.stub().resolves({ Body: makeBodyStream(), ContentLength: 10 }),
            };
            task.backbeatDest = {
                send: sinon.stub().resolves({
                    Location: [{ key: 'new-key', dataStoreName: 'file' }],
                }),
            };
            task._getAndPutPartOnce(makeSourceEntry(), makeDestEntry(), part, fakeLogger, (err, result) => {
                assert.ifError(err);
                assert.deepStrictEqual(result, {
                    key: 'new-key',
                    start: 0,
                    size: 10,
                    dataStoreName: 'file',
                    dataStoreETag: '1:abc',
                    blockId: undefined,
                });
                sinon.assert.calledOnce(task._publishDataWriteMetrics);
                done();
            });
        });
    });

    describe('Expect: 100-continue threshold', () => {
        const part = { key: 'data-key', size: 10, start: 0, dataStoreName: 'file', dataStoreETag: '1:abc' };

        beforeEach(() => {
            sinon.stub(task, '_publishReadMetrics').returns();
            sinon.stub(task, '_publishDataWriteMetrics').returns();
        });

        function makeDestWithExpectCapture() {
            let expectHeader;
            return {
                dest: {
                    send: async command => {
                        const req = new HttpRequest({ hostname: 'x', headers: {}, body: 'x' });
                        await command.middlewareStack.resolve(async () => ({}), {})({ request: req });
                        expectHeader = req.headers.Expect;
                        return { Location: [{ key: 'x', dataStoreName: 'file' }] };
                    },
                },
                getExpect: () => expectHeader,
            };
        }

        it('should not set Expect header for objects below the threshold', done => {
            task.S3source = { send: sinon.stub().resolves({
                Body: makeBodyStream(), ContentLength: replicationExpectContinueThreshold - 1,
            }) };
            const { dest, getExpect } = makeDestWithExpectCapture();
            task.backbeatDest = dest;
            task._getAndPutPartOnce(makeSourceEntry(), makeDestEntry(), part, fakeLogger, err => {
                assert.ifError(err);
                assert.strictEqual(getExpect(), undefined);
                done();
            });
        });

        it('should set Expect header for objects at or above the threshold', done => {
            task.S3source = { send: sinon.stub().resolves({
                Body: makeBodyStream(), ContentLength: replicationExpectContinueThreshold,
            }) };
            const { dest, getExpect } = makeDestWithExpectCapture();
            task.backbeatDest = dest;
            task._getAndPutPartOnce(makeSourceEntry(), makeDestEntry(), part, fakeLogger, err => {
                assert.ifError(err);
                assert.strictEqual(getExpect(), '100-continue');
                done();
            });
        });
    });

    describe('putMetadata : cascade handling', () => {
        let entry;

        beforeEach(() => {
            entry = QueueEntry.createFromKafkaEntry(replicationEntry);
            task.targetRole = 'arn:aws:iam::123456789012:role/crr-role';
        });

        it('should pass through MicroVersionIdAlreadyStoredException and skip metrics', done => {
            const metricsStub = sinon.stub(task, '_publishMetadataWriteMetrics').returns();
            const loopErr = new MicroVersionIdAlreadyStoredException({
                message: 'incoming microVersionId already at destination',
            });
            task.backbeatDest = { send: sinon.stub().rejects(loopErr) };
            task._putMetadataOnce(entry, false, null, fakeLogger, err => {
                assert.ok(err instanceof MicroVersionIdAlreadyStoredException);
                sinon.assert.notCalled(metricsStub);
                done();
            });
        });

        it('should pass through StaleMicroVersionIdException', done => {
            sinon.stub(task, '_publishMetadataWriteMetrics').returns();
            const staleErr = new StaleMicroVersionIdException({
                message: 'incoming revision is older than destination',
            });
            task.backbeatDest = { send: sinon.stub().rejects(staleErr) };
            task._putMetadataOnce(entry, false, null, fakeLogger, err => {
                assert.ok(err instanceof StaleMicroVersionIdException);
                done();
            });
        });

        it('should publish metrics and succeed on normal response', done => {
            const metricsStub = sinon.stub(task, '_publishMetadataWriteMetrics').returns();
            task.backbeatDest = { send: sinon.stub().resolves({}) };
            task._putMetadataOnce(entry, false, null, fakeLogger, err => {
                assert.ifError(err);
                sinon.assert.calledOnce(metricsStub);
                done();
            });
        });
    });

    describe('_handleReplicationOutcome : cascade outcomes', () => {
        let sourceEntry, destEntry, kafkaEntry;

        beforeEach(() => {
            sourceEntry = QueueEntry.createFromKafkaEntry(replicationEntry);
            destEntry = makeDestEntry();
            kafkaEntry = {};
            sinon.stub(task, '_publishReplicationStatus').returns();

            sinon.stub(sourceEntry, 'toCompletedEntry').returns(sourceEntry);
            sinon.stub(sourceEntry, 'toFailedEntry').returns(sourceEntry);
            sinon.stub(sourceEntry, 'setReplicationSiteDataStoreVersionId').returns(sourceEntry);
            sinon.stub(sourceEntry, 'getReplicationSiteDataStoreVersionId').returns('v1');
        });

        it('should mark COMPLETED for MicroVersionIdAlreadyStoredException', done => {
            task._handleReplicationOutcome(
                new MicroVersionIdAlreadyStoredException({}),
                sourceEntry, destEntry, kafkaEntry, fakeLogger, () => {
                    sinon.assert.calledWith(task._publishReplicationStatus,
                        sourceEntry, 'COMPLETED', sinon.match.any);
                    done();
                });
        });

        it('should mark COMPLETED for StaleMicroVersionIdException', done => {
            task._handleReplicationOutcome(
                new StaleMicroVersionIdException({}),
                sourceEntry, destEntry, kafkaEntry, fakeLogger, () => {
                    sinon.assert.calledWith(task._publishReplicationStatus,
                        sourceEntry, 'COMPLETED', sinon.match.any);
                    done();
                });
        });

        it('should mark COMPLETED on successful replication', done => {
            task._handleReplicationOutcome(
                null, sourceEntry, destEntry, kafkaEntry, fakeLogger, () => {
                    sinon.assert.calledWith(task._publishReplicationStatus,
                        sourceEntry, 'COMPLETED', sinon.match.any);
                    done();
                });
        });

        it('should mark FAILED for real errors', done => {
            const realErr = Object.assign(new Error('network failure'), { origin: 'target' });
            task._handleReplicationOutcome(
                realErr, sourceEntry, destEntry, kafkaEntry, fakeLogger, () => {
                    sinon.assert.calledWith(task._publishReplicationStatus,
                        sourceEntry, 'FAILED', sinon.match.any);
                    done();
                });
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
            const sendStub = sinon.stub().resolves({});
            task.backbeatDest = {
                send: sendStub,
            };
            task.targetRole = 'arn:aws:iam::123456789012:role/crr-role';
            task._putMetadataOnce(entry, true, null, fakeLogger, err => {
                assert.ifError(err);
                assert(sendStub.calledOnce);
                assert.deepStrictEqual(sendStub.firstCall.args[0].input.AccountId, '123456789012');
                done();
            });
        });
        it('should not pass accountId when not in assumeRole', done => {
            sinon.stub(task, '_publishMetadataWriteMetrics').returns();
            const entry = QueueEntry.createFromKafkaEntry(replicationEntry);
            const sendStub = sinon.stub().resolves({});
            task.backbeatDest = {
                send: sendStub,
            };
            task.targetRole = 'arn:aws:iam::123456789012:role/crr-role';
            sinon.stub(task.destConfig.auth, 'type').value('role');
            task._putMetadataOnce(entry, true, null, fakeLogger, err => {
                assert.ifError(err);
                assert(sendStub.calledOnce);
                assert.strictEqual(sendStub.firstCall.args[0].input.AccountId, undefined);
                done();
            });
        });
    });

    describe('_getUpdatedSourceEntry', () => {
        const ObjectQueueEntry =
            require('../../../lib/models/ObjectQueueEntry');

        function makeEntry() {
            return new ObjectQueueEntry('source-bucket', 'key', {
                'md-model-version': 2,
                'replicationInfo': {
                    status: 'PENDING',
                    content: ['DATA', 'METADATA'],
                    destination: '',
                    role: 'arn:aws:iam::111:role/src',
                    backends: [{
                        site: 'site',
                        status: 'PENDING',
                        dataStoreVersionId: 'v-1',
                        destination: 'arn:aws:s3:::bucket-a',
                        role: 'arn:aws:iam::222:role/dst',
                    }],
                },
            })
                .setReplicationBackend({
                    site: 'site',
                    destination: 'arn:aws:s3:::bucket-a',
                    role: 'arn:aws:iam::222:role/dst',
                });
        }

        it('returns a COMPLETED entry preserving the dataStoreVersionId', () => {
            task.site = 'site';
            const updated = task._getUpdatedSourceEntry({
                sourceEntry: makeEntry(),
                replicationStatus: 'COMPLETED',
            });
            assert.strictEqual(
                updated.getReplicationSiteStatus({
                    site: 'site',
                    destination: 'arn:aws:s3:::bucket-a',
                }),
                'COMPLETED');
            assert.strictEqual(
                updated.getReplicationSiteDataStoreVersionId({
                    site: 'site',
                    destination: 'arn:aws:s3:::bucket-a',
                }),
                'v-1');
        });

        it('returns a FAILED entry preserving the dataStoreVersionId', () => {
            task.site = 'site';
            const updated = task._getUpdatedSourceEntry({
                sourceEntry: makeEntry(),
                replicationStatus: 'FAILED',
            });
            assert.strictEqual(
                updated.getReplicationSiteStatus({
                    site: 'site',
                    destination: 'arn:aws:s3:::bucket-a',
                }),
                'FAILED');
            assert.strictEqual(
                updated.getReplicationSiteDataStoreVersionId({
                    site: 'site',
                    destination: 'arn:aws:s3:::bucket-a',
                }),
                'v-1');
        });
    });

    describe('_processQueueEntry', () => {
        it('should call _putMetadata with mdOnly=false for zero-byte objects', done => {
            const destEntry = {
                getBucket: () => 'dest-bucket',
                getObjectKey: () => 'key',
                getLogInfo: () => ({}),
                setLocation: () => {},
                getReplicationBackend: () => 'site',
                getReplicationSiteStatus: () => 'PENDING',
            };
            const sourceEntry = {
                getBucket: () => 'src-bucket',
                getObjectKey: () => 'key',
                getEncodedVersionId: () => encode(generateVersionId('test', 'RG001')),
                getMicroVersionId: () => null,
                getReplicationContent: () => ['DATA', 'METADATA'],
                getContentLength: () => 0,
                getLocation: () => [],
                getReducedLocations: () => [],
                getLastModified: () => new Date().toISOString(),
                getIsDeleteMarker: () => false,
                getReplicationBackend: () => 'site',
                getLogInfo: () => ({}),
                toReplicaEntry: () => destEntry,
            };

            task.metricsHandler = { rpo: () => {} };
            task.mProducer = { publishMetrics: () => {} };

            sinon.stub(task, '_setupRoles').callsFake((e, l, cb) => cb(null, 'srcRole', 'dstRole'));
            sinon.stub(task, '_setTargetAccountMd').callsFake((e, r, l, cb) => cb(null));
            sinon.stub(task, '_publishReplicationStatus');

            const putMetadataStub = sinon.stub(task, '_putMetadata')
                .callsFake((e, mdOnly, conflict, l, cb) => {
                    assert.strictEqual(mdOnly, false,
                        'zero-byte objects must use DATA,METADATA (create) mode, not METADATA-only (update) mode');
                    cb(null);
                });

            task.processQueueEntry(sourceEntry, {}, () => {
                sinon.assert.calledOnce(putMetadataStub);
                done();
            });
        });
    });

    it('_processQueueEntryRetryFull should delete orphans on putMetadata errors', done => {
        const writtenLocations = [{ key: 'data-key', dataStoreName: 'file' }];
        const sourceEntry = QueueEntry.createFromKafkaEntry(replicationEntry);
        const destEntry = makeDestEntry();
        sinon.stub(task, '_publishReplicationStatus').returns();
        sinon.stub(task, '_deleteOrphans').callsFake((entry, locations, log, cb) => cb());
        sinon.stub(task, '_getAndPutData').callsFake((src, dest, log, cb) =>
            cb(null, writtenLocations, undefined));
        sinon.stub(task, '_putMetadata').callsFake((entry, mdOnly, conflict, log, cb) =>
            cb(new MicroVersionIdAlreadyStoredException({ message: 'collision' })));

        task._processQueueEntryRetryFull(sourceEntry, destEntry, {}, fakeLogger, err => {
            assert.ifError(err);
            sinon.assert.calledOnce(task._deleteOrphans);
            sinon.assert.calledWith(task._deleteOrphans,
                destEntry, writtenLocations, sinon.match.any, sinon.match.any);
            done();
        });
    });

    describe('_publishReplicationStatus', () => {
        const ObjectQueueEntry =
            require('../../../lib/models/ObjectQueueEntry');

        function makeEntry() {
            return new ObjectQueueEntry('source-bucket', 'key', {
                'md-model-version': 2,
                'replicationInfo': {
                    status: 'PENDING',
                    content: ['DATA', 'METADATA'],
                    destination: '',
                    role: 'arn:aws:iam::111:role/src',
                    backends: [{
                        site: 'site',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                        destination: 'arn:aws:s3:::bucket-a',
                        role: 'arn:aws:iam::222:role/dst',
                    }],
                },
            })
                .setReplicationBackend({
                    site: 'site',
                    destination: 'arn:aws:s3:::bucket-a',
                    role: 'arn:aws:iam::222:role/dst',
                });
        }

        it('serialises the ReplicationBackend into the kafka status payload', () => {
            task.site = 'site';
            task.repConfig = { replicationStatusTopic: 'repstatus' };
            task.metricsHandler = {
                metadataReplicationStatus: () => {},
                dataReplicationStatus: () => {},
            };
            const send = sinon.stub().yields(null);
            task.replicationStatusProducer = { send };

            task._publishReplicationStatus(
                makeEntry(), 'COMPLETED', { log: fakeLogger, kafkaEntry: {} });

            sinon.assert.calledOnce(send);
            const kafkaEntries = send.firstCall.args[0];
            assert.strictEqual(kafkaEntries.length, 1);
            const payload = JSON.parse(kafkaEntries[0].message);
            assert.strictEqual(payload.site, 'site');
            assert.strictEqual(payload.destination, 'arn:aws:s3:::bucket-a');
            assert.strictEqual(payload.role, 'arn:aws:iam::222:role/dst');
        });
    });

    describe('_setupRolesOnce', () => {
        const ObjectQueueEntry =
            require('../../../lib/models/ObjectQueueEntry');

        function _makeEntry(backends) {
            const entry = new ObjectQueueEntry('source-bucket', 'key', {
                'md-model-version': 2,
                'replicationInfo': {
                    status: 'PENDING',
                    content: ['DATA', 'METADATA'],
                    destination: '',
                    role: 'arn:aws:iam::111:role/src',
                    backends,
                },
            });

            // Stamp the per-task identity the way QueueProcessor does
            // before dispatching, so getReplicationBackend() returns the right
            // disambiguator and per-backend lookups resolve.
            return entry.setReplicationBackend(backends[0]);
        }

        it('validates per-backend role via account substitution', done => {
            task.site = 'site';
            sinon.stub(task, '_setupSourceClients').returns();
            task.S3source = {
                send: () => Promise.resolve({
                    ReplicationConfiguration: {
                        Role: 'arn:aws:iam::111:role/src,arn:aws:iam::000:role/repRule',
                        Rules: [{
                            Status: 'Enabled',
                            Prefix: '',
                            Destination: {
                                Bucket: 'arn:aws:s3:::bucket-a',
                                StorageClass: 'site',
                                Account: '222',
                            },
                        }],
                    },
                }),
            };
            const entry = _makeEntry([{
                site: 'site', status: 'PENDING', dataStoreVersionId: '',
                destination: 'arn:aws:s3:::bucket-a',
                role: 'arn:aws:iam::222:role/repRule',
            }]);
            task._setupRolesOnce(entry, fakeLogger, (err, src, dst) => {
                assert.ifError(err);
                assert.strictEqual(src, 'arn:aws:iam::111:role/src');
                assert.strictEqual(dst, 'arn:aws:iam::222:role/repRule');
                done();
            });
        });

        it('rejects when per-backend role does not match substituted role', done => {
            task.site = 'site';
            sinon.stub(task, '_setupSourceClients').returns();
            task.S3source = {
                send: () => Promise.resolve({
                    ReplicationConfiguration: {
                        Role: 'arn:aws:iam::111:role/src,arn:aws:iam::000:role/repRule',
                        Rules: [{
                            Status: 'Enabled',
                            Prefix: '',
                            Destination: {
                                Bucket: 'arn:aws:s3:::bucket-a',
                                StorageClass: 'site',
                                Account: '222',
                            },
                        }],
                    },
                }),
            };
            const entry = _makeEntry([{
                site: 'site', status: 'PENDING', dataStoreVersionId: '',
                destination: 'arn:aws:s3:::bucket-a',
                role: 'arn:aws:iam::999:role/repRule',
            }]);
            task._setupRolesOnce(entry, fakeLogger, err => {
                assert(err);
                assert.strictEqual(err.is.BadRole, true);
                done();
            });
        });

        it('matches V2 rules by Filter.Prefix', done => {
            task.site = 'site';
            sinon.stub(task, '_setupSourceClients').returns();
            task.S3source = {
                send: () => Promise.resolve({
                    ReplicationConfiguration: {
                        Role: 'arn:aws:iam::111:role/src,arn:aws:iam::222:role/dst',
                        Rules: [{
                            Status: 'Enabled',
                            Filter: { Prefix: 'logs/' },
                            Destination: {
                                Bucket: 'arn:aws:s3:::bucket-a',
                                StorageClass: 'site',
                            },
                        }],
                    },
                }),
            };

            // Object key 'logs/x' is under the 'logs/' filter prefix,
            // so replication should proceed.
            const entry = new ObjectQueueEntry('source-bucket', 'logs/key', {
                'md-model-version': 2,
                'replicationInfo': {
                    status: 'PENDING',
                    content: ['DATA', 'METADATA'],
                    destination: '',
                    role: 'arn:aws:iam::111:role/src',
                    backends: [{
                        site: 'site', status: 'PENDING', dataStoreVersionId: '',
                        destination: 'arn:aws:s3:::bucket-a',
                        role: 'arn:aws:iam::222:role/dst',
                    }],
                },
            });

            entry
                .setReplicationBackend({
                    site: 'site',
                    destination: 'arn:aws:s3:::bucket-a',
                    role: 'arn:aws:iam::222:role/dst',
                });

            task._setupRolesOnce(entry, fakeLogger, err => {
                assert.ifError(err);
                done();
            });
        });

        it('rejects with PreconditionFailed when V1 prefix does not match the object key', done => {
            task.site = 'site';
            sinon.stub(task, '_setupSourceClients').returns();
            task.S3source = {
                send: () => Promise.resolve({
                    ReplicationConfiguration: {
                        Role: 'arn:aws:iam::111:role/src,arn:aws:iam::222:role/dst',
                        Rules: [{
                            Status: 'Enabled',
                            Prefix: 'logs/',
                            Destination: {
                                Bucket: 'arn:aws:s3:::bucket-a',
                                StorageClass: 'site',
                            },
                        }],
                    },
                }),
            };

            const entry = _makeEntry([{
                site: 'site',
                status: 'PENDING',
                dataStoreVersionId: '',
                destination: 'arn:aws:s3:::bucket-a',
                role: 'arn:aws:iam::222:role/dst',
            }]);

            task._setupRolesOnce(entry, fakeLogger, err => {
                assert(err);
                assert.strictEqual(err.is.PreconditionFailed, true);
                done();
            });
        });

        it('rejects with PreconditionFailed when the only matching rule is Disabled', done => {
            task.site = 'site';
            sinon.stub(task, '_setupSourceClients').returns();
            task.S3source = {
                send: () => Promise.resolve({
                    ReplicationConfiguration: {
                        Role: 'arn:aws:iam::111:role/src,arn:aws:iam::222:role/dst',
                        Rules: [{
                            Status: 'Disabled',
                            Prefix: '',
                            Destination: {
                                Bucket: 'arn:aws:s3:::bucket-a',
                                StorageClass: 'site',
                            },
                        }],
                    },
                }),
            };
            const entry = _makeEntry([{
                site: 'site',
                status: 'PENDING',
                dataStoreVersionId: '',
                destination: 'arn:aws:s3:::bucket-a',
                role: 'arn:aws:iam::222:role/dst',
            }]);

            task._setupRolesOnce(entry, fakeLogger, err => {
                assert(err);
                assert.strictEqual(err.is.PreconditionFailed, true);
                done();
            });
        });

        it('accepts when at least one enabled rule matches among several', done => {
            task.site = 'site';
            sinon.stub(task, '_setupSourceClients').returns();
            task.S3source = {
                send: () => Promise.resolve({
                    ReplicationConfiguration: {
                        Role: 'arn:aws:iam::111:role/src,arn:aws:iam::222:role/dst',
                        Rules: [
                            {
                                Status: 'Enabled',
                                Prefix: 'logs/',
                                Destination: {
                                    Bucket: 'arn:aws:s3:::ignored',
                                    StorageClass: 'site',
                                },
                            },
                            {
                                Status: 'Enabled',
                                Prefix: '',
                                Destination: {
                                    Bucket: 'arn:aws:s3:::bucket-a',
                                    StorageClass: 'site',
                                },
                            },
                        ],
                    },
                }),
            };

            const entry = _makeEntry([{
                site: 'site',
                status: 'PENDING',
                dataStoreVersionId: '',
                destination: 'arn:aws:s3:::bucket-a',
                role: 'arn:aws:iam::222:role/dst',
            }]);

            task._setupRolesOnce(entry, fakeLogger, err => {
                assert.ifError(err);
                done();
            });
        });

        it('rejects with PreconditionFailed when no rule matches', done => {
            task.site = 'site';
            sinon.stub(task, '_setupSourceClients').returns();
            task.S3source = {
                send: () => Promise.resolve({
                    ReplicationConfiguration: {
                        Role: 'arn:aws:iam::111:role/src,arn:aws:iam::222:role/dst',
                        Rules: [
                            {
                                Status: 'Enabled',
                                Prefix: 'logs/',
                                Destination: {
                                    Bucket: 'arn:aws:s3:::bucket-a',
                                    StorageClass: 'site',
                                }
                            },
                            {
                                Status: 'Enabled',
                                Prefix: 'data/',
                                Destination: {
                                    Bucket: 'arn:aws:s3:::bucket-a',
                                    StorageClass: 'site',
                                },
                            },
                        ],
                    },
                }),
            };

            const entry = _makeEntry([{
                site: 'site', status: 'PENDING', dataStoreVersionId: '',
                destination: 'arn:aws:s3:::bucket-a',
                role: 'arn:aws:iam::222:role/dst',
            }]);

            task._setupRolesOnce(entry, fakeLogger, err => {
                assert(err);
                assert.strictEqual(err.is.PreconditionFailed, true);
                done();
            });
        });

        it('rejects with BadRole when the bucket config role has more than two ARNs', done => {
            task.site = 'site';
            sinon.stub(task, '_setupSourceClients').returns();
            task.S3source = {
                send: () => Promise.resolve({
                    ReplicationConfiguration: {
                        Role: 'arn:aws:iam::111:role/a,arn:aws:iam::222:role/b,arn:aws:iam::333:role/c',
                        Rules: [{
                            Status: 'Enabled',
                            Prefix: '',
                            Destination: {
                                Bucket: 'arn:aws:s3:::bucket-a',
                                StorageClass: 'site',
                            },
                        }],
                    },
                }),
            };

            const entry = _makeEntry([{
                site: 'site',
                status: 'PENDING',
                dataStoreVersionId: '',
                destination: 'arn:aws:s3:::bucket-a',
                role: 'arn:aws:iam::222:role/dst',
            }]);

            task._setupRolesOnce(entry, fakeLogger, err => {
                assert(err);
                assert.strictEqual(err.is.BadRole, true);
                done();
            });
        });

        it('picks the rule matching the backend destination when several share a StorageClass', done => {
            task.site = 'site';
            sinon.stub(task, '_setupSourceClients').returns();
            task.S3source = {
                send: () => Promise.resolve({
                    ReplicationConfiguration: {
                        Role: 'arn:aws:iam::111:role/src,arn:aws:iam::222:role/dst',
                        Rules: [
                            {
                                Status: 'Enabled',
                                Filter: { Prefix: '' },
                                Destination: {
                                    Bucket: 'arn:aws:s3:::bucket-a',
                                    StorageClass: 'site',
                                    Account: '222',
                                },
                            },
                            {
                                Status: 'Enabled',
                                Filter: { Prefix: '' },
                                Destination: {
                                    Bucket: 'arn:aws:s3:::bucket-b',
                                    StorageClass: 'site',
                                    Account: '333',
                                },
                            },
                        ],
                    },
                }),
            };

            const entry = _makeEntry([{
                site: 'site',
                status: 'PENDING',
                dataStoreVersionId: '',
                destination: 'arn:aws:s3:::bucket-b',
                role: 'arn:aws:iam::333:role/dst',
            }]);

            task._setupRolesOnce(entry, fakeLogger, (err, src, dst) => {
                assert.ifError(err);
                assert.strictEqual(src, 'arn:aws:iam::111:role/src');
                assert.strictEqual(dst, 'arn:aws:iam::333:role/dst');
                done();
            });
        });

        it('rejects when the backend role does not match its rule Account substitution', done => {
            task.site = 'site';
            sinon.stub(task, '_setupSourceClients').returns();
            task.S3source = {
                send: () => Promise.resolve({
                    ReplicationConfiguration: {
                        Role: 'arn:aws:iam::111:role/src,arn:aws:iam::222:role/dst',
                        Rules: [
                            {
                                Status: 'Enabled',
                                Filter: { Prefix: '' },
                                Destination: {
                                    Bucket: 'arn:aws:s3:::bucket-a',
                                    StorageClass: 'site',
                                    Account: '222',
                                },
                            },
                            {
                                Status: 'Enabled',
                                Filter: { Prefix: '' },
                                Destination: {
                                    Bucket: 'arn:aws:s3:::bucket-b',
                                    StorageClass: 'site',
                                    Account: '333',
                                },
                            },
                        ],
                    },
                }),
            };

            const entry = _makeEntry([{
                site: 'site',
                status: 'PENDING',
                dataStoreVersionId: '',
                destination: 'arn:aws:s3:::bucket-b',
                role: 'arn:aws:iam::999:role/dst',
            }]);

            task._setupRolesOnce(entry, fakeLogger, err => {
                assert(err);
                assert.strictEqual(err.is.BadRole, true);
                done();
            });
        });

        it('falls back to literal compare for legacy configs without Account', done => {
            task.site = 'site';
            sinon.stub(task, '_setupSourceClients').returns();
            task.S3source = {
                send: () => Promise.resolve({
                    ReplicationConfiguration: {
                        Role: 'arn:aws:iam::111:role/src,arn:aws:iam::222:role/legacy',
                        Rules: [{
                            Status: 'Enabled',
                            Prefix: '',
                            Destination: {
                                Bucket: 'arn:aws:s3:::bucket-a',
                                StorageClass: 'site',
                            },
                        }],
                    },
                }),
            };
            const entry = new ObjectQueueEntry('source-bucket', 'key', {
                'md-model-version': 2,
                'replicationInfo': {
                    status: 'PENDING',
                    content: ['DATA', 'METADATA'],
                    destination: 'arn:aws:s3:::bucket-a',
                    role: 'arn:aws:iam::111:role/src,arn:aws:iam::222:role/legacy',
                    backends: [{
                        site: 'site', status: 'PENDING', dataStoreVersionId: '',
                    }],
                },
            }).setSite('site');
            task._setupRolesOnce(entry, fakeLogger, (err, src, dst) => {
                assert.ifError(err);
                assert.strictEqual(src, 'arn:aws:iam::111:role/src');
                assert.strictEqual(dst, 'arn:aws:iam::222:role/legacy');
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
                    host: 'sts.zenko.local',
                    port: 80,
                    accessKey: 'accessKey1',
                    secretKey: 'verySecretKey1',
                },
            });
            assert.deepStrictEqual(task.clientManager._s3Config, {
                host: 's3.zenko.local',
                port: 80,
            });
            assert.deepStrictEqual(task.clientManager._transport, 'http');
            assert.deepStrictEqual(task.clientManager._stsConfig.endpoint, 'http://sts.zenko.local:80');
            assert.deepStrictEqual(task.clientManager._stsConfig.credentials, {
                accessKeyId: 'accessKey1',
                secretAccessKey: 'verySecretKey1',
            });
        });

        it('should setup destination BackbeatClient with proper creds when not in assumeRole', async () => {
            task.destConfig.auth = {
                type: 'service',
                account: 'replication-service',
            };
            sinon.stub(task, '_createCredentials').returns({
                getCredentialsProvider: () => async () => ({
                    accessKeyId: 'accessKeyNoAssumeRole',
                    secretAccessKey: 'secretKeyNoAssumeRole',
                }),
            });
            task._setupDestClients('arn:aws:iam::123456789012:role/crr-role', fakeLogger);
            const endpointObject = await task.backbeatDest.config.endpoint();
            const port = endpointObject.port || (endpointObject.protocol === 'https:' ? 443 : 80);
            const endpoint = `${endpointObject.protocol}//` +
                `${endpointObject.hostname}:${port}` +
                `${endpointObject.path}`;
            const credentials = await task.backbeatDest.config.credentials();
            assert.strictEqual(credentials.accessKeyId, 'accessKeyNoAssumeRole');
            assert.strictEqual(credentials.secretAccessKey, 'secretKeyNoAssumeRole');
            assert.strictEqual(endpoint, 'http://s3.zenko.local:80/');
        });
    });

    describe('_shouldSkipMetadata', () => {
        let sourceMvId, olderSourceMvId, encodedNewer, encodedOlder;

        before(() => {
            const ids = makeMicroVersionIds();
            olderSourceMvId = ids.older;
            sourceMvId = ids.newer;
            encodedNewer = ids.newerEncoded;
            encodedOlder = ids.olderEncoded;
        });

        it('returns false when conflict is null', () => {
            assert.strictEqual(task._shouldSkipMetadata(sourceMvId, null, fakeLogger), false);
        });

        it('returns false when conflict is undefined', () => {
            assert.strictEqual(task._shouldSkipMetadata(sourceMvId, undefined, fakeLogger), false);
        });

        it('returns false when conflict has no microVersionId field', () => {
            assert.strictEqual(task._shouldSkipMetadata(sourceMvId, { isCollision: true }, fakeLogger), false);
        });

        it('returns false when conflict has null microVersionId', () => {
            const conflict = { isCollision: true, microVersionId: null };
            assert.strictEqual(task._shouldSkipMetadata(sourceMvId, conflict, fakeLogger), false);
        });

        it('returns false when conflict has undecodable microVersionId', () => {
            const conflict = { isCollision: true, microVersionId: 'tooshort' };
            assert.strictEqual(task._shouldSkipMetadata(sourceMvId, conflict, fakeLogger), false);
        });

        it('returns false when source microVersionId is null (cannot compare)', () => {
            const conflict = { isCollision: true, microVersionId: encodedNewer };
            assert.strictEqual(task._shouldSkipMetadata(null, conflict, fakeLogger), false);
        });

        it('returns true when source revision equals destination revision', () => {
            const conflict = { isCollision: true, microVersionId: encodedNewer };
            assert.strictEqual(task._shouldSkipMetadata(sourceMvId, conflict, fakeLogger), true);
        });

        it('returns true when source revision is older than destination revision', () => {
            const conflict = { isCollision: true, microVersionId: encodedNewer };
            assert.strictEqual(task._shouldSkipMetadata(olderSourceMvId, conflict, fakeLogger), true);
        });

        it('returns false when source revision is newer than destination revision', () => {
            const conflict = { isCollision: true, microVersionId: encodedOlder };
            assert.strictEqual(task._shouldSkipMetadata(sourceMvId, conflict, fakeLogger), false);
        });
    });

    describe('_putMetadataOnce with conflict', () => {
        it('skips the request when conflict revision is equal to source (already at destination)', done => {
            sinon.stub(task, '_publishMetadataWriteMetrics').returns();
            const { newer, newerEncoded } = makeMicroVersionIds();
            const entry = QueueEntry.createFromKafkaEntry(replicationEntry);
            sinon.stub(entry, 'getMicroVersionId').returns(newer);
            const conflict = { isCollision: true, microVersionId: newerEncoded };
            const sendStub = sinon.stub().resolves({});
            task.backbeatDest = { send: sendStub };
            task.targetRole = 'arn:aws:iam::123456789012:role/crr-role';
            task._putMetadataOnce(entry, false, conflict, fakeLogger, err => {
                assert.ifError(err);
                sinon.assert.notCalled(sendStub);
                done();
            });
        });

        it('proceeds with the request when conflict revision is older than source', done => {
            sinon.stub(task, '_publishMetadataWriteMetrics').returns();
            const { olderEncoded, newer } = makeMicroVersionIds();
            const entry = QueueEntry.createFromKafkaEntry(replicationEntry);
            sinon.stub(entry, 'getMicroVersionId').returns(newer);
            const conflict = { isCollision: true, microVersionId: olderEncoded };
            const sendStub = sinon.stub().resolves({});
            task.backbeatDest = { send: sendStub };
            task.targetRole = 'arn:aws:iam::123456789012:role/crr-role';
            task._putMetadataOnce(entry, false, conflict, fakeLogger, err => {
                assert.ifError(err);
                sinon.assert.calledOnce(sendStub);
                done();
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
