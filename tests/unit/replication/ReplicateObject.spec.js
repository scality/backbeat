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
            task._putMetadataOnce(entry, true, fakeLogger, err => {
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
            task._putMetadataOnce(entry, true, fakeLogger, err => {
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
