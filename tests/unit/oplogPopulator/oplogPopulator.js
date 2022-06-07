const assert = require('assert');
const sinon = require('sinon');
const errors = require('arsenal').errors;
const werelogs = require('werelogs');
const events = require('events');
const { MongoClient } = require('mongodb');

const logger = new werelogs.Logger('connect-wrapper-logger');

const OplogPopulator =
    require('../../../extensions/oplogPopulator/OplogPopulator');
const ChangeStream =
    require('../../../lib/wrappers/ChangeStream');

const oplogPopulatorConfig = {
    topic: 'oplog',
    kafkaConnectHost: '127.0.0.1',
    kafkaConnectPort: 8083
};

const mongoConfig = {
    replicaSetHosts:
    'localhost:27017,localhost:27018,localhost:27019',
    writeConcern: 'majority',
    replicaSet: 'rs0',
    readPreference: 'primary',
    database: 'metadata',
    authCredentials: {
        username: 'user',
        password: 'password'
    }
};

const activeExtensions = ['ingestion', 'replication', 'notification', 'lifecycle'];

describe('OplogPopulator', () => {
    let oplogPopulator;

    beforeEach(() => {
        oplogPopulator = new OplogPopulator({
            config: oplogPopulatorConfig,
            mongoConfig,
            activeExtensions,
            logger,
        });
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('constructor', () => {
        it('should fail when params invalid', () => {
            assert.throws(() => new OplogPopulator({}));
            assert.throws(() => new OplogPopulator({
                config: oplogPopulatorConfig,
            }));
            assert.throws(() => new OplogPopulator({
                mongoConfig,
            }));
            assert.throws(() => new OplogPopulator({
                activeExtensions,
            }));
            assert.throws(() => new OplogPopulator({
                logger,
            }));
            assert.throws(() => new OplogPopulator({
                config: oplogPopulatorConfig,
                mongoConfig,
            }));
            assert.throws(() => new OplogPopulator({
                config: oplogPopulatorConfig,
                activeExtensions,
            }));
            assert.throws(() => new OplogPopulator({
                config: oplogPopulatorConfig,
                logger,
            }));
            assert.throws(() => new OplogPopulator({
                mongoConfig: oplogPopulatorConfig,
                activeExtensions,
            }));
            assert.throws(() => new OplogPopulator({
                mongoConfig: oplogPopulatorConfig,
                logger,
            }));
            assert.throws(() => new OplogPopulator({
                activeExtensions,
                logger,
            }));
            const op = new OplogPopulator({
                config: oplogPopulatorConfig,
                mongoConfig,
                activeExtensions,
                logger,
            });
            assert(op instanceof OplogPopulator);
        });

        it('should set mongo connection info', () => {
            const mongoUrl = 'mongodb://user:password@localhost:27017,' +
                'localhost:27018,localhost:27019/?w=majority&readPreference=primary&replicaSet=rs0';
            assert.strictEqual(oplogPopulator._mongoUrl, mongoUrl);
            assert.strictEqual(oplogPopulator._replicaSet, 'rs0');
            assert.strictEqual(oplogPopulator._database, 'metadata');
        });
    });

    describe('_setupMongoClient', () => {
        it('should connect to mongo and setup client', async () => {
            const collectionStub = sinon.stub();
            const dbStub = sinon.stub().returns({
                collection: collectionStub,
            });
            const mongoConnectStub = sinon.stub(MongoClient, 'connect')
                .resolves({ db: dbStub });
            await oplogPopulator._setupMongoClient()
            .then(() => {
                const mongoUrl = 'mongodb://user:password@localhost:27017,localhost:27018,' +
                    'localhost:27019/?w=majority&readPreference=primary&replicaSet=rs0';
                assert(mongoConnectStub.calledOnceWith(
                    mongoUrl,
                    {
                        replicaSet: 'rs0',
                        useNewUrlParser: true
                    }
                ));
                assert(dbStub.calledOnceWith('metadata', { ignoreUndefined: true }));
                assert(collectionStub.calledOnceWith('__metastore'));
            }).catch(err => assert.ifError(err));
        });

        it('should fail when mongo connection fails', async () => {
            const mongoConnectStub = sinon.stub(MongoClient, 'connect')
                .rejects(errors.InternalError);
            await oplogPopulator._setupMongoClient()
            .then(() => {
                const mongoUrl = 'mongodb://user:password@localhost:27017,' +
                    'localhost:27018,localhost:27019/?w=majority&readPreference=primary';
                assert(mongoConnectStub.calledOnceWith(
                    mongoUrl,
                    {
                        replicaSet: 'rs0',
                        useNewUrlParser: true
                    }
                ));
            }).catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('should fail if it can\'t get metadata db', async () => {
            const dbStub = sinon.stub().returns({
                collection: sinon.stub().throws(errors.InternalError),
            });
            const mongoConnectStub = sinon.stub(MongoClient, 'connect')
                .resolves({ db: dbStub });
            await oplogPopulator._setupMongoClient()
            .then(() => {
                const mongoUrl = 'mongodb://user:password@localhost:27017,' +
                    'localhost:27018,localhost:27019/?w=majority&readPreference=primary';
                assert(mongoConnectStub.calledOnceWith(
                    mongoUrl,
                    {
                        replicaSet: 'rs0',
                        useNewUrlParser: true
                    }
                ));
                assert(dbStub.calledOnceWith('metadata', { ignoreUndefined: true }));
            }).catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('should fail if it can\'t get metastore collection', async () => {
            const collectionStub = sinon.stub().throws(errors.InternalError);
            const dbStub = sinon.stub().returns({
                collection: collectionStub,
            });
            const mongoConnectStub = sinon.stub(MongoClient, 'connect')
                .resolves({ db: dbStub });
            await oplogPopulator._setupMongoClient()
            .then(() => {
                const mongoUrl = 'mongodb://user:password@localhost:27017,' +
                    'localhost:27018,localhost:27019/?w=majority&readPreference=primary';
                assert(mongoConnectStub.calledOnceWith(
                    mongoUrl,
                    {
                        replicaSet: 'rs0',
                        useNewUrlParser: true
                    }
                ));
                assert(dbStub.calledOnceWith('metadata', { ignoreUndefined: true }));
                assert(collectionStub.calledOnceWith('__metastore'));
            }).catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });


    describe('_getBackbeatEnabledBuckets', () => {
        [
            {
                extensions: ['notification'],
                filter: [
                    { 'value.notificationConfiguration': { $type: 3 } },
                ],
            },
            {
                extensions: ['replication'],
                filter: [
                    {
                        'value.replicationConfiguration.rules': {
                            $elemMatch: {
                                enabled: true,
                            },
                        },
                    },
                ],
            },
            {
                extensions: ['lifecycle'],
                filter: [
                    {
                        'value.lifecycleConfiguration.rules': {
                            $elemMatch: {
                                ruleStatus: 'Enabled',
                            },
                        },
                    },
                ],
            },
            {
                extensions: ['ingestion'],
                filter: [
                    {
                        'value.ingestion.status': 'enabled',
                    }
                ],
            },
            {
                extensions: ['notification', 'replication', 'ingestion', 'lifecycle'],
                filter: [
                    { 'value.notificationConfiguration': { $type: 3 } },
                    {
                        'value.replicationConfiguration.rules': {
                            $elemMatch: {
                                enabled: true,
                            },
                        },
                    },
                    {
                        'value.lifecycleConfiguration.rules': {
                            $elemMatch: {
                                ruleStatus: 'Enabled',
                            },
                        },
                    },
                    {
                        'value.ingestion.status': 'enabled',
                    }
                ],
            }
        ].forEach(scenario => {
            const { extensions, filter } = scenario;
            it(`should correctly set filter (${extensions})`, async () => {
                const findStub = sinon.stub().returns({
                    project: () => ({
                        map: () => ({
                            toArray: () => ['example-bucket-1'],
                        }),
                    })
                });
                oplogPopulator._metastore = { find: findStub };
                oplogPopulator._activeExtensions = extensions;
                await oplogPopulator._getBackbeatEnabledBuckets()
                .then(buckets => {
                    assert(findStub.calledOnceWith({
                        $or: filter,
                    }));
                    assert.deepEqual(buckets, ['example-bucket-1']);
                })
                .catch(err => assert.ifError(err));
            });
        });

        it('should return fail when toArray operation fails', async () => {
            const findStub = sinon.stub().returns({
                project: () => ({
                    map: () => ({
                        toArray: sinon.stub().throws(errors.InternalError),
                    }),
                })
            });
            oplogPopulator._metastore = { find: findStub };
            await oplogPopulator._getBackbeatEnabledBuckets()
            .then(buckets => {
                assert.strict(buckets, undefined);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('should return fail when map operation fails', async () => {
            const findStub = sinon.stub().returns({
                project: () => ({
                    map: () => sinon.stub().throws(errors.InternalError),
                })
            });
            oplogPopulator._metastore = { find: findStub };
            await oplogPopulator._getBackbeatEnabledBuckets()
            .then(buckets => {
                assert.strict(buckets, undefined);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('should return fail when project operation fails', async () => {
            const findStub = sinon.stub().returns({
                project: () => sinon.stub().throws(errors.InternalError),
            });
            oplogPopulator._metastore = { find: findStub };
            await oplogPopulator._getBackbeatEnabledBuckets()
            .then(buckets => {
                assert.strict(buckets, undefined);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('should return fail when find operation fails', async () => {
            const findStub = sinon.stub().throws(errors.InternalError);
            oplogPopulator._metastore = { find: findStub };
            await oplogPopulator._getBackbeatEnabledBuckets()
            .then(buckets => {
                assert.strict(buckets, undefined);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });

    describe('_isBucketBackbeatEnabled', () => {
        [
            {
                exts: 'all extensions active',
                metadata: {
                    notificationConfiguration: {},
                    lifecycleConfiguration: {
                        rules: [
                            {
                                ruleStatus: 'Enabled',
                            }
                        ]
                    },
                    replicationConfiguration: {
                        rules: [
                            {
                                enabled: true,
                            },
                        ]
                    },
                    ingestion: {
                        status: 'enabled',
                    },
                },
                result: true,
            },
            {
                exts: 'notification active',
                metadata: {
                    notificationConfiguration: {},
                    replicationConfiguration: null,
                    lifecycleConfiguration: null,
                    ingestion: null,
                },
                result: true,
            },
            {
                exts: 'replication active',
                metadata: {
                    notificationConfiguration: null,
                    lifecycleConfiguration: null,
                    ingestion: null,
                    replicationConfiguration: {
                        rules: [
                            {
                                enabled: false,
                            },
                            {
                                enabled: true,
                            },
                        ]
                    },
                },
                result: true,
            },
            {
                exts: 'replication disabled',
                metadata: {
                    notificationConfiguration: null,
                    lifecycleConfiguration: null,
                    ingestion: null,
                    replicationConfiguration: {
                        rules: [
                            {
                                enabled: false,
                            },
                        ]
                    },
                },
                result: false,
            },
            {
                exts: 'replication no rules',
                metadata: {
                    notificationConfiguration: null,
                    lifecycleConfiguration: null,
                    ingestion: null,
                    replicationConfiguration: {
                        rules: []
                    },
                },
                result: false,
            },
            {
                exts: 'lifecycle active',
                metadata: {
                    notificationConfiguration: null,
                    replicationConfiguration: null,
                    ingestion: null,
                    lifecycleConfiguration: {
                        rules: [
                            {
                                ruleStatus: 'Disabled',
                            },
                            {
                                ruleStatus: 'Enabled',
                            }
                        ]
                    },
                },
                result: true,
            },
            {
                exts: 'lifecycle no rules',
                metadata: {
                    notificationConfiguration: null,
                    replicationConfiguration: null,
                    ingestion: null,
                    lifecycleConfiguration: {
                        rules: []
                    },
                },
                result: false,
            },
            {
                exts: 'lifecycle no rules active',
                metadata: {
                    notificationConfiguration: null,
                    replicationConfiguration: null,
                    ingestion: null,
                    lifecycleConfiguration: {
                        rules: [
                            {
                                ruleStatus: 'Disabled',
                            },
                        ]
                    },
                },
                result: false,
            },
            {
                exts: 'ingestion active',
                metadata: {
                    notificationConfiguration: null,
                    replicationConfiguration: null,
                    lifecycleConfiguration: null,
                    ingestion: {
                        status: 'enabled',
                    },
                },
                result: true,
            },
            {
                exts: 'ingestion disabled',
                metadata: {
                    notificationConfiguration: null,
                    replicationConfiguration: null,
                    lifecycleConfiguration: null,
                    ingestion: {
                        status: 'disabled',
                    },
                },
                result: false,
            },
            {
                exts: 'no extension active',
                metadata: {
                    notificationConfiguration: null,
                    replicationConfiguration: null,
                    lifecycleConfiguration: null,
                },
                result: false,
            },
        ].forEach(scenario => {
            const { exts, metadata, result } = scenario;
            it(`Should validate bucket if at least one extension active (${exts})`, () => {
                const valid = oplogPopulator._isBucketBackbeatEnabled(metadata);
                assert.strictEqual(valid, result);
            });
        });
    });

    describe('_setMetastoreChangeStream ::', () =>  {
        it('Should create and listen to the metastore change stream', () => {
            const changeStreamPipeline = [
                {
                    $project: {
                        '_id': 1,
                        'operationType': 1,
                        'documentKey._id': 1,
                        'fullDocument.value': 1
                    },
                },
            ];
            oplogPopulator._metastore = {
                watch: sinon.stub().returns(new events.EventEmitter()),
            };
            oplogPopulator._setMetastoreChangeStream();
            assert(oplogPopulator._changeStreamWrapper instanceof ChangeStream);
            assert.deepEqual(oplogPopulator._changeStreamWrapper._pipeline, changeStreamPipeline);
        });
    });

    describe('_handleChangeStreamChangeEvent', () => {
        it('should stop listening to bucket if got deleted', () => {
            const stopListening = sinon.stub();
            oplogPopulator._allocator = {
                stopListeningToBucket: stopListening.resolves(),
                has: () => true,
            };
            const changeDocument = {
                operationType: 'delete',
                documentKey: {
                    _id: 'example-bucket',
                },
            };
            oplogPopulator._handleChangeStreamChangeEvent(changeDocument);
            assert(stopListening.calledOnceWith('example-bucket'));
        });

        it('should do nothing if operation type not supported', () => {
            const stopListening = sinon.stub().resolves();
            const startListening = sinon.stub().resolves();
            oplogPopulator._allocator = {
                stopListeningToBucket: stopListening,
                listenToBucket: startListening,
                has: () => true,
            };
            const changeDocument = {
                operationType: 'drop',
                documentKey: {
                    _id: 'example-bucket',
                },
            };
            oplogPopulator._handleChangeStreamChangeEvent(changeDocument);
            assert(stopListening.notCalled);
            assert(startListening.notCalled);
        });

        ['replace', 'insert', 'update'].forEach(opType => {
            it(`should stop listening if bucket becomes invalid (${opType})`, () => {
                const stopListening = sinon.stub().resolves();
                const startListening = sinon.stub().resolves();
                oplogPopulator._allocator = {
                    stopListeningToBucket: stopListening,
                    listenToBucket: startListening,
                    has: () => true,
                };
                sinon.stub(oplogPopulator, '_isBucketBackbeatEnabled')
                    .returns(false);
                const changeDocument = {
                    operationType: opType,
                    documentKey: {
                        _id: 'example-bucket',
                    },
                    fullDocument: {},
                };
                oplogPopulator._handleChangeStreamChangeEvent(changeDocument);
                assert(stopListening.calledOnceWith('example-bucket'));
                assert(startListening.notCalled);
            });

            it(`should start listening to bucket if it becomes valid ((${opType})`, () => {
                const stopListening = sinon.stub().resolves();
                const startListening = sinon.stub().resolves();
                oplogPopulator._allocator = {
                    stopListeningToBucket: stopListening,
                    listenToBucket: startListening,
                    has: () => false,
                };
                sinon.stub(oplogPopulator, '_isBucketBackbeatEnabled')
                    .returns(true);
                const changeDocument = {
                    operationType: opType,
                    documentKey: {
                        _id: 'example-bucket',
                    },
                    fullDocument: {},
                };
                oplogPopulator._handleChangeStreamChangeEvent(changeDocument);
                assert(startListening.calledOnceWith('example-bucket'));
                assert(stopListening.notCalled);
            });

            it(`should do nothing if bucket invalid and not listening to bucket ((${opType})`, () => {
                const stopListening = sinon.stub().resolves();
                const startListening = sinon.stub().resolves();
                oplogPopulator._allocator = {
                    stopListeningToBucket: stopListening,
                    listenToBucket: startListening,
                    has: () => false,
                };
                sinon.stub(oplogPopulator, '_isBucketBackbeatEnabled')
                    .returns(false);
                const changeDocument = {
                    operationType: opType,
                    documentKey: {
                        _id: 'example-bucket',
                    },
                    fullDocument: {},
                };
                oplogPopulator._handleChangeStreamChangeEvent(changeDocument);
                assert(startListening.notCalled);
                assert(stopListening.notCalled);
            });
        });
    });
});
