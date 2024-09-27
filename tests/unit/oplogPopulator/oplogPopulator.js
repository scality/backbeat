const assert = require('assert');
const sinon = require('sinon');
const errors = require('arsenal').errors;
const werelogs = require('werelogs');
const events = require('events');
const { MongoClient } = require('mongodb');

const logger = new werelogs.Logger('connect-wrapper-logger');

const locations = require('../../../conf/locationConfig.json');

const OplogPopulator =
    require('../../../extensions/oplogPopulator/OplogPopulator');
const ChangeStream =
    require('../../../lib/wrappers/ChangeStream');
const ConnectorsManager = require('../../../extensions/oplogPopulator/modules/ConnectorsManager');
const RetainBucketsDecorator = require('../../../extensions/oplogPopulator/allocationStrategy/RetainBucketsDecorator');
const LeastFullConnector = require('../../../extensions/oplogPopulator/allocationStrategy/LeastFullConnector');
const ImmutableConnector = require('../../../extensions/oplogPopulator/allocationStrategy/ImmutableConnector');
const AllocationStrategy = require('../../../extensions/oplogPopulator/allocationStrategy/AllocationStrategy');
const constants = require('../../../extensions/oplogPopulator/constants');

const oplogPopulatorConfig = {
    topic: 'oplog',
    kafkaConnectHost: '127.0.0.1',
    kafkaConnectPort: 8083,
    numberOfConnectors: 1,
    probeServer: { port: 8552 },
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
            enableMetrics: false,
            logger,
        });
        oplogPopulator._mongoVersion = '4.0.0';
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
                enableMetrics: false,
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

    describe('_arePipelinesImmutable', () => {
        it('should return true if pipeline is immutable', () => {
            oplogPopulator._mongoVersion = '6.0.0';
            assert(oplogPopulator._arePipelinesImmutable());
        });

        it('should return false if pipeline is not immutable', () => {
            oplogPopulator._mongoVersion = '5.0.0';
            assert(!oplogPopulator._arePipelinesImmutable());
        });
    });

    describe('initStrategy', () => {
        afterEach(() => {
            sinon.restore();
        });

        it('should return an instance of RetainBucketsDecorator for immutable pipelines', () => {
            const arePipelinesImmutableStub = sinon.stub(oplogPopulator, '_arePipelinesImmutable').returns(true);
            const strategy = oplogPopulator.initStrategy();
            assert(strategy instanceof RetainBucketsDecorator);
            assert(strategy._strategy instanceof ImmutableConnector);
            assert(arePipelinesImmutableStub.calledOnce);
        });

        it('should return an instance of RetainBucketsDecorator for immutable pipelines', () => {
            const arePipelinesImmutableStub = sinon.stub(oplogPopulator, '_arePipelinesImmutable').returns(false);
            const strategy = oplogPopulator.initStrategy();
            assert(strategy instanceof RetainBucketsDecorator);
            assert(strategy._strategy instanceof LeastFullConnector);
            assert(arePipelinesImmutableStub.calledOnce);
        });
    });

    describe('setup', () => {
        afterEach(() => {
            oplogPopulator._config.numberOfConnectors = 1;
        });

        it('should handle error during setup', async () => {
            const error = new Error('InternalError');
            const loadOplogHelperClassesStub = sinon.stub(oplogPopulator, '_loadOplogHelperClasses').throws(error);
            const loggerErrorStub = sinon.stub(oplogPopulator._logger, 'error');

            await assert.rejects(oplogPopulator.setup(), error);

            assert(loadOplogHelperClassesStub.calledOnce);
            assert(loggerErrorStub.calledWith('An error occured when setting up the OplogPopulator', {
                method: 'OplogPopulator.setup',
                error: 'InternalError',
            }));
        });

        it('should setup oplog populator', async () => {
            const setupMongoClientStub = sinon.stub(oplogPopulator, '_setupMongoClient').resolves();
            const setMetastoreChangeStreamStub = sinon.stub(oplogPopulator, '_setMetastoreChangeStream');
            const initializeConnectorsManagerStub = sinon.stub(oplogPopulator, '_initializeConnectorsManager');
            const getBackbeatEnabledBucketsStub = sinon.stub(oplogPopulator, '_getBackbeatEnabledBuckets').resolves([]);

            await oplogPopulator.setup();

            assert(setupMongoClientStub.calledOnce);
            assert(getBackbeatEnabledBucketsStub.calledOnce);
            assert(setMetastoreChangeStreamStub.calledOnce);
            assert(initializeConnectorsManagerStub.calledOnce);
        });

        it('should setup oplog populator with immutable pipelines', async () => {
            const setupMongoClientStub = sinon.stub(oplogPopulator, '_setupMongoClient').resolves();
            const setMetastoreChangeStreamStub = sinon.stub(oplogPopulator, '_setMetastoreChangeStream');
            const initializeConnectorsManagerStub = sinon.stub(oplogPopulator, '_initializeConnectorsManager');
            const getBackbeatEnabledBucketsStub = sinon.stub(oplogPopulator, '_getBackbeatEnabledBuckets').resolves([]);

            oplogPopulator._mongoVersion = '6.0.0';

            await oplogPopulator.setup();

            assert(setupMongoClientStub.calledOnce);
            assert(getBackbeatEnabledBucketsStub.calledOnce);
            assert(setMetastoreChangeStreamStub.calledOnce);
            assert(initializeConnectorsManagerStub.calledOnce);
        });

        it('should bind the connector-updated event from the connectors manager', async () => {
            const setupMongoClientStub = sinon.stub(oplogPopulator, '_setupMongoClient').resolves();
            const setMetastoreChangeStreamStub = sinon.stub(oplogPopulator, '_setMetastoreChangeStream');
            const initializeConnectorsManagerStub = sinon.stub(oplogPopulator, '_initializeConnectorsManager');
            const getBackbeatEnabledBucketsStub = sinon.stub(oplogPopulator, '_getBackbeatEnabledBuckets').resolves([]);
            await oplogPopulator.setup();
            assert(setupMongoClientStub.calledOnce);
            assert(getBackbeatEnabledBucketsStub.calledOnce);
            assert(setMetastoreChangeStreamStub.calledOnce);
            assert(initializeConnectorsManagerStub.calledOnce);
            const onConnectorUpdatedOrDestroyedStub =
            sinon.stub(oplogPopulator._allocationStrategy, 'onConnectorUpdatedOrDestroyed');
            oplogPopulator._connectorsManager.emit(constants.connectorUpdatedEvent);
            assert(onConnectorUpdatedOrDestroyedStub.calledOnce);
        });

        it('should bind the bucket-removed event from the allocator', async () => {
            const setupMongoClientStub = sinon.stub(oplogPopulator, '_setupMongoClient').resolves();
            const setMetastoreChangeStreamStub = sinon.stub(oplogPopulator, '_setMetastoreChangeStream');
            const initializeConnectorsManagerStub = sinon.stub(oplogPopulator, '_initializeConnectorsManager');
            const getBackbeatEnabledBucketsStub = sinon.stub(oplogPopulator, '_getBackbeatEnabledBuckets').resolves([]);
            await oplogPopulator.setup();
            assert(setupMongoClientStub.calledOnce);
            assert(getBackbeatEnabledBucketsStub.calledOnce);
            assert(setMetastoreChangeStreamStub.calledOnce);
            assert(initializeConnectorsManagerStub.calledOnce);
            const onBucketRemovedStub = sinon.stub(oplogPopulator._allocationStrategy, 'onBucketRemoved');
            oplogPopulator._allocator.emit(constants.bucketRemovedFromConnectorEvent);
            assert(onBucketRemovedStub.calledOnce);
        });

        it('should bind the connectors-reconciled event from the connectors manager', async () => {
            const setupMongoClientStub = sinon.stub(oplogPopulator, '_setupMongoClient').resolves();
            const setMetastoreChangeStreamStub = sinon.stub(oplogPopulator, '_setMetastoreChangeStream');
            const initializeConnectorsManagerStub = sinon.stub(oplogPopulator, '_initializeConnectorsManager');
            const getBackbeatEnabledBucketsStub = sinon.stub(oplogPopulator, '_getBackbeatEnabledBuckets').resolves([]);
            await oplogPopulator.setup();
            assert(setupMongoClientStub.calledOnce);
            assert(getBackbeatEnabledBucketsStub.calledOnce);
            assert(setMetastoreChangeStreamStub.calledOnce);
            assert(initializeConnectorsManagerStub.calledOnce);
            const onConnectorsReconciledStub = sinon.stub(oplogPopulator._metricsHandler, 'onConnectorsReconciled');
            oplogPopulator._connectorsManager.emit(constants.connectorsReconciledEvent);
            assert(onConnectorsReconciledStub.calledOnce);
        });

        it('should not listen to metastore if no allocation strategy', async () => {
            oplogPopulator._config.numberOfConnectors = 0;
            sinon.stub(oplogPopulator, '_setupMongoClient').resolves();
            const setMetastoreChangeStreamStub = sinon.stub(oplogPopulator, '_setMetastoreChangeStream');
            const initializeConnectorsManagerStub = sinon.stub(oplogPopulator, '_initializeConnectorsManager');
            sinon.stub(oplogPopulator, '_getBackbeatEnabledBuckets').resolves([]);
            oplogPopulator._allocationStrategy = null;
            await oplogPopulator.setup();
            assert(setMetastoreChangeStreamStub.notCalled);
            assert(initializeConnectorsManagerStub.calledOnce);
        });
    });

    describe('_initializeConnectorsManager', () => {
        it('should initialize connectors manager', async () => {
            oplogPopulator._connectorsManager = new ConnectorsManager({
                nbConnectors: oplogPopulator._config.numberOfConnectors,
                database: oplogPopulator._database,
                mongoUrl: oplogPopulator._mongoUrl,
                oplogTopic: oplogPopulator._config.topic,
                cronRule: oplogPopulator._config.connectorsUpdateCronRule,
                prefix: oplogPopulator._config.prefix,
                heartbeatIntervalMs: oplogPopulator._config.heartbeatIntervalMs,
                kafkaConnectHost: oplogPopulator._config.kafkaConnectHost,
                kafkaConnectPort: oplogPopulator._config.kafkaConnectPort,
                metricsHandler: oplogPopulator._metricsHandler,
                allocationStrategy: new AllocationStrategy({ logger }),
                logger: oplogPopulator._logger,
            });
            const connectorsManagerStub = sinon.stub(oplogPopulator._connectorsManager, 'initializeConnectors');
            await oplogPopulator._initializeConnectorsManager();
            assert(connectorsManagerStub.calledOnce);
        });
    });

    describe('_setupMongoClient', () => {
        it('should connect to mongo and setup client', async () => {
            const collectionStub = sinon.stub();
            const mongoCommandStub = sinon.stub().returns({
                version: '4.3.17',
            });
            const dbStub = sinon.stub().returns({
                collection: collectionStub,
                command: mongoCommandStub,
            });
            const mongoConnectStub = sinon.stub(MongoClient, 'connect').resolves({
                db: dbStub,
            });
            await oplogPopulator._setupMongoClient()
                .then(() => {
                    const mongoUrl = 'mongodb://user:password@localhost:27017,localhost:27018,' +
                        'localhost:27019/?w=majority&readPreference=primary&replicaSet=rs0';
                    assert(mongoConnectStub.calledOnceWith(
                        mongoUrl,
                        {
                            replicaSet: 'rs0',
                            useNewUrlParser: true,
                            useUnifiedTopology: true,
                        }
                    ));
                    assert(dbStub.calledOnceWith('metadata', { ignoreUndefined: true }));
                    assert(collectionStub.calledOnceWith('__metastore'));
                    assert(mongoCommandStub.calledOnceWith({
                        buildInfo: 1,
                    }));
                    assert.equal(oplogPopulator._mongoVersion, '4.3.17');
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
        const coldLocations = Object.keys(locations).filter(loc => locations[loc].isCold);
        const replicationFilter = {
            'value.replicationConfiguration.rules': {
                $elemMatch: {
                    enabled: true,
                },
            },
        };
        const notificationFilter = {
            'value.notificationConfiguration': {
                $type: 3,
                $not: {
                    $size: 0,
                },
            },
        };
        const lifecycleFilter = {
            'value.lifecycleConfiguration.rules': {
                $elemMatch: {
                    $and: [
                        {
                            actions: {
                                $elemMatch: {
                                    actionName: {
                                        $in: ['Transition', 'NoncurrentVersionTransition'],
                                    },
                                },
                            },
                        }, {
                            $or: [
                                {
                                    ruleStatus: 'Enabled'
                                },
                                {
                                    actions: {
                                        $elemMatch: {
                                            transition: {
                                                $elemMatch: {
                                                    storageClass: {
                                                        $in: coldLocations
                                                    }
                                                },
                                            },
                                        },
                                    },
                                },
                                {
                                    actions: {
                                        $elemMatch: {
                                            nonCurrentVersionTransition: {
                                                $elemMatch: {
                                                    storageClass: {
                                                        $in: coldLocations
                                                    }
                                                },
                                            },
                                        },
                                    }
                                },
                            ],
                        },
                    ],
                },
            },
        };
        [
            {
                extensions: ['notification'],
                filter: [
                    notificationFilter,
                ],
            },
            {
                extensions: ['replication'],
                filter: [
                    replicationFilter
                ],
            },
            {
                extensions: ['lifecycle'],
                filter: [
                    lifecycleFilter,
                ],
            },
            {
                extensions: ['notification', 'replication', 'lifecycle'],
                filter: [
                    notificationFilter,
                    replicationFilter,
                    lifecycleFilter,
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
                const tmpOplogPopulator = new OplogPopulator({
                    config: oplogPopulatorConfig,
                    mongoConfig,
                    activeExtensions: extensions,
                    enableMetrics: false,
                    logger,
                });
                tmpOplogPopulator._metastore = { find: findStub };
                tmpOplogPopulator._loadOplogHelperClasses();
                await tmpOplogPopulator._getBackbeatEnabledBuckets()
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
                desc: 'all extensions active',
                exts: ['notification', 'lifecycle', 'replication'],
                metadata: {
                    notificationConfiguration: {
                        queueConfig: [
                            {
                                events: [
                                    's3:ObjectCreated:*',
                                ],
                                queueArn: 'arn:scality:bucketnotif:::dest1',
                            }
                        ]
                    },
                    lifecycleConfiguration: {
                        rules: [
                            {
                                ruleStatus: 'Enabled',
                                actions: [
                                    {
                                        actionName: 'Transition',
                                        transition: [
                                            {
                                                days: 0,
                                                storageClass: 'dmf',
                                            },
                                        ],
                                    },
                                ],
                            }
                        ]
                    },
                    replicationConfiguration: {
                        rules: [
                            {
                                enabled: true,
                            },
                        ]
                    }
                },
                result: true,
            },
            {
                desc: 'notification active',
                exts: ['notification'],
                metadata: {
                    notificationConfiguration: {
                        queueConfig: [
                            {
                                events: [
                                    's3:ObjectCreated:*',
                                ],
                                queueArn: 'arn:scality:bucketnotif:::dest1',
                            }
                        ]
                    },
                    replicationConfiguration: null,
                    lifecycleConfiguration: null,
                    ingestion: null,
                },
                result: true,
            },
            {
                desc: 'notification disabled',
                exts: ['notification'],
                metadata: {
                    notificationConfiguration: {},
                    replicationConfiguration: null,
                    lifecycleConfiguration: null,
                    ingestion: null,
                },
                result: false,
            },
            {
                desc: 'replication mixed rules',
                exts: ['replication'],
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
                desc: 'single replication rule',
                exts: [],
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
                desc: 'no replication rules',
                exts: [],
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
                desc: 'lifecycle mixed rules',
                exts: ['lifecycle'],
                metadata: {
                    notificationConfiguration: null,
                    replicationConfiguration: null,
                    ingestion: null,
                    lifecycleConfiguration: {
                        rules: [
                            {
                                ruleStatus: 'Disabled',
                                actions: [
                                    {
                                        actionName: 'Transition',
                                        transition: [
                                            {
                                                days: 0,
                                                storageClass: 'dmf1',
                                            },
                                        ],
                                    },
                                ],
                            },
                            {
                                ruleStatus: 'Enabled',
                                actions: [
                                    {
                                        actionName: 'Transition',
                                        transition: [
                                            {
                                                days: 0,
                                                storageClass: 'dmf2',
                                            },
                                        ],
                                    },
                                ],
                            }
                        ]
                    },
                },
                result: true,
            },
            {
                desc: 'no lifecycle rules',
                exts: [],
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
                desc: 'lifecycle rule disabled',
                exts: [],
                metadata: {
                    notificationConfiguration: null,
                    replicationConfiguration: null,
                    ingestion: null,
                    lifecycleConfiguration: {
                        rules: [
                            {
                                ruleStatus: 'Disabled',
                                actions: [
                                    {
                                        actionName: 'Transition',
                                        transition: [
                                            {
                                                days: 0,
                                                storageClass: 'dmf',
                                            },
                                        ],
                                    },
                                ],
                            },
                        ]
                    },
                },
                result: false,
            },
            {
                desc: 'no extension active',
                exts: [],
                metadata: {
                    notificationConfiguration: null,
                    replicationConfiguration: null,
                    lifecycleConfiguration: null,
                },
                result: false,
            },
        ].forEach(scenario => {
            const { desc, exts, metadata, result } = scenario;
            it(`Should validate bucket if at least one extension active (${desc})`, () => {
                const tmpOplogPopulator = new OplogPopulator({
                    config: oplogPopulatorConfig,
                    mongoConfig,
                    activeExtensions: exts,
                    enableMetrics: false,
                    logger,
                });
                tmpOplogPopulator._loadOplogHelperClasses();
                const valid = tmpOplogPopulator._isBucketBackbeatEnabled(metadata);
                assert.strictEqual(valid, result);
            });
        });
    });

    describe('_setMetastoreChangeStream ::', () => {
        const changeStreamPipeline = [
            {
                $project: {
                    '_id': 1,
                    'operationType': 1,
                    'documentKey._id': 1,
                    'fullDocument.value': 1,
                    'clusterTime': {
                        $dateToString: {
                            date: '$clusterTime'
                        }
                    },
                },
            },
        ];

        it('should create and listen to the metastore change stream', () => {
            oplogPopulator._metastore = {
                watch: sinon.stub().returns(new events.EventEmitter()),
            };
            oplogPopulator._setMetastoreChangeStream();
            assert(oplogPopulator._changeStreamWrapper instanceof ChangeStream);
            assert.deepEqual(oplogPopulator._changeStreamWrapper._pipeline, changeStreamPipeline);
        });

        it('should use resumeAfter with mongo 3.6', () => {
            oplogPopulator._mongoVersion = '3.6.2';
            oplogPopulator._metastore = {
                watch: sinon.stub().returns(new events.EventEmitter()),
            };
            oplogPopulator._setMetastoreChangeStream();
            assert(oplogPopulator._changeStreamWrapper instanceof ChangeStream);
            assert.equal(oplogPopulator._changeStreamWrapper._resumeField, 'resumeAfter');
        });

        it('should use resumeAfter with mongo 4.0', () => {
            oplogPopulator._mongoVersion = '4.0.7';
            oplogPopulator._metastore = {
                watch: sinon.stub().returns(new events.EventEmitter()),
            };
            oplogPopulator._setMetastoreChangeStream();
            assert(oplogPopulator._changeStreamWrapper instanceof ChangeStream);
            assert.equal(oplogPopulator._changeStreamWrapper._resumeField, 'resumeAfter');
        });

        it('should use startAfter with mongo 4.2', () => {
            oplogPopulator._mongoVersion = '4.2.3';
            oplogPopulator._metastore = {
                watch: sinon.stub().returns(new events.EventEmitter()),
            };
            oplogPopulator._setMetastoreChangeStream();
            assert(oplogPopulator._changeStreamWrapper instanceof ChangeStream);
            assert.equal(oplogPopulator._changeStreamWrapper._resumeField, 'startAfter');
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
