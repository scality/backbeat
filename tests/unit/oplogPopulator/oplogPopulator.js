const assert = require('assert');
const sinon = require('sinon');
const errors = require('arsenal').errors;
const werelogs = require('werelogs');
const events = require('events');
const MongoClient = require('mongodb').MongoClient;


const logger = new werelogs.Logger('connect-wrapper-logger');

const OplogPopulator =
    require('../../../extensions/oplogPopulator/OplogPopulator');
const KafkaConnectWrapper =
    require('../../../lib/wrappers/KafkaConnectWrapper');
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

const connectorConfig = {
    'name': 'source-connector',
    'database': 'metadata',
    'connection.uri': 'mongodb://user:password@localhost:27017,localhost:27018,' +
        'localhost:27019/?w=majority&readPreference=primary&replicaSet=rs0',
    'topic.namespace.map': '{\"*\":\"oplog\"}',
    'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
    'change.stream.full.document': 'updateLookup',
    'pipeline': '[]',
    'collection': '',
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
        it('should fail when params invalid', done => {
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
            return done();
        });

        it('should initialize kafka wrapper', done => {
            assert(oplogPopulator._connectWrapper instanceof KafkaConnectWrapper);
            return done();
        });

        it('should set mongo connection info', done => {
            const mongoUrl = 'mongodb://user:password@localhost:27017,' +
                'localhost:27018,localhost:27019/?w=majority&readPreference=primary&replicaSet=rs0';
            assert.strictEqual(oplogPopulator._mongoUrl, mongoUrl);
            assert.strictEqual(oplogPopulator._replicaSet, 'rs0');
            assert.strictEqual(oplogPopulator._database, 'metadata');
            return done();
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

    describe('getDefaultConnectorConfiguration', () => {
        it('should return default configuration (relica set)', done => {
            const config = oplogPopulator._getDefaultConnectorConfiguration(
                'source-connector');
            assert.deepEqual(config, connectorConfig);
            return done();
        });

        it('should return default configuration (sharded)', done => {
            // making config without replica set
            const shardedMongoConfig = Object.assign({}, mongoConfig);
            shardedMongoConfig.replicaSet = '';
            const op = new OplogPopulator({
                config: oplogPopulatorConfig,
                mongoConfig: shardedMongoConfig,
                activeExtensions,
                logger,
            });
            // getting default config
            const config = op._getDefaultConnectorConfiguration(
                'source-connector');
            // removing replica set from expected default config
            const shardedConnectorConfig = Object.assign({}, connectorConfig);
            shardedConnectorConfig['connection.uri'] = 'mongodb://user:password@localhost:27017,' +
                'localhost:27018,localhost:27019/?w=majority&readPreference=primary';
            assert.deepEqual(config, shardedConnectorConfig);
            return done();
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
                extensions: ['gc'],
                filter: [],
            },
            {
                extensions: ['mongoProcessor'],
                filter: [],
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

    describe('_generateNewConnectorPipeline', () => {
        it('should return new pipeline', done => {
            const buckets = ['example-bucket-1', 'example-bucket-2'];
            const pipeline = oplogPopulator._generateNewConnectorPipeline(buckets);
            assert.strictEqual(pipeline, JSON.stringify([
                {
                    $match: {
                        'ns.coll': {
                            $in: buckets,
                        }
                    }
                }
            ]));
            return done();
        });
    });

    describe('_listenToBucket', () => {
        it('Should update connector pipeline', done => {
            const updateStub = sinon.stub(oplogPopulator, 'updateConnectorPipeline');
            oplogPopulator._listenToBucket('example-bucket');
            assert(oplogPopulator._bucketsForConnector.has('example-bucket'));
            assert(updateStub.calledOnceWith('source-connector', ['example-bucket']));
            return done();
        });
    });

    describe('_stopListeningToBucket', () => {
        it('Should update connector pipeline', done => {
            const updateStub = sinon.stub(oplogPopulator, 'updateConnectorPipeline');
            oplogPopulator._bucketsForConnector.add('example-bucket');
            oplogPopulator._stopListeningToBucket('example-bucket');
            assert.strictEqual(oplogPopulator._bucketsForConnector.has('example-bucket'), false);
            assert(updateStub.calledOnceWith('source-connector', []));
            return done();
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
            it(`Should validate bucket if at least one extension active (${exts})`, done => {
                const valid = oplogPopulator._isBucketBackbeatEnabled(metadata);
                assert.strictEqual(valid, result);
                return done();
            });
        });
    });

    describe.skip('_handleChangeStreamChangeEvent', () => {
        // TODO: tests will be written in BB-164
        // as function will change
    });

    describe('_setMetastoreChangeStream ::', () =>  {
        it('Should create and listen to the metastore change stream', done => {
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
            return done();
        });
    });

    describe.skip('updateConnectorConfig', () => {
        // TODO: tests will be written in BB-164
        // as function will change
    });

    describe('_configureConnector', () => {
        it('should update a connector', async () => {
            const getConnectorsStub = sinon.stub(oplogPopulator._connectWrapper, 'getConnectors')
                .resolves(['source-connector']);
            const updateConfStub = sinon.stub(oplogPopulator._connectWrapper, 'updateConnectorConfig')
                .resolves(null);
            const createConnectorStub = sinon.stub(oplogPopulator._connectWrapper, 'createConnector')
                .resolves(null);
            await oplogPopulator._configureConnector({
                name: 'source-connector',
                config: {},
            })
            .then(() => {
                assert(getConnectorsStub.calledOnce);
                assert(updateConfStub.calledOnceWith('source-connector', {}));
                assert(createConnectorStub.notCalled);
            })
            .catch(err => assert.ifError(err));
        });

        it('should create a connector', async () => {
            const getConnectorsStub = sinon.stub(oplogPopulator._connectWrapper, 'getConnectors')
                .resolves([]);
            const updateConfStub = sinon.stub(oplogPopulator._connectWrapper, 'updateConnectorConfig')
                .resolves(null);
            const createConnectorStub = sinon.stub(oplogPopulator._connectWrapper, 'createConnector')
                .resolves(null);
            await oplogPopulator._configureConnector({
                name: 'source-connector',
                config: {},
            })
            .then(() => {
                assert(getConnectorsStub.calledOnce);
                assert(updateConfStub.notCalled);
                assert(createConnectorStub.calledWithMatch({
                    name: 'source-connector',
                    config: {},
                }));
            })
            .catch(err => assert.ifError(err));
        });

        it('should throw error if it fails to get connectors', async () => {
            const getConnectorsStub = sinon.stub(oplogPopulator._connectWrapper, 'getConnectors')
                .rejects(errors.InternalError);
            const updateConfStub = sinon.stub(oplogPopulator._connectWrapper, 'updateConnectorConfig')
                .resolves(null);
            const createConnectorStub = sinon.stub(oplogPopulator._connectWrapper, 'createConnector')
                .resolves(null);
            await oplogPopulator._configureConnector({
                name: 'source-connector',
                config: {},
            })
            .then(() => {
                assert(getConnectorsStub.calledOnce);
                assert(updateConfStub.notCalled);
                assert(createConnectorStub.notCalled);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('should throw error if it fails to update connector', async () => {
            const getConnectorsStub = sinon.stub(oplogPopulator._connectWrapper, 'getConnectors')
                .resolves(['source-connector']);
            const updateConfStub = sinon.stub(oplogPopulator._connectWrapper, 'updateConnectorConfig')
                .rejects(errors.InternalError);
            const createConnectorStub = sinon.stub(oplogPopulator._connectWrapper, 'createConnector')
                .resolves(null);
            await oplogPopulator._configureConnector({
                name: 'source-connector',
                config: {},
            })
            .then(() => {
                assert(getConnectorsStub.calledOnce);
                assert(updateConfStub.calledOnce);
                assert(createConnectorStub.notCalled);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('should throw error if it fails to create connector', async () => {
            const getConnectorsStub = sinon.stub(oplogPopulator._connectWrapper, 'getConnectors')
                .resolves([]);
            const updateConfStub = sinon.stub(oplogPopulator._connectWrapper, 'updateConnectorConfig')
                .resolves(null);
            const createConnectorStub = sinon.stub(oplogPopulator._connectWrapper, 'createConnector')
                .rejects(errors.InternalError);
            await oplogPopulator._configureConnector({
                name: 'source-connector',
                config: {},
            })
            .then(() => {
                assert(getConnectorsStub.calledOnce);
                assert(updateConfStub.notCalled);
                assert(createConnectorStub.calledOnce);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });

    describe('setup', () => {
        it('should setup the OplogPopulator', async () => {
            const setupMongoStub = sinon.stub(oplogPopulator, '_setupMongoClient')
                .resolves();
            const setupMetastore = sinon.stub(oplogPopulator, '_setMetastoreChangeStream')
                .resolves();
            const getbucketStub = sinon.stub(oplogPopulator, '_getBackbeatEnabledBuckets')
                .resolves([]);
            const getDefaultConfigStub = sinon.stub(oplogPopulator, '_getDefaultConnectorConfiguration')
                .returns(connectorConfig);
            const generatePipelineStub = sinon.stub(oplogPopulator, '_generateNewConnectorPipeline')
                .returns('[]');
            const configureConnectorsStub = sinon.stub(oplogPopulator, '_configureConnector')
                .resolves();
            await oplogPopulator.setup()
            .then(() => {
                assert(setupMongoStub.calledOnce);
                assert(getbucketStub.calledOnce);
                assert(getDefaultConfigStub.calledOnceWithMatch('source-connector'));
                assert(generatePipelineStub.calledOnceWithMatch([]));
                assert(configureConnectorsStub.calledOnceWithMatch({
                    name: 'source-connector',
                    config: connectorConfig,
                }));
                assert(setupMetastore.calledOnce);
            })
            .catch(err => assert.ifError(err));
        });

        it('should fail if it can\'t connect to mongo', async () => {
            const setupMongoStub = sinon.stub(oplogPopulator, '_setupMongoClient')
                .rejects(errors.InternalError);
            await oplogPopulator.setup()
            .then(() => {
                assert(setupMongoStub.calledOnce);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('should fail if it can\'t establish change stream', async () => {
            const setupMongoStub = sinon.stub(oplogPopulator, '_setupMongoClient')
                .resolves();
            const setupMetastore = sinon.stub(oplogPopulator, '_setMetastoreChangeStream')
                .rejects(errors.InternalError);
            await oplogPopulator.setup()
            .then(() => {
                assert(setupMongoStub.calledOnce);
                assert(setupMetastore.calledOnce);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('should fail if it can\'t get backbeat enabled buckets', async () => {
            const setupMongoStub = sinon.stub(oplogPopulator, '_setupMongoClient')
                .resolves();
            const setupMetastore = sinon.stub(oplogPopulator, '_setMetastoreChangeStream')
                .resolves();
            const getbucketStub = sinon.stub(oplogPopulator, '_getBackbeatEnabledBuckets')
                .rejects(errors.InternalError);
            await oplogPopulator.setup()
            .then(() => {
                assert(setupMongoStub.calledOnce);
                assert(setupMetastore.calledOnce);
                assert(getbucketStub.calledOnce);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });

        it('should fail if connector configuration fails', async () => {
            const setupMongoStub = sinon.stub(oplogPopulator, '_setupMongoClient')
                .resolves();
            const setupMetastore = sinon.stub(oplogPopulator, '_setMetastoreChangeStream')
                .resolves();
            const getbucketStub = sinon.stub(oplogPopulator, '_getBackbeatEnabledBuckets')
                .resolves([]);
            const getDefaultConfigStub = sinon.stub(oplogPopulator, '_getDefaultConnectorConfiguration')
                .returns(connectorConfig);
            const generatePipelineStub = sinon.stub(oplogPopulator, '_generateNewConnectorPipeline')
                .returns('[]');
            const configureConnectorsStub = sinon.stub(oplogPopulator, '_configureConnector')
                .rejects(errors.InternalError);
            await oplogPopulator.setup()
            .then(() => {
                assert(setupMongoStub.calledOnce);
                assert(setupMetastore.calledOnce);
                assert(getbucketStub.calledOnce);
                assert(getDefaultConfigStub.calledOnceWithMatch(oplogPopulatorConfig.connectors[0]));
                assert(generatePipelineStub.calledOnceWithMatch([]));
                assert(configureConnectorsStub.calledOnce);
            })
            .catch(err => assert.deepEqual(err, errors.InternalError));
        });
    });
});
