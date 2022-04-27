const assert = require('assert');
const werelogs = require('werelogs');
const sinon = require('sinon');
const events = require('events');
const MongoClient = require('mongodb').MongoClient;

const NotificationConfigManager
    = require('../../../extensions/notification/NotificationConfigManager');
const { errors } = require('arsenal');
const mongoConfig
    = require('../../config.json').queuePopulator.mongo;

const logger = new werelogs.Logger('NotificationConfigManager:test');

const notificationConfiguration = {
    queueConfig: [
        {
            events: ['s3:ObjectCreated:Put'],
            queueArn: 'arn:scality:bucketnotif:::destination1',
            filterRules: [],
        },
    ],
};

const notificationConfigurationVariant = {
    queueConfig: [
        {
            events: ['s3:ObjectCreated:*'],
            queueArn: 'arn:scality:bucketnotif:::destination2',
            filterRules: [],
        },
    ],
};

describe('NotificationConfigManager ::', () => {
    const params = {
        mongoConfig,
        logger,
    };

    afterEach(() => {
        sinon.restore();
    });

    describe('Constructor & setup ::', () => {
        it('Constructor should validate params', done => {
            assert.throws(() => new NotificationConfigManager());
            assert.throws(() => new NotificationConfigManager({}));
            assert.throws(() => new NotificationConfigManager({
                mongoConfig: null,
                logger: null,
            }));
            const manager = new NotificationConfigManager(params);
            assert(manager instanceof NotificationConfigManager);
            return done();
        });

        it('Setup should initialize the mongoClient and the change stream', done => {
            const manager = new NotificationConfigManager(params);
            const setMongoStub = sinon.stub(manager, '_setupMongoClient').callsArg(0);
            const setChangeStreamStub = sinon.stub(manager, '_setMetastoreChangeStream').returns();
            manager.setup(err => {
                assert.ifError(err);
                assert(setMongoStub.calledOnce);
                assert(setChangeStreamStub.calledOnce);
                // cache should initially be empty
                assert.strictEqual(manager._cachedConfigs.count(), 0);
                return done();
            });
        });

        it('Setup should fail when mongo setup fails', done => {
            const manager = new NotificationConfigManager(params);
            const setMongoStub = sinon.stub(manager, '_setupMongoClient').callsArgWith(0,
                errors.InternalError);
            const setChangeStreamStub = sinon.stub(manager, '_setMetastoreChangeStream');
            manager.setup(err => {
                assert.deepEqual(err, errors.InternalError);
                assert(setMongoStub.calledOnce);
                assert(setChangeStreamStub.notCalled);
                return done();
            });
        });

        it('Setup should fail when changeStream setup fails', done => {
            const manager = new NotificationConfigManager(params);
            const setMongoStub = sinon.stub(manager, '_setupMongoClient').callsArg(0);
            const setChangeStreamStub = sinon.stub(manager, '_setMetastoreChangeStream').throws(
                errors.InternalError);
            manager.setup(err => {
                assert.deepEqual(err, errors.InternalError);
                assert(setMongoStub.calledOnce);
                assert(setChangeStreamStub.calledOnce);
                return done();
            });
        });
    });

    describe('_setupMongoClient ::', () => {
        it('Should setup the mongo client and get metastore collection', () => {
            const manager = new NotificationConfigManager(params);
            const getCollectionStub = sinon.stub();
            const getDbStub = sinon.stub().returns({
                collection: getCollectionStub,
                });
            const mongoConnectStub = sinon.stub(MongoClient, 'connect').callsArgWith(2, null, {
                db: getDbStub,
            });
            manager._setupMongoClient(err => {
                assert.ifError(err);
                assert(mongoConnectStub.calledOnce);
                assert(getDbStub.calledOnce);
                assert(getCollectionStub.calledOnce);
            });
        });

        it('Should fail when mongo client setup fails', () => {
            const manager = new NotificationConfigManager(params);
            sinon.stub(MongoClient, 'connect').callsArgWith(2,
                errors.InternalError, null);
            manager._setupMongoClient(err => {
                assert.deepEqual(err, errors.InternalError);
            });
        });

        it('Should fail when when getting the metadata db', () => {
            const manager = new NotificationConfigManager(params);
            const getDbStub = sinon.stub().throws(errors.InternalError);
            sinon.stub(MongoClient, 'connect').callsArgWith(2, null, {
                db: getDbStub,
            });
            manager._setupMongoClient(err => {
                assert.deepEqual(err, errors.InternalError);
            });
        });

        it('Should fail when mongo client fails to get metastore', () => {
            const manager = new NotificationConfigManager(params);
            const getCollectionStub = sinon.stub().throws(errors.InternalError);
            const getDbStub = sinon.stub().returns({
                collection: getCollectionStub,
                });
            sinon.stub(MongoClient, 'connect').callsArgWith(2, null, {
                db: getDbStub,
            });
            manager._setupMongoClient(err => {
                assert.deepEqual(err, errors.InternalError);
            });
        });
    });

    describe('_handleChangeStreamChangeEvent ::', () => {
        it('Should remove entry from cache', async () => {
            const changeStreamEvent = {
                _id: 'resumeToken',
                operationType: 'delete',
                fullDocument: {
                    _id: 'example-bucket-1',
                    value: {
                        notificationConfiguration,
                    }
                }
            };
            const manager = new NotificationConfigManager(params);
            // populating cache
            manager._cachedConfigs.add('example-bucket-1', notificationConfiguration);
            assert.strictEqual(manager._cachedConfigs.count(), 1);
            // handling change stream event
            manager._handleChangeStreamChangeEvent(changeStreamEvent);
            // should delete bucket config from cache
            assert.strictEqual(manager._cachedConfigs.get('example-bucket-1'),
                undefined);
            assert.strictEqual(manager._cachedConfigs.count(), 0);
        });

        it('Should replace entry from cache', async () => {
            const changeStreamEvent = {
                _id: 'resumeToken',
                operationType: 'replace',
                fullDocument: {
                    _id: 'example-bucket-1',
                    value: {
                        notificationConfiguration:
                            notificationConfigurationVariant,
                    }
                }
            };
            const manager = new NotificationConfigManager(params);
            // populating cache
            manager._cachedConfigs.add('example-bucket-1', notificationConfiguration);
            assert.strictEqual(manager._cachedConfigs.count(), 1);
            // handling change stream event
            manager._handleChangeStreamChangeEvent(changeStreamEvent);
            // should update bucket config in cache
            assert.deepEqual(manager._cachedConfigs.get('example-bucket-1'),
                notificationConfigurationVariant);
            assert.strictEqual(manager._cachedConfigs.count(), 1);
            // same thing should happen with "update" event
            changeStreamEvent.operationType = 'update';
            // reseting config to default one
            changeStreamEvent.fullDocument.value.notificationConfiguration =
                notificationConfiguration;
            // emiting the new "update" event
            manager._handleChangeStreamChangeEvent(changeStreamEvent);
            // cached config must be updated
            assert.deepEqual(manager._cachedConfigs.get('example-bucket-1'),
                notificationConfiguration);
            assert.strictEqual(manager._cachedConfigs.count(), 1);
        });

        it('Should do nothing when config not in cache', async () => {
            const changeStreamEvent = {
                _id: 'resumeToken',
                operationType: 'delete',
                fullDocument: {
                    _id: 'example-bucket-2',
                    value: {
                        notificationConfiguration:
                            notificationConfigurationVariant,
                    }
                }
            };
            const manager = new NotificationConfigManager(params);
            // populating cache
            manager._cachedConfigs.add('example-bucket-1', notificationConfiguration);
            assert.strictEqual(manager._cachedConfigs.count(), 1);
            // handling change stream event
            manager._handleChangeStreamChangeEvent(changeStreamEvent);
            // cache should not change
            assert.deepEqual(manager._cachedConfigs.get('example-bucket-1'),
                notificationConfiguration);
            assert.strictEqual(manager._cachedConfigs.count(), 1);
        });

        it('Should do nothing when operation is not supported', async () => {
            const changeStreamEvent = {
                _id: 'resumeToken',
                operationType: 'insert',
                fullDocument: {
                    _id: 'example-bucket-2',
                    value: {
                        notificationConfiguration:
                            notificationConfigurationVariant,
                    }
                }
            };
            const manager = new NotificationConfigManager(params);
            // populating cache
            manager._cachedConfigs.add('example-bucket-1', notificationConfiguration);
            assert.strictEqual(manager._cachedConfigs.count(), 1);
            assert(manager._cachedConfigs.get('example-bucket-1'));
            // handling change stream event
            manager._handleChangeStreamChangeEvent(changeStreamEvent);
            // cache should not change
            assert.deepEqual(manager._cachedConfigs.get('example-bucket-1'),
                notificationConfiguration);
            assert.strictEqual(manager._cachedConfigs.count(), 1);
        });
    });

    describe('_setMetastoreChangeStream ::', () =>  {
        it('Should create and listen to the metastore change stream', done => {
            const manager = new NotificationConfigManager(params);
            manager._metastore = {
                watch: () => new events.EventEmitter(),
                findOne: () => (
                    {
                        value: {
                            notificationConfiguration,
                        }
                    }),
            };
            try {
                manager._setMetastoreChangeStream();
                assert(manager._metastoreChangeStream instanceof events.EventEmitter);
                const changeHandlers = events.getEventListeners(manager._metastoreChangeStream, 'change');
                const errorHandlers = events.getEventListeners(manager._metastoreChangeStream, 'error');
                assert.strictEqual(changeHandlers.length, 1);
                assert.strictEqual(errorHandlers.length, 1);
            } catch (error) {
                assert.ifError(error);
            }
            return done();
        });

        it('Should fail if it fails to create change stream', done => {
            const manager = new NotificationConfigManager(params);
            manager._metastore = {
                watch: sinon.stub().throws(errors.InternalError),
                findOne: () => (
                    {
                        value: {
                            notificationConfiguration,
                        }
                    }),
            };
            assert.throws(() => manager._setMetastoreChangeStream());
            return done();
        });
    });

    describe('_handleChangeStreamErrorEvent ::', () =>  {
        it('Should reset change steam on error without closing it (already closed)', async () => {
            const manager = new NotificationConfigManager(params);
            const removeEventListenerStub = sinon.stub();
            const closeStub = sinon.stub();
            const setMetastoreChangeStreamStub = sinon.stub(manager, '_setMetastoreChangeStream');
            manager._metastoreChangeStream = {
                removeEventListener: removeEventListenerStub,
                isClosed: () => true,
                close: closeStub,
            };
            manager._handleChangeStreamErrorEvent(err => {
                assert.ifError(err);
                assert(setMetastoreChangeStreamStub.calledOnce);
                assert(closeStub.notCalled);
                assert(removeEventListenerStub.calledWith('change',
                    manager._handleChangeStreamChangeEvent.bind(manager)));
                assert(removeEventListenerStub.calledWith('error',
                    manager._handleChangeStreamErrorEvent.bind(manager)));
            });
        });

        it('Should close then reset the change steam on error', async () => {
            const manager = new NotificationConfigManager(params);
            const removeEventListenerStub = sinon.stub();
            const closeStub = sinon.stub();
            const setMetastoreChangeStreamStub = sinon.stub(manager, '_setMetastoreChangeStream');
            manager._metastoreChangeStream = {
                removeEventListener: removeEventListenerStub,
                isClosed: () => false,
                close: closeStub,
            };
            manager._handleChangeStreamErrorEvent(err => {
                assert.ifError(err);
                assert(setMetastoreChangeStreamStub.calledOnce);
                assert(closeStub.called);
                assert(removeEventListenerStub.calledWith('change',
                    manager._handleChangeStreamChangeEvent.bind(manager)));
                assert(removeEventListenerStub.calledWith('error',
                    manager._handleChangeStreamErrorEvent.bind(manager)));
            });
        });
    });

    describe('getConfig ::', () =>  {
        it('Should return notification configuration of bucket', async () => {
            const manager = new NotificationConfigManager(params);
            manager._metastore = {
                findOne: () => (
                    {
                        value: {
                            notificationConfiguration,
                        }
                    }),
            };
            assert.strictEqual(manager._cachedConfigs.count(), 0);
            const config = await manager.getConfig('example-bucket-1');
            assert.deepEqual(config, notificationConfiguration);
            // should also cache config
            assert.strictEqual(manager._cachedConfigs.count(), 1);
            assert.deepEqual(manager._cachedConfigs.get('example-bucket-1'),
                notificationConfiguration);
        });

        it('Should return undefined when bucket doesn\'t have notification configuration', async () => {
            const manager = new NotificationConfigManager(params);
            manager._metastore = {
                findOne: () => ({ value: {} }),
            };
            const config = await manager.getConfig('example-bucket-1');
            assert.strictEqual(config, undefined);
        });

        it('Should return undefined when mongo findOne fails', async () => {
            const manager = new NotificationConfigManager(params);
            manager._metastore = {
                findOne: sinon.stub().throws(errors.InternalError),
            };
            const config = await manager.getConfig('example-bucket-1');
            assert.strictEqual(config, undefined);
        });
    });
});
