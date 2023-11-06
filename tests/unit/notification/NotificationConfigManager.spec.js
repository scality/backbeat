const assert = require('assert');
const werelogs = require('werelogs');
const sinon = require('sinon');
const events = require('events');
const MongoClient = require('mongodb').MongoClient;

const ChangeStream = require('../../../lib/wrappers/ChangeStream');
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
            const mongoCommandStub = sinon.stub().returns({
                version: '4.3.17',
            });
            const getDbStub = sinon.stub().returns({
                collection: getCollectionStub,
                command: mongoCommandStub,
            });
            const mongoConnectStub = sinon.stub(MongoClient, 'connect').callsArgWith(2, null, {
                db: getDbStub,
            });
            manager._setupMongoClient(err => {
                assert.ifError(err);
                assert(mongoConnectStub.calledOnce);
                assert(getDbStub.calledOnce);
                assert(getCollectionStub.calledOnce);
                assert(mongoCommandStub.calledOnceWith({
                    buildInfo: 1,
                }));
                assert.equal(manager._mongoVersion, '4.3.17');
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
        it('Should remove entry from cache', () => {
            const changeStreamEvent = {
                _id: 'resumeToken',
                operationType: 'delete',
                documentKey: {
                    _id: 'example-bucket-1',
                },
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

        it('Should replace entry from cache', () => {
            const changeStreamEvent = {
                _id: 'resumeToken',
                operationType: 'replace',
                documentKey: {
                    _id: 'example-bucket-1',
                },
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

        it('Should do nothing when config not in cache', () => {
            const changeStreamEvent = {
                _id: 'resumeToken',
                operationType: 'delete',
                documentKey: {
                    _id: 'example-bucket-2',
                },
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

        it('Should do nothing when operation is not supported', () => {
            const changeStreamEvent = {
                _id: 'resumeToken',
                operationType: 'insert',
                documentKey: {
                    _id: 'example-bucket-1',
                },
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
         it('should use resumeAfter with mongo 3.6', () => {
            const manager = new NotificationConfigManager(params);
            manager._mongoVersion = '3.6.2';
            manager._metastore = {
                watch: sinon.stub().returns(new events.EventEmitter()),
            };
            manager._setMetastoreChangeStream();
            assert(manager._metastoreChangeStream instanceof ChangeStream);
            assert.equal(manager._metastoreChangeStream._resumeField, 'resumeAfter');
        });

        it('should use resumeAfter with mongo 4.0', () => {
            const manager = new NotificationConfigManager(params);
            manager._mongoVersion = '4.0.7';
            manager._metastore = {
                watch: sinon.stub().returns(new events.EventEmitter()),
            };
            manager._setMetastoreChangeStream();
            assert(manager._metastoreChangeStream instanceof ChangeStream);
            assert.equal(manager._metastoreChangeStream._resumeField, 'resumeAfter');
        });

        it('should use startAfter with mongo 4.2', () => {
            const manager = new NotificationConfigManager(params);
            manager._mongoVersion = '4.2.3';
            manager._metastore = {
                watch: sinon.stub().returns(new events.EventEmitter()),
            };
            manager._setMetastoreChangeStream();
            assert(manager._metastoreChangeStream instanceof ChangeStream);
            assert.equal(manager._metastoreChangeStream._resumeField, 'startAfter');
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
