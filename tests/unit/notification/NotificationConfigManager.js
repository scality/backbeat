const assert = require('assert');
const werelogs = require('werelogs');
const sinon = require('sinon');

const NotificationConfigManager = require('../../../extensions/notification/NotificationConfigManager');
const MongoConfigManager = require('../../../extensions/notification/configManager/MongoConfigManager');
const ZookeeperConfigManager = require('../../../extensions/notification/configManager/ZookeeperConfigManager');

const logger = new werelogs.Logger('NotificationConfigManager:test');

const bucketConfig = {
    bucket: 'bucket1',
    notificationConfiguration: {
        queueConfig: [
            {
                events: ['s3:ObjectCreated:Put'],
                queueArn: 'arn:scality:bucketnotif:::destination1',
                filterRules: [],
            },
        ],
    },
};

describe('NotificationConfigManager', () => {
    describe('constructor', () => {
        it('should use the mongodb backend', () => {
            const params = {
                mongoConfig: {
                    database: 'eb1e786d-da1e-3fc5-83d2-46f083ab9764',
                    readPreference: 'primary',
                    replicaSetHosts: 'localhost:27017',
                    shardCollections: true,
                    writeConcern: 'majority'
                },
                bucketMetastore: '__metastore',
                logger,
            };

            const manager = new NotificationConfigManager(params);
            assert(manager._configManagerBackend instanceof MongoConfigManager);
            assert.strictEqual(manager._usesZookeeperBackend, false);
        });

        it('should use the zookeeper backend', () => {
            const params = {
                zkClient: {},
                logger,
            };

            const manager = new NotificationConfigManager(params);
            assert(manager._configManagerBackend instanceof ZookeeperConfigManager);
            assert.strictEqual(manager._usesZookeeperBackend, true);
        });
    });

    describe('getConfig', () => {
        it('should return the configuration', () => {
            const params = {
                zkClient: {},
                logger,
            };
            const manager = new NotificationConfigManager(params);

            sinon.stub(manager._configManagerBackend, 'getConfig').returns(bucketConfig);

            const result = manager.getConfig('bucket1');
            assert.strictEqual(result, bucketConfig);
        });

        it('should return the configuration with a callback', done => {
            const params = {
                zkClient: {},
                logger,
            };
            const manager = new NotificationConfigManager(params);

            sinon.stub(manager._configManagerBackend, 'getConfig').returns(bucketConfig);

            manager.getConfig('bucket1', (err, result) => {
                assert.ifError(err);
                assert.strictEqual(result, bucketConfig);
                done();
            });
        });

        it('should return the configuration in a promise', done => {
            const params = {
                zkClient: {},
                logger,
            };
            const manager = new NotificationConfigManager(params);

            sinon.stub(manager._configManagerBackend, 'getConfig').resolves(bucketConfig);

            manager.getConfig('bucket1')
                .then(result => {
                    assert.strictEqual(result, bucketConfig);
                    done();
                })
                .catch(err => assert.ifError(err));
        });
    });

    describe('setConfig', () => {
        it('should do nothing if using a mongodb backend', () => {
            const params = {
                mongoConfig: {
                    database: 'eb1e786d-da1e-3fc5-83d2-46f083ab9764',
                    readPreference: 'primary',
                    replicaSetHosts: 'localhost:27017',
                    shardCollections: true,
                    writeConcern: 'majority'
                },
                bucketMetastore: '__metastore',
                logger,
            };

            const manager = new NotificationConfigManager(params);
            manager._configManagerBackend.setConfig = sinon.stub().returns(true);
            manager.setConfig('bucket1', bucketConfig);
            assert(manager._configManagerBackend.setConfig.notCalled);
        });

        it('should call the zookeeper backend', () => {
            const params = {
                zkClient: {},
                logger,
            };

            const manager = new NotificationConfigManager(params);
            const stub = sinon.stub(manager._configManagerBackend, 'setConfig').returns(true);
            manager.setConfig('bucket1', bucketConfig);
            assert(stub.calledOnce);
        });
    });

    describe('removeConfig', () => {
        it('should do nothing if using a mongodb backend', () => {
            const params = {
                mongoConfig: {
                    database: 'eb1e786d-da1e-3fc5-83d2-46f083ab9764',
                    readPreference: 'primary',
                    replicaSetHosts: 'localhost:27017',
                    shardCollections: true,
                    writeConcern: 'majority'
                },
                bucketMetastore: '__metastore',
                logger,
            };

            const manager = new NotificationConfigManager(params);
            manager._configManagerBackend.removeConfig = sinon.stub().returns(true);
            manager.setConfig('bucket1');
            assert(manager._configManagerBackend.removeConfig.notCalled);
        });

        it('should call the zookeeper backend', () => {
            const params = {
                zkClient: {},
                logger,
            };

            const manager = new NotificationConfigManager(params);
            const stub = sinon.stub(manager._configManagerBackend, 'removeConfig').returns(true);
            manager.removeConfig('bucket1');
            assert(stub.calledOnce);
        });
    });

    describe('setup', () => {
        it('should call the setup method', done => {
            const params = {
                zkClient: {},
                logger,
            };

            const manager = new NotificationConfigManager(params);
            const stub = sinon.stub(manager._configManagerBackend, 'setup').yields();

            manager.setup(err => {
                assert.ifError(err);
                assert(stub.calledOnce);
                done();
            });
        });
    });
});
