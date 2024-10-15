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

        it('should call callback when the promise resolves', done => {
            const params = {
                zkClient: {},
                logger,
            };
            const manager = new NotificationConfigManager(params);

            sinon.stub(manager._configManagerBackend, 'getConfig').resolves(bucketConfig);

            manager.getConfig('bucket1', (err, result) => {
                assert.ifError(err);
                assert.strictEqual(result, bucketConfig);
                done();
            });
        });
    });

    describe('setConfig', () => {
        it('should call setConfig of the backend', () => {
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
            manager._configManagerBackend.setConfig = sinon.stub().returns(false);
            manager.setConfig('bucket1', bucketConfig);
            assert(manager._configManagerBackend.setConfig.calledOnce);
        });
    });

    describe('removeConfig', () => {
        it('should call removeConfig of the backend', () => {
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
            manager._configManagerBackend.removeConfig = sinon.stub().returns(false);
            manager.removeConfig('bucket1');
            assert(manager._configManagerBackend.removeConfig.calledOnce);
        });
    });

    describe('setup', () => {
        it('should call the setup method of the backend', done => {
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
