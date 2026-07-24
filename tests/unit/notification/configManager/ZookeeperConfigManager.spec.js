const assert = require('assert');
const async = require('async');
const werelogs = require('werelogs');
const ZookeeperMock = require('zookeeper-mock');
const ZookeeperManager = require('../../../../lib/clients/ZookeeperManager');
const sinon = require('sinon');

const ZookeeperConfigManager
    = require('../../../../extensions/notification/configManager/ZookeeperConfigManager');
const constants
    = require('../../../../extensions/notification/constants');

const logger = new werelogs.Logger('ZookeeperConfigManager:test');
const { zkConfigParentNode } = constants;
const concurrency = 10;
const bucketPrefix = 'bucket';
const timeoutMs = 100;

function getTestConfigValue(bucket) {
    const data = {
        bucket,
        notificationConfiguration: {
            queueConfig: [
                {
                    events: ['s3:ObjectCreated:Put'],
                    queueArn: 'arn:scality:bucketnotif:::destination1',
                    filterRules: [],
                    id: `${zkConfigParentNode}-${bucket}`,
                },
            ],
        },
    };
    return data;
}

function populateTestConfigs(zkClient, numberOfConfigs, cb) {
    async.timesLimit(numberOfConfigs, concurrency, (n, done) => {
        const bucket = `${bucketPrefix}${n + 1}`;
        const val = getTestConfigValue(bucket);
        const strVal = JSON.stringify(val);
        const node = `/${zkConfigParentNode}/${bucket}`;
        async.series([
            next => zkClient.mkdirp(node, next),
            next => zkClient.setData(node, strVal, next),
        ], done);
    }, cb);
}

function listBuckets(zkClient, cb) {
    const node = `/${zkConfigParentNode}`;
    zkClient.getChildren(node, cb);
}

function deleteTestConfigs(zkClient, cb) {
    const node = `/${zkConfigParentNode}`;
    listBuckets(zkClient, (error, children) => {
        if (error) {
            assert.ifError(error);
            cb(error);
        } else {
            async.eachLimit(children, concurrency, (child, next) => {
                const childNode = `${node}/${child}`;
                zkClient.remove(childNode, next);
            }, cb);
        }
    });
}

function checkCount(zkClient, manager, cb) {
    listBuckets(zkClient, (err, buckets) => {
        assert.ifError(err);
        const zkConfigCount = buckets.length;
        const managerConfigs = [...manager._configs.keys()];
        assert.strictEqual(zkConfigCount, managerConfigs.length);
        cb();
    });
}

function managerInit(manager, cb) {
    manager.setup(err => {
        assert.ifError(err);
        cb();
    });
}

function checkParentConfigZkNode(manager, cb) {
    const zkPath = `/${zkConfigParentNode}`;
    manager._checkNodeExists(zkPath, (err, exists) => {
        assert.ifError(err);
        assert(exists);
        cb();
    });
}

describe('ZookeeperConfigManager', () => {
    const zk = new ZookeeperMock({ doLog: false });
    const zkClient = zk.createClient();
    const params = {
        zkClient,
        zkConcurrency: 10,
        logger,
    };

    describe('Constructor', () => {
        function checkConfigParentNodeStub(cb) {
            return cb(new Error('error checking config parent node'));
        }

        after(() => {
            sinon.restore();
            zk._resetState();
        });

        it('constructor and setup checks', done => {
            assert.throws(() => new ZookeeperConfigManager());
            assert.throws(() => new ZookeeperConfigManager({}));
            assert.throws(() => new ZookeeperConfigManager({
                zkClient: null,
                logger: null,
            }));
            const manager = new ZookeeperConfigManager(params);
            assert(manager instanceof ZookeeperConfigManager);
            async.series([
                next => managerInit(manager, next),
                next => checkParentConfigZkNode(manager, next),
            ], done);
        });

        it('should return error if checkConfigParentNode fails', done => {
            const manager = new ZookeeperConfigManager(params);
            sinon.stub(manager, '_checkConfigurationParentNode')
                .callsFake(checkConfigParentNodeStub);
            manager.setup(err => {
                assert(err);
                return done();
            });
        });
    });

    describe('Operations', () => {
        beforeEach(done => populateTestConfigs(zkClient, 5, done));

        afterEach(() => {
            zk._resetState();
        });

        it('should get bucket notification configuration', () => {
            const manager = new ZookeeperConfigManager(params);
            managerInit(manager, () => {
                const bucket = 'bucket1';
                const result = manager.getConfig(bucket);
                assert.strictEqual(result.bucket, bucket);
            });
        });

        it('should return undefined for a non-existing bucket', () => {
            const manager = new ZookeeperConfigManager(params);
            managerInit(manager, () => {
                const bucket = 'bucket100';
                const result = manager.getConfig(bucket);
                assert.strictEqual(result, undefined);
            });
        });

        it('should add bucket notification configuration', done => {
            const manager = new ZookeeperConfigManager(params);
            managerInit(manager, () => {
                const bucket = 'bucket100';
                const config = getTestConfigValue(bucket);
                const result = manager.setConfig(bucket, config);
                assert(result);
                setTimeout(() => {
                    checkCount(zkClient, manager, done);
                }, timeoutMs);
            });
        });

        it('should update bucket notification configuration', done => {
            const manager = new ZookeeperConfigManager(params);
            managerInit(manager, () => {
                const bucket = 'bucket1';
                const config = getTestConfigValue(`${bucket}-updated`);
                const result = manager.setConfig(bucket, config);
                assert(result);
                setTimeout(() => {
                    const updatedConfig = manager.getConfig(bucket);
                    assert.strictEqual(updatedConfig.bucket,
                        `${bucket}-updated`);
                    checkCount(zkClient, manager, done);
                }, timeoutMs);
            });
        });

        it('should remove bucket notification configuration', done => {
            const manager = new ZookeeperConfigManager(params);
            managerInit(manager, () => {
                const bucket = 'bucket1';
                let result = manager.removeConfig(bucket);
                assert(result);
                setTimeout(() => {
                    result = manager.getConfig(bucket);
                    assert.strictEqual(result, undefined);
                    checkCount(zkClient, manager, done);
                }, timeoutMs);
            });
        });

        it('config count should be zero when all configs are removed', done => {
            const manager = new ZookeeperConfigManager(params);
            async.series([
                next => managerInit(manager, next),
                next => deleteTestConfigs(zkClient, next),
                next => setTimeout(next, timeoutMs),
                next => checkCount(zkClient, manager, next),
            ], done);
        });
    });

    describe('_setBucketNotifConfig resilience', () => {
        afterEach(() => {
            sinon.restore();
            zk._resetState();
        });

        it('should create the node and persist the config when it does not exist yet', done => {
            const manager = new ZookeeperConfigManager(params);
            const bucket = 'freshBucket';
            const config = getTestConfigValue(bucket);
            const node = `/${zkConfigParentNode}/${bucket}`;
            async.series([
                next => managerInit(manager, next),
                next => manager._setBucketNotifConfig(
                    bucket, JSON.stringify(config), err => {
                        assert.ifError(err);
                        return next();
                    }),
                next => zkClient.getData(node, undefined, (err, data) => {
                    assert.ifError(err);
                    assert.strictEqual(
                        JSON.parse(data.toString()).bucket, bucket);
                    return next();
                }),
            ], done);
        });

        it('should retry the config write on a transient failure', done => {
            const manager = new ZookeeperConfigManager(params);
            const bucket = 'retryBucket';
            const config = getTestConfigValue(bucket);
            const node = `/${zkConfigParentNode}/${bucket}`;
            managerInit(manager, () => {
                const transient = new Error('transient zookeeper error');
                const stub = sinon.stub(manager, '_setBucketNotifConfig');
                stub.onCall(0).callsFake((b, d, cb) => cb(transient));
                stub.onCall(1).callsFake((b, d, cb) => cb(transient));
                stub.callThrough();
                // setConfig triggers the listener retry
                manager.setConfig(bucket, config);
                setTimeout(() => {
                    assert.strictEqual(stub.callCount, 3);
                    zkClient.getData(node, undefined, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(
                            JSON.parse(data.toString()).bucket, bucket);
                        return done();
                    });
                }, 2000);
            });
        });
    });

    describe('setup', () => {
        it('should setup zookeeper client when it\'s not provided', done => {
            const manager = new ZookeeperConfigManager({
                zkPath: '/notification',
                zkConfig: {
                    connectionString: '127.0.0.1:2181',
                    autoCreateNamespace: false,
                },
                zkConcurrency: 10,
                logger,
            });
            sinon.stub(manager, '_checkConfigurationParentNode').yields(null, null);
            sinon.stub(manager, '_listBucketsWithConfig').yields(null, null);
            sinon.stub(manager, '_updateLocalStore').yields(null);
            const interval = setInterval(() => {
                if (manager._zkClient) {
                    manager._zkClient.emit('ready');
                    clearInterval(interval);
                }
            }, 30);
            manager.setup(err => {
                assert.ifError(err);
                assert(manager._zkClient instanceof ZookeeperManager);
                done();
            });
        });

        it('should not create zookeeper client when it\'s provided', done => {
            const manager = new ZookeeperConfigManager(params);
            sinon.stub(manager, '_checkConfigurationParentNode').yields(null, null);
            sinon.stub(manager, '_listBucketsWithConfig').yields(null, null);
            sinon.stub(manager, '_updateLocalStore').yields(null);
            manager.setup(err => {
                assert.ifError(err);
                assert(!(manager._zkClient instanceof ZookeeperManager));
                done();
            });
        });
    });
});
