const assert = require('assert');
const async = require('async');
const werelogs = require('werelogs');
const ZookeeperMock = require('zookeeper-mock');

const NotificationConfigManager
    = require('../../../extensions/notification/NotificationConfigManager');
const constants
    = require('../../../extensions/notification/constants');

const logger = new werelogs.Logger('NotificationConfigManager:test');
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
        const managerConfigs = manager.getBucketsWithConfigs();
        assert.strictEqual(zkConfigCount, managerConfigs.length);
        cb();
    });
}

function managerInit(manager, cb) {
    manager.init(err => {
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

describe('NotificationConfigManager', () => {
    const zkClient = new ZookeeperMock();
    const params = {
        zkClient,
        parentNode: zkConfigParentNode,
        logger,
    };

    describe('Constructor', () => {
        after(() => {
            zkClient._resetState();
        });

        it('constructor and init checks', done => {
            assert.throws(() => new NotificationConfigManager());
            assert.throws(() => new NotificationConfigManager({}));
            assert.throws(() => new NotificationConfigManager({
                zkClient: null,
                logger: null,
            }));
            const manager = new NotificationConfigManager(params);
            assert(manager instanceof NotificationConfigManager);
            async.series([
                next => managerInit(manager, next),
                next => checkParentConfigZkNode(manager, next),
            ], done);
        });
    });

    describe('Operations', () => {
        beforeEach(done => populateTestConfigs(zkClient, 5, done));

        afterEach(() => {
            zkClient._resetState();
        });

        it('should get bucket notification configuration', () => {
            const manager = new NotificationConfigManager(params);
            managerInit(manager, () => {
                const bucket = 'bucket1';
                const result = manager.getConfig(bucket);
                assert.strictEqual(result.bucket, bucket);
            });
        });

        it('should return undefined for a non-existing bucket', () => {
            const manager = new NotificationConfigManager(params);
            managerInit(manager, () => {
                const bucket = 'bucket100';
                const result = manager.getConfig(bucket);
                assert.strictEqual(result, undefined);
            });
        });

        it('should add bucket notification configuration', done => {
            const manager = new NotificationConfigManager(params);
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
            const manager = new NotificationConfigManager(params);
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
            const manager = new NotificationConfigManager(params);
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
            const manager = new NotificationConfigManager(params);
            async.series([
                next => managerInit(manager, next),
                next => deleteTestConfigs(zkClient, next),
                next => setTimeout(next, timeoutMs),
                next => checkCount(zkClient, manager, next),
            ], done);
        });
    });
});
