const assert = require('assert');
const async = require('async');
const werelogs = require('werelogs');
const ZookeeperMock = require('zookeeper-mock');

const NotificationConfigManager
    = require('../../../extensions/notification/NotificationConfigManager');

const logger = new werelogs.Logger('NotificationConfigManager:test');
const zkConfigParentNode = 'config';
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
            next => zkClient.setData(node, Buffer.from(strVal), next),
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

describe('NotificationConfigManager multiple managers functional tests', () => {
    const zkClient = new ZookeeperMock();
    const params = {
        zkClient,
        parentNode: zkConfigParentNode,
        logger,
    };
    let configManager1 = null;
    let configManager2 = null;

    function checkCount() {
        const buckets1 = configManager1.getBucketsWithConfigs();
        const buckets2 = configManager2.getBucketsWithConfigs();
        assert.strictEqual(buckets1.length, buckets2.length);
    }

    function checkBucketConfig(bucket) {
        const existingConfig1 = configManager1.getConfig(bucket);
        const existingConfig2 = configManager2.getConfig(bucket);
        assert.deepStrictEqual(existingConfig1, existingConfig2);
    }

    beforeEach(done => {
        configManager1 = new NotificationConfigManager(params);
        configManager2 = new NotificationConfigManager(params);
        populateTestConfigs(zkClient, 5, () => {
            async.series([
                next => configManager1.init(next),
                next => configManager2.init(next),
            ], done);
        });
    });

    afterEach(() => {
        zkClient._resetState();
    });

    it('managers should have the same config values after init', done => {
        listBuckets(zkClient, (err, buckets) => {
            assert.ifError(err);
            buckets.forEach(bucket => {
                checkBucketConfig(bucket);
            });
            checkCount();
            done();
        });
    });

    it('managers should have the config if a new one is added', done => {
        const bucket = 'bucket100';
        const config = getTestConfigValue(bucket);
        const result = configManager1.setConfig(bucket, config);
        assert(result);
        setTimeout(() => {
            checkBucketConfig(bucket);
            checkCount();
            done();
        }, timeoutMs);
    });

    it('managers should have the same updated config value', done => {
        const bucket = 'bucket1';
        checkBucketConfig(bucket);
        const config = getTestConfigValue(`${bucket}-updated`);
        const result = configManager1.setConfig(bucket, config);
        assert(result);
        setTimeout(() => {
            checkBucketConfig(bucket);
            checkCount();
            done();
        }, timeoutMs);
    });

    it('managers should have the same cofig count after a config is removed',
        done => {
            const bucket = 'bucket1';
            const result = configManager1.removeConfig(bucket);
            assert(result);
            setTimeout(() => {
                checkBucketConfig(bucket);
                checkCount();
                done();
            }, timeoutMs);
        });

    it('managers should have the same config count if all configs are removed',
        done => {
            deleteTestConfigs(zkClient, err => {
                assert.ifError(err);
                setTimeout(() => {
                    checkCount();
                    done();
                }, timeoutMs);
            });
        });
});
