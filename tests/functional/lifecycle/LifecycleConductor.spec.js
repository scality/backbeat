'use strict'; // eslint-disable-line

const assert = require('assert');
const async = require('async');

const werelogs = require('werelogs');

const zookeeper = require('../../../lib/clients/zookeeper');
const BackbeatTestConsumer = require('../../utils/BackbeatTestConsumer');
const LifecycleConductor = require(
    '../../../extensions/lifecycle/conductor/LifecycleConductor');

const zkConfig = {
    zookeeper: {
        connectionString: '127.0.0.1:2181',
        autoCreateNamespace: true,
    },
};
const kafkaConfig = {
    hosts: '127.0.0.1:9092',
    backlogMetrics: {
        zkPath: '/test/lifecycle/kafka-backlog-metrics',
        intervalS: 1,
    },
};

const lcConfig = {
    zookeeperPath: '/test/lifecycle',
    bucketTasksTopic: 'backbeat-lifecycle-bucket-tasks-spec',
    objectTasksTopic: 'backbeat-lifecycle-object-tasks-spec',
    kafka: kafkaConfig,
    conductor: {
        cronRule: '*/5 * * * * *',
        backlogControl: {
            enabled: false,
        },
    },
    producer: {
        groupId: 'backbeat-lifecycle-producer-group-spec',
    },
    consumer: {
        groupId: 'backbeat-lifecycle-consumer-group-spec',
    },
    rules: {
        expiration: {
            enabled: true,
        },
    },
    auth: { type: 'account', account: 'lifecycle' },
};

const repConfig = {
    dataMoverTopic: 'backbeat-data-mover-spec',
};

const lcConductor = new LifecycleConductor(zkConfig.zookeeper,
                                           kafkaConfig, lcConfig, repConfig);

const TIMEOUT = 120000;
const CONSUMER_TIMEOUT = 60000;

werelogs.configure({ level: 'info', dump: 'error' });

describe('lifecycle conductor', function lifecycleConductor() {
    let zkClient;
    let consumer;

    this.timeout(TIMEOUT);

    before(done => {
        async.series([
            next => lcConductor.init(next),
            next => {
                zkClient = zookeeper.createClient(
                    zkConfig.zookeeper.connectionString,
                    zkConfig.zookeeper);
                zkClient.connect();
                zkClient.once('ready', next);
            },
            next => lcConductor.initZkPaths(next),
            next => {
                consumer = new BackbeatTestConsumer({
                    kafka: { hosts: kafkaConfig.hosts },
                    topic: lcConfig.bucketTasksTopic,
                    groupId: 'test-consumer-group',
                });
                consumer.on('ready', next);
            },
            next => {
                consumer.subscribe();
                // it seems the consumer needs some extra time to
                // start consuming the first messages
                setTimeout(next, 2000);
            },
        ], done);
    });

    after(done => {
        async.waterfall([
            next => zkClient.removeRecur(lcConfig.zookeeperPath, next),
            next => consumer.close(next),
            next => lcConductor.stop(next),
        ], done);
    });

    it('should populate queue from lifecycled bucket list ' +
    'in zookeeper', done => async.waterfall([
        next => async.each(
            ['owner1:uid1:bucket1', 'owner2:uid2:bucket2'],
            (bucket, done) => zkClient.create(
                `${lcConfig.zookeeperPath}/data/buckets/${bucket}`, done),
            next),
        next => {
            lcConductor.processBuckets();
            consumer.expectUnorderedMessages([
                {
                    value: {
                        action: 'processObjects',
                        target: { bucket: 'bucket1', owner: 'owner1' },
                        details: {},
                    },
                },
                {
                    value: {
                        action: 'processObjects',
                        target: { bucket: 'bucket2', owner: 'owner2' },
                        details: {},
                    },
                },
            ], CONSUMER_TIMEOUT, next);
        },
        next => async.each(
            ['owner3:uid3:bucket3', 'owner4:uid4:bucket4'],
            (bucket, done) => zkClient.create(
                `${lcConfig.zookeeperPath}/data/buckets/${bucket}`, done),
            next),
        next => {
            lcConductor.processBuckets();
            consumer.expectUnorderedMessages([
                {
                    value: {
                        action: 'processObjects',
                        target: { bucket: 'bucket1', owner: 'owner1' },
                        details: {},
                    },
                },
                {
                    value: {
                        action: 'processObjects',
                        target: { bucket: 'bucket2', owner: 'owner2' },
                        details: {},
                    },
                },
                {
                    value: {
                        action: 'processObjects',
                        target: { bucket: 'bucket3', owner: 'owner3' },
                        details: {},
                    },
                },
                {
                    value: {
                        action: 'processObjects',
                        target: { bucket: 'bucket4', owner: 'owner4' },
                        details: {},
                    },
                },
            ], CONSUMER_TIMEOUT, next);
        },
    ], err => {
        assert.ifError(err);
        done();
    }));
});
