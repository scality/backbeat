'use strict'; // eslint-disable-line

const assert = require('assert');
const async = require('async');

const werelogs = require('werelogs');

const zookeeper = require('../../../lib/clients/zookeeper');
const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatTestConsumer = require('../../utils/BackbeatTestConsumer');
const KafkaBacklogMetrics = require('../../../lib/KafkaBacklogMetrics');

const zkConfig = {
    connectionString: '127.0.0.1:2181',
    autoCreateNamespace: true,
};
const kafkaConfig = {
    hosts: '127.0.0.1:9092',
    backlogMetrics: {
        zkPath: '/test/kafka-backlog-metrics',
        intervalS: 1,
    },
};

const TOPIC = 'kafka-backlog-metrics-test-topic';
const GROUP_ID = 'kafka-backlog-metrics-test-consumer-group';

const TIMEOUT = 120000;
const CONSUMER_TIMEOUT = 60000;

werelogs.configure({ level: 'info', dump: 'error' });

describe('KafkaBacklogMetrics class', function kafkaBacklogMetrics() {
    let zkPath;
    let zkClient;
    let kafkaBacklogMetrics;
    let consumer;
    let producer;

    this.timeout(TIMEOUT);

    before(done => {
        zkPath = kafkaConfig.backlogMetrics.zkPath;
        async.series([
            next => {
                kafkaBacklogMetrics = new KafkaBacklogMetrics(
                    zkConfig.connectionString, kafkaConfig.backlogMetrics);
                kafkaBacklogMetrics.init();
                kafkaBacklogMetrics.once('ready', next);
            },
            next => {
                zkClient = zookeeper.createClient(
                    zkConfig.connectionString,
                    zkConfig);
                zkClient.connect();
                zkClient.once('ready', next);
            },
            next => {
                producer = new BackbeatProducer({
                    kafka: { hosts: kafkaConfig.hosts },
                    topic: TOPIC,
                });
                producer.once('error', next);
                producer.once('ready', () => {
                    producer.removeAllListeners('error');
                    next();
                });
            },
            next => {
                consumer = new BackbeatTestConsumer({
                    kafka: { hosts: kafkaConfig.hosts },
                    topic: TOPIC,
                    groupId: GROUP_ID,
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
            next => zkClient.removeRecur(zkPath, next),
            next => producer.close(next),
            next => consumer.close(next),
        ], done);
    });

    it('should publish and check consumer backlog', done => {
        const message = { key: 'k1', message: 'm1' };
        let topicOffset;
        async.series([
            next => {
                consumer.expectOrderedMessages(
                    [message], CONSUMER_TIMEOUT, next);
                producer.send([message], err => {
                    assert.ifError(err);
                });
            },
            next => kafkaBacklogMetrics.publishConsumerBacklog(
                consumer._consumer, TOPIC, GROUP_ID, next),
            next => kafkaBacklogMetrics.checkConsumerLag(
                TOPIC, GROUP_ID, 0, (err, info) => {
                    assert.ifError(err);
                    // there shall be no lag
                    assert.strictEqual(info, undefined);
                    next();
                }),
            next => {
                zkClient.getData(
                    `${zkPath}/${TOPIC}/0/topic`, (err, offsetData) => {
                        assert.ifError(err);
                        topicOffset = JSON.parse(offsetData);
                        assert(Number.isInteger(topicOffset));
                        next();
                    });
            },
            next => {
                zkClient.getData(
                    `${zkPath}/${TOPIC}/0/consumers/${GROUP_ID}`,
                    (err, offsetData) => {
                        assert.ifError(err);
                        const consumerOffset = JSON.parse(offsetData);
                        assert(Number.isInteger(consumerOffset));
                        assert(consumerOffset === topicOffset);
                        next();
                    });
            },
        ], err => {
            assert.ifError(err);
            done();
        });
    });

    it('should return backlog info when lagging', done => {
        async.series([
            next => zkClient.setOrCreate(
                `${zkPath}/${TOPIC}/0/topic`, Buffer.from('5'), next),
            next => zkClient.setOrCreate(
                `${zkPath}/${TOPIC}/0/consumers/${GROUP_ID}`,
                Buffer.from('3'), next),
            next => kafkaBacklogMetrics.checkConsumerLag(
                TOPIC, GROUP_ID, 1, (err, info) => {
                    assert.ifError(err);
                    // there shall be some lag
                    assert.notStrictEqual(info, undefined);
                    assert.strictEqual(info.lag, 2);
                    assert.strictEqual(info.topic, TOPIC);
                    assert.strictEqual(info.partition, 0);
                    assert.strictEqual(info.groupId, GROUP_ID);
                    next();
                }),
        ], err => {
            assert.ifError(err);
            done();
        });
    });

    it('should snapshot latest topic offsets and check progress', done => {
        const message = { key: 'k2', message: 'm2' };
        async.series([
            next => {
                consumer.expectOrderedMessages(
                    [message], CONSUMER_TIMEOUT, next);
                producer.send([message], err => {
                    assert.ifError(err);
                });
            },
            next => kafkaBacklogMetrics.snapshotTopicOffsets(
                consumer._consumer, TOPIC, 'test-snapshot', next),
            next => kafkaBacklogMetrics.checkConsumerProgress(
                TOPIC, GROUP_ID, 'test-snapshot', (err, info) => {
                    assert.ifError(err);
                    // there shall be lag because the consumer did not
                    // publish its backlog yet
                    assert.notStrictEqual(info, undefined);
                    assert.strictEqual(info.lag, 1);
                    assert.strictEqual(info.topic, TOPIC);
                    assert.strictEqual(info.partition, 0);
                    assert.strictEqual(info.groupId, GROUP_ID);
                    next();
                }),
            next => {
                zkClient.getData(
                    `${zkPath}/${TOPIC}/0/snapshots/test-snapshot`,
                    (err, offsetData) => {
                        assert.ifError(err);
                        const snapshotOffset = JSON.parse(offsetData);
                        assert(Number.isInteger(snapshotOffset));
                        next();
                    });
            },
            next => kafkaBacklogMetrics.publishConsumerBacklog(
                consumer._consumer, TOPIC, GROUP_ID, next),
            next => kafkaBacklogMetrics.checkConsumerProgress(
                TOPIC, GROUP_ID, 'test-snapshot', (err, info) => {
                    assert.ifError(err);
                    // there shall be no lag now
                    assert.strictEqual(info, undefined);
                    next();
                }),
        ], err => {
            assert.ifError(err);
            done();
        });
    });
});
