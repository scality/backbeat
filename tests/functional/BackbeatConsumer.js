const assert = require('assert');
const async = require('async');
const zookeeperHelper = require('../../lib/clients/zookeeper');
const BackbeatProducer = require('../../lib/BackbeatProducer');
const BackbeatConsumer = require('../../lib/BackbeatConsumer');
const zookeeperConf = { connectionString: 'localhost:2181' };
const kafkaConf = { hosts: 'localhost:9092' };
const topic = 'backbeat-consumer-spec';
const groupId = `replication-group-${Math.random()}`;
const messages = [
    { key: 'foo', message: 'hello' },
    { key: 'bar', message: 'world' },
    { key: 'qux', message: 'hi' },
];
describe('BackbeatConsumer', () => {
    let zookeeper;
    let producer;
    let consumer;
    const consumedMessages = [];
    function queueProcessor(message, cb) {
        consumedMessages.push(message.value);
        process.nextTick(cb);
    }
    before(function before(done) {
        this.timeout(60000);

        let zookeeperReady = false;
        let producerReady = false;
        let consumerReady = false;
        function _doneIfReady() {
            if (zookeeperReady && producerReady && consumerReady) {
                done();
            }
        }
        producer = new BackbeatProducer({ kafka: kafkaConf, topic,
                                          pollIntervalMs: 100 });
        consumer = new BackbeatConsumer({ zookeeper: zookeeperConf,
                                          kafka: kafkaConf, groupId, topic,
                                          queueProcessor,
                                          backlogMetrics: {
                                              zkPath: '/test/backlog-metrics',
                                              intervalS: 1,
                                          },
                                          bootstrap: true,
                                        });
        zookeeper = zookeeperHelper.createClient(
            zookeeperConf.connectionString);
        zookeeper.connect();
        zookeeper.on('ready', () => {
            zookeeperReady = true;
            _doneIfReady();
        });
        producer.on('ready', () => {
            producerReady = true;
            _doneIfReady();
        });
        consumer.on('ready', () => {
            consumerReady = true;
            _doneIfReady();
        });
    });
    after(done => {
        producer.close(() => {
            consumer.close(() => {
                zookeeper.close();
                zookeeper = null;
                producer = null;
                consumer = null;
                done();
            });
        });
    });

    it('should be able to read messages sent to the topic and publish ' +
    'backlog metrics', done => {
        let consumeCb = null;
        let totalConsumed = 0;
        let topicOffset;
        let consumerOffset;
        const zkMetricsPath = `/test/backlog-metrics/${topic}/0`;
        function _checkZkMetrics(done) {
            async.waterfall([
                next => zookeeper.getData(`${zkMetricsPath}/topic`, next),
                (topicOffsetData, stat, next) => {
                    topicOffset = Number.parseInt(topicOffsetData, 10);
                    zookeeper.getData(`${zkMetricsPath}/consumers/${groupId}`,
                                      next);
                },
            ], (err, consumerOffsetData) => {
                assert.ifError(err);
                consumerOffset = Number.parseInt(consumerOffsetData, 10);
                assert.strictEqual(topicOffset, consumerOffset);
                done();
            });
        }
        consumer.subscribe();
        consumer.on('consumed', messagesConsumed => {
            totalConsumed += messagesConsumed;
            assert(totalConsumed <= messages.length);
            if (totalConsumed === messages.length) {
                assert.deepStrictEqual(
                    messages.map(e => e.message),
                    consumedMessages.map(buffer => buffer.toString()));
                // metrics are published every second, so they
                // should be there after 5s
                setTimeout(() => {
                    _checkZkMetrics(() => {
                        consumeCb();
                        consumer.unsubscribe();
                    });
                }, 5000);
                assert.deepStrictEqual(
                    messages.map(e => e.message),
                    consumedMessages.map(buffer => buffer.toString()));
            }
        });
        consumeCb = done;
        producer.send(messages, err => {
            assert.ifError(err);
        });
    }).timeout(30000);
});
