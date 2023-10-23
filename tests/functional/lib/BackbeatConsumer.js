const assert = require('assert');
const async = require('async');

const { metrics } = require('arsenal');

const zookeeperHelper = require('../../../lib/clients/zookeeper');
const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const { promMetricNames } =
      require('../../../lib/constants').kafkaBacklogMetrics;
const zookeeperConf = { connectionString: 'localhost:2181' };
const producerKafkaConf = {
    hosts: 'localhost:9092',
};
const consumerKafkaConf = {
    hosts: 'localhost:9092',
    backlogMetrics: {
        zkPath: '/test/kafka-backlog-metrics',
        intervalS: 1,
    },
};

describe('BackbeatConsumer main tests', () => {
    const topic = 'backbeat-consumer-spec';
    const groupId = `replication-group-${Math.random()}`;
    const messages = [
        { key: 'foo', message: '{"hello":"foo"}' },
        { key: 'bar', message: '{"world":"bar"}' },
        { key: 'qux', message: '{"hi":"qux"}' },
    ];
    let zookeeper;
    let producer;
    let consumer;
    let consumedMessages = [];

    function queueProcessor(message, cb) {
        consumedMessages.push(message.value);
        process.nextTick(cb);
    }
    before(function before(done) {
        this.timeout(60000);

        producer = new BackbeatProducer({ kafka: producerKafkaConf, topic,
                                          pollIntervalMs: 100 });
        consumer = new BackbeatConsumer({
            zookeeper: zookeeperConf,
            kafka: consumerKafkaConf, groupId, topic,
            queueProcessor,
            bootstrap: true,
        });
        async.parallel([
            innerDone => producer.on('ready', innerDone),
            innerDone => consumer.on('ready', innerDone),
            innerDone => {
                zookeeper = zookeeperHelper.createClient(
                    zookeeperConf.connectionString);
                zookeeper.connect();
                zookeeper.on('ready', innerDone);
            },
        ], done);
    });
    afterEach(() => {
        consumedMessages = [];
        consumer.removeAllListeners('consumed');
    });
    after(function after(done) {
        this.timeout(10000);
        async.parallel([
            innerDone => producer.close(innerDone),
            innerDone => consumer.close(innerDone),
            innerDone => {
                zookeeper.close();
                innerDone();
            },
        ], done);
    });

    it('should be able to read messages sent to the topic and publish ' +
    'topic metrics', done => {
        let consumeCb = null;
        let totalConsumed = 0;
        let topicOffset;
        let consumerOffset;
        const zkMetricsPath = `/test/kafka-backlog-metrics/${topic}/0`;
        const latestConsumedMetric = metrics.ZenkoMetrics.getMetric(
            promMetricNames.latestConsumedMessageTimestamp);
        const beforeConsume = Date.now();
        // reset to 0 before the test
        latestConsumedMetric.reset();

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
        async function _checkPromMetrics() {
            const latestConsumedMetricValues =
                  (await latestConsumedMetric.get()).values;
            assert.strictEqual(latestConsumedMetricValues.length, 1);
            assert(latestConsumedMetricValues[0].value >= beforeConsume / 1000);
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
                // Prometheus metrics are updated locally in memory so
                // immediately visible
                _checkPromMetrics();
            }
        });
        consumeCb = done;
        producer.send(messages, err => {
            assert.ifError(err);
        });

        // Check that rdkafka metrics are indeed exported
        assert(metrics.ZenkoMetrics.getMetric('rdkafka_cgrp_assignment_size') !== undefined);
    }).timeout(30000);

    it('should not consume messages when paused and when resumed, consume ' +
    'messages from the previous offset', done => {
        let totalConsumed = 0;
        const kafkaConsumer = consumer._consumer;
        consumer.subscribe();

        async.series([
            next => {
                assert.equal(kafkaConsumer.subscription().length, 1);
                consumer.on('consumed', messagesConsumed => {
                    totalConsumed += messagesConsumed;
                    if (totalConsumed === 1) {
                        consumer.pause();
                        next();
                    }
                });
                producer.send([messages[0]], err => {
                    assert.ifError(err);
                });
            },
            next => {
                assert.equal(kafkaConsumer.subscription().length, 0);
                consumer.on('consumed', messagesConsumed => {
                    totalConsumed += messagesConsumed;
                    // should not consume when paused
                    return next(
                        new Error('expected consumer to be paused, messages ' +
                            'were still consumed')
                        );
                });
                // wait 5 seconds to see if any messages were consumed
                setTimeout(() => {
                    consumer.removeAllListeners('consumed');
                    assert.equal(totalConsumed, 1);
                    return next();
                }, 5000);
                producer.send(messages, err => {
                    assert.ifError(err);
                });
            },
            next => {
                consumer.resume();
                assert.equal(kafkaConsumer.subscription().length, 1);
                consumer.on('consumed', messagesConsumed => {
                    totalConsumed += messagesConsumed;
                    if (totalConsumed === messages.length + 1) {
                        next();
                    }
                });
            },
        ], err => {
            assert.ifError(err);
            // when resumed, the messages are read from previous offset
            assert.equal(totalConsumed, 4);
            done();
        });
    }).timeout(30000);
});

describe('BackbeatConsumer concurrency tests', () => {
    const topicConc = 'backbeat-consumer-spec-conc-1000';
    const groupIdConc = `replication-group-conc-${Math.random()}`;
    let producer;
    let consumer;
    let consumedMessages = [];
    const unstuck = { value: false };

    function queueProcessor(unstuck, message, cb) {
        if (message.value.toString() !== 'taskStuck') {
            consumedMessages.push(message.value);
            process.nextTick(cb);
        } else {
            const interval = setInterval(() => {
                if (unstuck.value) {
                    clearInterval(interval);
                    cb();
                }
            }, 1000);
        }
    }
    before(function before(done) {
        this.timeout(60000);

        producer = new BackbeatProducer({
            kafka: producerKafkaConf,
            topic: topicConc,
            pollIntervalMs: 100,
        });
        consumer = new BackbeatConsumer({
            zookeeper: zookeeperConf,
            kafka: consumerKafkaConf, groupId: groupIdConc, topic: topicConc,
            queueProcessor: queueProcessor.bind(null, unstuck),
            concurrency: 10,
            bootstrap: true,
        });
        async.parallel([
            innerDone => producer.on('ready', innerDone),
            innerDone => consumer.on('ready', innerDone),
        ], done);
    });
    afterEach(() => {
        consumedMessages = [];
        consumer.removeAllListeners('consumed');
    });
    after(done => {
        async.parallel([
            innerDone => producer.close(innerDone),
            innerDone => consumer.close(innerDone),
        ], done);
    });

    it('should be able to process 1000 messages with concurrency', done => {
        const boatloadOfMessages = [];
        for (let i = 0; i < 1000; ++i) {
            boatloadOfMessages.push({
                key: `message-${i}`,
                message: `{"message_index":"${i}"}`,
            });
        }
        async.series([
            next => {
                setTimeout(() => producer.send(boatloadOfMessages, err => {
                    assert.ifError(err);
                }), 1000);
                let totalConsumed = 0;
                consumer.subscribe();
                consumer.on('consumed', messagesConsumed => {
                    totalConsumed += messagesConsumed;
                    assert(totalConsumed <= boatloadOfMessages.length);
                    if (totalConsumed === boatloadOfMessages.length) {
                        next();
                    }
                });
            },
            next => {
                // looping to ease reporting when test fails
                // (otherwise node gets stuck for ages during diff
                // generation with an assert.deepStrictEqual() on
                // whole message arrays)
                assert.strictEqual(consumedMessages.length,
                                   boatloadOfMessages.length);
                for (let i = 0; i < consumedMessages.length; ++i) {
                    assert.deepStrictEqual(consumedMessages[i].toString(),
                                           boatloadOfMessages[i].message);
                }
                next();
            },
        ], done);
    });

    it.only('should not prevent progress with concurrency if one task is stuck',
    done => {
        const boatloadOfMessages = [];
        const stuckIndex = 500;
        for (let i = 0; i < 1000; ++i) {
            boatloadOfMessages.push({
                key: `message-${i}`,
                message: i === stuckIndex ?
                    'taskStuck' : `{"message_index":"${i}"}`,
            });
        }
        async.series([
            next => {
                setTimeout(() => producer.send(boatloadOfMessages, err => {
                    assert.ifError(err);
                }), 5000);
                let totalConsumed = 0;
                consumer.subscribe();
                consumer.on('consumed', messagesConsumed => {
                    totalConsumed += messagesConsumed;
                    assert(totalConsumed <= boatloadOfMessages.length);
                    if (totalConsumed === boatloadOfMessages.length) {
                        next();
                    }
                });
            },
            next => {
                // looping to ease reporting when test fails
                // (otherwise node gets stuck for ages during diff
                // generation with an assert.deepStrictEqual() on
                // whole message arrays)
                assert.strictEqual(consumedMessages.length,
                                   boatloadOfMessages.length - 1);
                for (let i = 0; i < consumedMessages.length; ++i) {
                    assert.deepStrictEqual(
                        consumedMessages[i].toString(),
                        i < stuckIndex ?
                            boatloadOfMessages[i].message :
                            boatloadOfMessages[i + 1].message);
                }
                unstuck.value = true;
                next();
            },
        ], done);
    });
});

describe('BackbeatConsumer "deferred committable" tests', () => {
    const topicConc = 'backbeat-consumer-spec-deferred';
    const groupIdConc = `replication-group-deferred-${Math.random()}`;
    let producer;
    let consumer;
    let consumedMessages = [];

    function queueProcessor(message, cb) {
        consumedMessages.push(message.value);
        if (JSON.parse(message.value.toString()).deferred) {
            process.nextTick(() => cb(null, { committable: false }));
            setTimeout(() => {
                consumer.onEntryCommittable(message);
            }, 900 + Math.floor(Math.random() * 200));
        } else {
            process.nextTick(cb);
        }
    }
    before(function before(done) {
        this.timeout(60000);

        producer = new BackbeatProducer({
            kafka: producerKafkaConf,
            topic: topicConc,
            pollIntervalMs: 100,
        });
        consumer = new BackbeatConsumer({
            zookeeper: zookeeperConf,
            kafka: consumerKafkaConf, groupId: groupIdConc, topic: topicConc,
            queueProcessor,
            concurrency: 10,
            bootstrap: true,
        });
        async.parallel([
            innerDone => producer.on('ready', innerDone),
            innerDone => consumer.on('ready', innerDone),
        ], done);
    });
    afterEach(() => {
        consumedMessages = [];
        consumer.removeAllListeners('consumed');
    });
    after(done => {
        async.parallel([
            innerDone => producer.close(innerDone),
            innerDone => consumer.close(innerDone),
        ], done);
    });

    it('should be able to process 1000 messages with some deferred ' +
    'committable status', done => {
        const boatloadOfMessages = [];
        for (let i = 0; i < 1000; ++i) {
            boatloadOfMessages.push({
                key: `message-${i}`,
                message: `{"message_index":"${i}",` +
                    `"deferred":${i % 2 === 0 ? 'true' : 'false'}}`,
            });
        }
        setTimeout(() => producer.send(boatloadOfMessages, err => {
            assert.ifError(err);
        }), 1000);
        let totalConsumed = 0;
        consumer.subscribe();
        consumer.on('consumed', messagesConsumed => {
            totalConsumed += messagesConsumed;
            assert(totalConsumed <= boatloadOfMessages.length);
            if (totalConsumed === boatloadOfMessages.length) {
                assert.strictEqual(
                    consumer.getOffsetLedger().getProcessingCount(),
                    500);
                // offsets are set to be committable after 1 second in this
                // test, so wait for 2 seconds
                setTimeout(() => {
                    assert.strictEqual(
                        consumer.getOffsetLedger().getProcessingCount(),
                        0);
                    done();
                }, 2000);
            }
        });
    });
});

describe('BackbeatConsumer with circuit breaker', () => {
    const nMessages = 0;

    const testCases = [
        {
            description: 'should consume if breaker state nominal',
            startDelayMs: 0,
            expectedMessages: nMessages,
            breakerConf: {
                probes: [
                    {
                        type: 'noop',
                        returnConstantValue: true,
                    },
                ],
            },
        },
        {
            description: 'should not consume if breaker state not nominal',
            startDelayMs: 50,
            expectedMessages: 0,
            breakerConf: {
                nominalEvaluateIntervalMs: 1,
                probes: [
                    {
                        type: 'noop',
                        returnConstantValue: false,
                    },
                ],
            },
        },
    ];

    testCases.forEach(t => {
        const topicBreaker = 'backbeat-consumer-spec-breaker';
        const groupIdBreaker = `replication-group-breaker-${Math.random()}`;
        let producer;
        let consumer;
        let consumedMessages = [];

        function queueProcessor(message, cb) {
            if (message.value.toString() !== 'taskStuck') {
                consumedMessages.push(message.value);
                process.nextTick(cb);
            }
        }
        before(function before(done) {
            this.timeout(60000);

            producer = new BackbeatProducer({
                kafka: producerKafkaConf,
                topic: topicBreaker,
                pollIntervalMs: 100,
            });
            consumer = new BackbeatConsumer({
                zookeeper: zookeeperConf,
                kafka: consumerKafkaConf, groupId: groupIdBreaker, topic: topicBreaker,
                queueProcessor,
                concurrency: 10,
                bootstrap: true,
                circuitBreaker: t.breakerConf,
            });
            async.parallel([
                innerDone => producer.on('ready', innerDone),
                innerDone => consumer.on('ready', innerDone),
            ], (err, res) => {
                setTimeout(() => done(err, res), t.startDelayMs);
            });
        });
        afterEach(() => {
            consumedMessages = [];
            consumer.removeAllListeners('consumed');
        });
        after(done => {
            async.parallel([
                innerDone => producer.close(innerDone),
                innerDone => consumer.close(innerDone),
            ], done);
        });

        it(t.description, done => {
            const boatloadOfMessages = [];
            for (let i = 0; i < nMessages; ++i) {
                boatloadOfMessages.push({
                    key: `message-${i}`,
                    message: `{"message_index":"${i}"}`,
                });
            }

            let totalConsumed = 0;

            async.series([
                next => {
                    setTimeout(() => producer.send(boatloadOfMessages, err => {
                        assert.ifError(err);
                    }), 1000);
                    consumer.subscribe();
                    setTimeout(next, 5000);
                    consumer.on('consumed', messagesConsumed => {
                        totalConsumed += messagesConsumed;
                    });
                },
                next => {
                    assert.strictEqual(totalConsumed, t.expectedMessages);
                    next();
                },
            ], done);
        });
    });
});

describe('BackbeatConsumer shutdown tests', () => {
    const topic = 'backbeat-consumer-spec-shutdown';
    const groupId = `bucket-processor-${Math.random()}`;
    const messages = [
        { key: 'm1', message: '{"value":"1"}' },
        { key: 'm2', message: '{"value":"2"}' },
    ];
    let zookeeper;
    let producer;

    before(function before(done) {
        this.timeout(60000);
        producer = new BackbeatProducer({
            topic,
            kafka: producerKafkaConf,
            pollIntervalMs: 100,
        });
        async.parallel([
            innerDone => producer.on('ready', innerDone),
            innerDone => {
                zookeeper = zookeeperHelper.createClient(
                    zookeeperConf.connectionString);
                zookeeper.connect();
                zookeeper.on('ready', innerDone);
            },
        ], done);
    });

    after(function after(done) {
        this.timeout(10000);
        async.parallel([
            innerDone => producer.close(innerDone),
            innerDone => {
                zookeeper.close();
                innerDone();
            },
        ], done);
    });

    it('should wait for current jobs to end and commit offset before shutting down', done => {
        function queueProcessor(message, cb) {
            setTimeout(cb, 2000);
        }
        const consumer = new BackbeatConsumer({
            zookeeper: zookeeperConf,
            kafka: consumerKafkaConf,
            queueProcessor,
            groupId,
            topic,
            bootstrap: true,
            concurrency: 2,
        });
        async.series([
            next => consumer.on('ready', next),
            next => {
                consumer.subscribe();
                setTimeout(() => {
                    producer.send(messages, assert.ifError);
                }, 3000);
                next();
            },
            next => {
                const interval = setInterval(() => {
                    if (consumer._processingQueue.idle()) {
                        return;
                    }
                    clearInterval(interval);
                    next();
                }, 500);
            },
            next => {
                assert(!consumer._processingQueue.idle());
                consumer.close(() => {
                    assert(consumer._processingQueue.idle());
                    assert.strictEqual(consumer.getOffsetLedger().getProcessingCount(topic), 0);
                    next();
                });
            },
        ], done);
    }).timeout(30000);
});
