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

describe('BackbeatConsumer rebalance tests', () => {
    const topic = 'backbeat-consumer-spec-rebalance';
    const groupId = `replication-group-${Math.random()}`;
    const messages = [
        { key: 'foo', message: '{"hello":"foo"}' },
        { key: 'bar', message: '{"world":"bar"}' },
        { key: 'qux', message: '{"hi":"qux"}' },
    ];
    let producer;
    let consumer;
    let consumer2;
    let processedMessages;
    let consumedMessages;

    function queueProcessor(message, cb) {
        assert(processedMessages.length < messages.length);

        consumer.emit('consumed.message', message.value.toString());

        // Shorter delay for first message, to ensure there is something being processed during the
        // rebalance
        setTimeout(() => {
            processedMessages.push(message.value);
            assert(processedMessages.length <= messages.length);

            process.nextTick(() => {
                cb();

                consumer.emit('processed.message', message.value.toString());

                if (processedMessages.length === messages.length) {
                    assert.deepStrictEqual(
                        processedMessages.map(buffer => buffer.toString()),
                        messages.map(e => e.message));
                    consumer.emit('processed.all');
                }
            });
        }, consumedMessages++ ? 4000 : 2000);
    }

    before(function before(done) {
        this.timeout(60000);

        // Bootstrap just once at the beginning of the test suite
        consumer = new BackbeatConsumer({
            zookeeper: zookeeperConf,
            kafka: { hosts: consumerKafkaConf.hosts }, groupId, topic,
            queueProcessor,
            bootstrap: true,
        });
        consumer.on('ready', () => consumer.close(done));
    });

    beforeEach(function before(done) {
        this.timeout(60000);

        consumedMessages = 0;
        processedMessages = [];
        producer = new BackbeatProducer({
            kafka: producerKafkaConf, topic,
            pollIntervalMs: 100
        });
        consumer = new BackbeatConsumer({
            zookeeper: zookeeperConf,
            kafka: consumerKafkaConf, groupId, topic,
            queueProcessor,
            concurrency: 2,
        });

        async.parallel([
            innerDone => producer.on('ready', innerDone),
            innerDone => async.series([
                cb => consumer.on('ready', cb),
                cb => {
                    consumer2 = new BackbeatConsumer({
                        zookeeper: zookeeperConf,
                        kafka: consumerKafkaConf, groupId, topic,
                        queueProcessor,
                    });
                    consumer2.on('ready', cb);
                },
            ], innerDone),
        ], done);
    });

    afterEach(function after(done) {
        this.timeout(10000);
        async.parallel([
            innerDone => producer.close(innerDone),
            innerDone => consumer.close(innerDone),
            innerDone => (consumer2 ? consumer2.close(innerDone) : innerDone()),
        ], done);
    });

    it('should handle rebalance when no task in progress', done => {
        consumer.on('processed.all', () => {
            // create second consumer: should rebalance...
            consumer2._queueProcessor = message => {
                assert.fail(`unexpected message received ${message.value}`);
            };
            consumer2.subscribe();

            // wait a bit, ensure no message happens afterwards...
            setTimeout(done, 5000);
        });

        consumer.subscribe();

        // send data to topic : should be consumed
        producer.send(messages, err => {
            assert.ifError(err);
        });
    }).timeout(40000);

    it('should commit current tasks during rebalance', done => {
        consumer.on('processed.all', () => {
            // wait a bit, ensure no message happens afterwards...
            setTimeout(done, 5000);
        });

        consumer.on('consumed.message', message => {
            consumer._log.debug('consumed', { message });
            if (consumedMessages === 0) {
                // trigger rebalance during processing of first message
                consumer2.subscribe();
            }
        });

        consumer.subscribe();

        // send data to topic : should be consumed
        producer.send(messages, err => {
            assert.ifError(err);
        });
    }).timeout(40000);
});

describe('BackbeatConsumer concurrency tests', () => {
    const topicConc = 'backbeat-consumer-spec-conc-1000';
    const groupIdConc = `replication-group-conc-${Math.random()}`;
    let producer;
    let consumer;
    let consumedMessages = [];
    let taskStuckCallbacks = [];

    function queueProcessor(message, cb) {
        if (message.value.toString() !== 'taskStuck') {
            consumedMessages.push(message.value);
            process.nextTick(cb);
        } else {
            taskStuckCallbacks.push(cb);
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

        taskStuckCallbacks.map(cb => cb());
        taskStuckCallbacks = [];
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

    it('should not prevent progress with concurrency if one task is stuck',
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
                                   boatloadOfMessages.length - 1);
                for (let i = 0; i < consumedMessages.length; ++i) {
                    assert.deepStrictEqual(
                        consumedMessages[i].toString(),
                        i < stuckIndex ?
                            boatloadOfMessages[i].message :
                            boatloadOfMessages[i + 1].message);
                }
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
    const topicBreaker = 'backbeat-consumer-spec-breaker';
    let groupIdBreaker;
    let producer;
    let consumer;
    let consumedMessages = [];

    function queueProcessor(message, cb) {
        consumedMessages.push(message.value);
        process.nextTick(cb);
    }

    beforeEach(function before(done) {
        this.timeout(60000);

        groupIdBreaker = `replication-group-breaker-${Math.random()}`;

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
            circuitBreaker: this.currentTest.breakerConf,
        });
        async.parallel([
            innerDone => producer.on('ready', innerDone),
            innerDone => consumer.on('ready', innerDone),
        ], done);
    });

    afterEach(done => {
        consumedMessages = [];
        consumer.removeAllListeners('consumed');

        async.parallel([
            innerDone => producer.close(innerDone),
            innerDone => consumer.close(innerDone),
        ], done);
    });

    const nMessages = 50;

    const testCases = [
        {
            description: 'should consume if breaker state nominal',
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
        const test = it(t.description, done => {
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

        // Attach breakerConf to the test, so it can be used from the hooks
        test.breakerConf = t.breakerConf;
    });
});
