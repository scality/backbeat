const assert = require('assert');
const async = require('async');
const sinon = require('sinon');
const werelogs = require('werelogs');

const { metrics } = require('arsenal');
const { ObjectMD } = require('arsenal').models;

const ZookeeperManager = require('../../../lib/clients/ZookeeperManager');
const BackbeatProducer = require('../../../lib/BackbeatProducer');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const ActionQueueEntry = require('../../../lib/models/ActionQueueEntry');
const ColdStorageStatusQueueEntry =
    require('../../../lib/models/ColdStorageStatusQueueEntry');
const GarbageCollectorTask =
    require('../../../extensions/gc/tasks/GarbageCollectorTask');
const LifecycleColdStatusArchiveTask = require(
    '../../../extensions/lifecycle/tasks/LifecycleColdStatusArchiveTask');
const {
    ProcessorMock,
    BackbeatClientMock,
    BackbeatMetadataProxyMock,
    GarbageCollectorProducerMock,
    BackbeatProducerMock,
} = require('../../unit/mocks');
const { BreakerState, CircuitBreaker } = require('breakbeat').CircuitBreaker;
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
const log = new werelogs.Logger('BackbeatConsumer:test');

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
                zookeeper = new ZookeeperManager(zookeeperConf.connectionString, null, log);
                zookeeper.on('ready', innerDone);
            },
        ], done);
    });
    afterEach(() => {
        consumer._consumer.unsubscribe();
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
                        consumer._consumer.unsubscribe();
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
    let timer;

    function queueProcessor(message, cb) {
        assert(processedMessages.length < messages.length);

        const res = consumer.emit('consumed.message', message.value.toString());

        if (res && message.value.toString() === 'taskStuck') {
            consumer._log.info('processing message...');
            return;
        }

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
        const bootstrapConsumer = new BackbeatConsumer({
            zookeeper: zookeeperConf,
            kafka: { hosts: consumerKafkaConf.hosts }, groupId, topic,
            queueProcessor,
            bootstrap: true,
        });
        bootstrapConsumer.on('ready', () => bootstrapConsumer.close(done));
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
            clientId: 'BackbeatConsumer-1',
            zookeeper: zookeeperConf,
            kafka: { ...consumerKafkaConf, maxPollIntervalMs: 45000 },
            groupId, topic,
            queueProcessor,
            concurrency: 2,
        });

        async.parallel([
            innerDone => producer.on('ready', innerDone),
            innerDone => async.series([
                cb => consumer.on('ready', cb),
                cb => {
                    consumer2 = new BackbeatConsumer({
                        clientId: 'BackbeatConsumer-2',
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
        if (timer) {
            clearInterval(timer);
            timer = null;
        }
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

    it('should fail healthcheck on rebalance timeout', done => {
        assert(consumer.isReady());
        assert(consumer2.isReady());

        consumer.once('consumed.message', () => {
            // trigger rebalance during processing of first message
            consumer2.subscribe();

            // Return true to allow the consumer to "be stuck" on the message
            return true;
        });

        consumer.subscribe();
        producer.send([{ key: 'msg', message: 'taskStuck' }], err => {
            assert.ifError(err);
        });

        // The consumer should become unhealthy eventually
        timer = setInterval(() => {
            if (!consumer.isReady()) {
                assert(consumer2.isReady());
                done();
            }
        }, 1000);
    }).timeout(60000);

});

describe('BackbeatConsumer deferred commit after rebalance', () => {
    const topic = 'backbeat-consumer-spec-ERR-STATE';
    const groupId = `replication-group-${Math.random()}`;
    let producer;
    let consumer1;
    let consumer2;

    before(function before(done) {
        this.timeout(60000);

        producer = new BackbeatProducer({
            kafka: producerKafkaConf, topic,
            pollIntervalMs: 100,
            compressionType: 'none',
        });
        consumer1 = new BackbeatConsumer({
            clientId: 'BackbeatConsumer-ERR-STATE-1',
            zookeeper: zookeeperConf,
            kafka: { ...consumerKafkaConf, compressionType: 'none' },
            groupId, topic,
            queueProcessor: (_msg, cb) => cb(),
            bootstrap: true,
        });
        async.parallel([
            innerDone => producer.on('ready', innerDone),
            innerDone => consumer1.on('ready', innerDone),
        ], err => {
            if (err) {return done(err);}
            consumer2 = new BackbeatConsumer({
                clientId: 'BackbeatConsumer-ERR-STATE-2',
                zookeeper: zookeeperConf,
                kafka: { ...consumerKafkaConf, compressionType: 'none' },
                groupId, topic,
                queueProcessor: (_msg, cb) => cb(),
            });
            consumer2.on('ready', done);
        });
    });

    after(function after(done) {
        this.timeout(10000);
        async.parallel([
            innerDone => producer.close(innerDone),
            innerDone => consumer1.close(innerDone),
            innerDone => (consumer2 ? consumer2.close(innerDone) : innerDone()),
        ], done);
    });

    it('should not crash when onEntryCommittable is called after partition revoke', done => {
        let deferredEntry = null;

        // Setup: when consumer1 receives a message, complete with
        // { committable: false }. This frees the processing queue
        // slot but does NOT commit the offset.
        consumer1._queueProcessor = (message, cb) => {
            deferredEntry = message;
            process.nextTick(() => cb(null, { committable: false }));
        };

        consumer2._queueProcessor = (_message, cb) => {
            process.nextTick(cb);
        };

        // 1 : consumer1 subscribes and consumes the message.
        consumer1.subscribe();
        producer.send([{ key: 'foo', message: '{"hello":"foo"}' }], err => {
            assert.ifError(err);
        });

        // 2 : wait until consumer1 has processed the message.
        // The processing queue is now idle but the
        // deferred commit is still pending.
        const waitForDeferred = setInterval(() => {
            if (!deferredEntry) {
                return;
            }
            clearInterval(waitForDeferred);

            // 3 : consumer2 joins the same group, triggering a
            // rebalance. consumer1's revoke handler sees an idle
            // queue and immediately unassigns the partition.
            consumer1.once('unassign', () => {
                // 4 : the external caller finishes its work and calls
                // onEntryCommittable() for the now-revoked partition.
                // It would crash without the try catch in the method, as
                // an error ERR__STATE is returned by librdkafka when trying to commit
                assert.doesNotThrow(() => {
                    consumer1.onEntryCommittable(deferredEntry);
                });
                done();
            });

            consumer2.subscribe();
        }, 100);
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
        this.timeout(120000);

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
        // resetting the circuit breaker to avoid having
        // a timeout when closing the consumer, as it depends
        // on a revoke rebalance event that only gets triggered
        // by polling (calling consumer.consume())
        consumer._circuitBreaker = new CircuitBreaker();

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

    it('should only resume consumption when breaker state is nominal', function test(done) {
        this.test.breakerConf = {
            probes: [
                {
                    type: 'noop',
                    returnConstantValue: true, // nominal state
                },
            ],
        };

        const boatloadOfMessages = [];
        for (let i = 0; i < nMessages; ++i) {
            boatloadOfMessages.push({
                key: `message-${i}`,
                message: `{"message_index":"${i}"}`,
            });
        }

        let totalConsumed = 0;

        consumer.subscribe();

        consumer.on('consumed', messagesConsumed => {
            totalConsumed += messagesConsumed;
        });

        async.series([
            next => {
                const interval = setInterval(() => {
                    if (consumer._consumer.assignments().length !== 0) {
                        clearInterval(interval);
                        next();
                    }
                }, 1000);
            },
            next => {
                setTimeout(() => producer.send(boatloadOfMessages, err => {
                    assert.ifError(err);
                }), 1000);
                consumer._circuitBreaker.emit('state-changed', BreakerState.Tripped);
                setTimeout(next, 3000);
            },
            next => {
                assert.strictEqual(totalConsumed, 0);
                consumer._circuitBreaker.emit('state-changed', BreakerState.Stabilizing);
                setTimeout(next, 3000);
            },
            next => {
                assert.strictEqual(totalConsumed, 0);
                consumer._circuitBreaker.emit('state-changed', BreakerState.Nominal);
                setTimeout(next, 3000);
            },
            next => {
                assert.strictEqual(totalConsumed, nMessages);
                next();
            }
        ], done);
    }).timeout(30000);
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
    let consumer;

    function queueProcessor(message, cb) {
        if (message.value.toString() !== 'taskStuck') {
            setTimeout(cb, 1000);
        }
    }

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
                zookeeper = new ZookeeperManager(zookeeperConf.connectionString, null, log);
                zookeeper.on('ready', innerDone);
            },
        ], done);
    });

    beforeEach(function beforeEach(done) {
        this.timeout(60000);
        consumer = new BackbeatConsumer({
            zookeeper: zookeeperConf,
            kafka: {
                maxPollIntervalMs: 45000,
                ...consumerKafkaConf,
            },
            queueProcessor,
            groupId,
            topic,
            bootstrap: true,
            concurrency: 2,
        });
        consumer.on('ready', () => {
            consumer.subscribe();
            done();
        });
    });

    afterEach(() => {
        consumer.removeAllListeners('consumed');
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

    it('should stop consuming and wait for current jobs to end before shutting down', done => {
        setTimeout(() => {
            producer.send(messages, assert.ifError);
        }, 3000);
        let totalConsumed = 0;
        consumer.on('consumed', messagesConsumed => {
            totalConsumed += messagesConsumed;
        });
        async.series([
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
                    // concurrency set to 2, so should only consume the first two
                    // initial messages before shutting down
                    assert(totalConsumed <= 2);
                    assert.strictEqual(consumer.getOffsetLedger().getProcessingCount(topic), 0);
                    next();
                });
            },
        ], done);
    }).timeout(30000);

    it('should immediatly shuttdown when no in progress tasks', done => {
        setTimeout(() => {
            producer.send([messages[0]], assert.ifError);
        }, 3000);
        async.series([
            next => {
                const interval = setInterval(() => {
                    if (!consumer._processingQueue.idle()) {
                        return;
                    }
                    clearInterval(interval);
                    next();
                }, 500);
            },
            next => {
                assert(consumer._processingQueue.idle());
                consumer.close(() => {
                    assert(consumer._processingQueue.idle());
                    assert.strictEqual(consumer.getOffsetLedger().getProcessingCount(topic), 0);
                    next();
                });
            },
        ], done);
    }).timeout(30000);

    it('should shuttdown when consumer has been disconnected', done => {
        async.series([
            next => {
                consumer._consumer.disconnect();
                consumer._consumer.on('disconnected', () => next());
            },
            next => consumer.close(next),
        ], done);
    }).timeout(30000);

    it('should close even when a job is stuck', done => {
        setTimeout(() => {
            producer.send([{ key: 'key', message: 'taskStuck' }], assert.ifError);
        }, 3000);
        async.series([
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
                    assert(!consumer._processingQueue.idle());
                    next();
                });
            },
        ], done);
    }).timeout(60000);
});
describe('BackbeatConsumer offset progress when GarbageCollectorTask fails',
() => {
    const topic = 'backbeat-consumer-spec-gc-task-fail';
    const groupId = `replication-group-gc-fail-${Math.random()}`;
    let producer;
    let consumer;

    before(function before(done) {
        this.timeout(60000);
        producer = new BackbeatProducer({
            kafka: producerKafkaConf, topic,
            pollIntervalMs: 100, compressionType: 'none',
        });
        consumer = new BackbeatConsumer({
            clientId: 'BackbeatConsumer-gc-task-fail',
            zookeeper: zookeeperConf,
            kafka: { ...consumerKafkaConf, compressionType: 'none' },
            groupId, topic,
            queueProcessor: (_msg, cb) => cb(),
            bootstrap: true,
        });
        async.parallel([
            innerDone => producer.on('ready', innerDone),
            innerDone => consumer.on('ready', innerDone),
        ], done);
    });

    after(function after(done) {
        this.timeout(10000);
        async.parallel([
            innerDone => producer.close(innerDone),
            innerDone => consumer.close(innerDone),
        ], done);
    });

    it('commits the offset after retries are exhausted', function (done) {
        this.timeout(60000);
        const N = 3;
        let processedCount = 0;

        const backbeatClient = new BackbeatClientMock();
        const backbeatMdProxy = new BackbeatMetadataProxyMock();
        const gcProducer = new GarbageCollectorProducerMock();
        const mdObj = new ObjectMD()
            .setLocation([{ key: 'k', size: 10, start: 0,
                dataStoreName: 'old-location' }])
            .setDataStoreName('old-location')
            .setAmzStorageClass('old-location')
            .setTransitionInProgress(true);
        backbeatMdProxy.setMdObj(mdObj);

        backbeatClient.batchDeleteResponse = {
            error: { statusCode: 500, retryable: true }, res: null,
        };

        const gcConfig = {
            consumer: {
                retry: {
                    maxRetries: 2,
                    backoff: { min: 50, max: 200, jitter: 0, factor: 1.5 },
                },
            },
        };
        const gcProcessor = new ProcessorMock(
            null, null, backbeatClient, backbeatMdProxy, gcProducer, null,
            gcConfig, new werelogs.Logger('test:gc'));

        consumer.on('error', () => {});
        consumer._queueProcessor = (kafkaEntry, cb) => {
            const entry = ActionQueueEntry.createFromKafkaEntry(kafkaEntry);
            const task = new GarbageCollectorTask(gcProcessor);
            task.processActionEntry(entry, (err, commitInfo) => {
                processedCount++;
                cb(err, commitInfo);
            });
        };

        consumer._consumer.committed(
            [{ topic, partition: 0 }], 5000, (e1, base) => {
            assert.ifError(e1);
            const baseline = base[0].offset;

            consumer.subscribe();

            const messages = [];
            for (let i = 0; i < N; i++) {
                const entry = ActionQueueEntry.create('deleteArchivedSourceData')
                    .addContext({
                        origin: 'lifecycle', ruleType: 'archive',
                        bucketName: 'b', objectKey: `k${i}`, versionId: 'v',
                    })
                    .setAttribute('serviceName', 'lifecycle-transition')
                    .setAttribute('target.oldLocation', 'old-location')
                    .setAttribute('target.newLocation', 'new-location')
                    .setAttribute('target.bucket', 'b')
                    .setAttribute('target.key', `k${i}`)
                    .setAttribute('target.version', 'v')
                    .setAttribute('target.accountId', '834789881858')
                    .setAttribute('target.owner', 'o');
                messages.push({ key: `k${i}`, message: entry.toKafkaMessage() });
            }
            producer.send(messages, e2 => assert.ifError(e2));

            const checkProcessed = setInterval(() => {
                if (processedCount < N) {
                    return;
                }
                clearInterval(checkProcessed);
                setTimeout(() => {
                    consumer._consumer.committed(
                        [{ topic, partition: 0 }], 5000, (e3, c) => {
                        assert.ifError(e3);
                        const observed = c[0].offset;
                        const expected = baseline + N;
                        assert.strictEqual(observed, expected,
                            'expected committed offset to advance from ' +
                            `${baseline} to ${expected} after ${N} entries ` +
                            'failed through GarbageCollectorTask retry ' +
                            `exhaustion, but it stayed at ${observed}`);
                        done();
                    });
                }, 7000);
            }, 100);
        });
    });
});

describe('BackbeatConsumer offset progress when ' +
         'LifecycleColdStatusArchiveTask fails', () => {
    const topic = 'backbeat-consumer-spec-lifecycle-task-fail';
    const groupId = `replication-group-lifecycle-fail-${Math.random()}`;
    let producer;
    let consumer;

    before(function before(done) {
        this.timeout(60000);
        producer = new BackbeatProducer({
            kafka: producerKafkaConf, topic,
            pollIntervalMs: 100, compressionType: 'none',
        });
        consumer = new BackbeatConsumer({
            clientId: 'BackbeatConsumer-lifecycle-task-fail',
            zookeeper: zookeeperConf,
            kafka: { ...consumerKafkaConf, compressionType: 'none' },
            groupId, topic,
            queueProcessor: (_msg, cb) => cb(),
            bootstrap: true,
        });
        async.parallel([
            innerDone => producer.on('ready', innerDone),
            innerDone => consumer.on('ready', innerDone),
        ], done);
    });

    after(function after(done) {
        this.timeout(10000);
        async.parallel([
            innerDone => producer.close(innerDone),
            innerDone => consumer.close(innerDone),
        ], done);
    });

    it('commits the offset after retries are exhausted', function (done) {
        this.timeout(60000);
        const N = 3;
        let processedCount = 0;
        const coldLocation = 'cold';

        const backbeatClient = new BackbeatClientMock();
        const backbeatMdProxy = new BackbeatMetadataProxyMock();
        const gcProducer = new GarbageCollectorProducerMock();
        const coldProducer = new BackbeatProducerMock();
        const objectProcessor = new ProcessorMock(
            { coldStorageStatusTopicPrefix: 'cold-' }, null,
            backbeatClient, backbeatMdProxy, gcProducer, coldProducer, null,
            new werelogs.Logger('test:lifecycle'));

        sinon.stub(backbeatMdProxy, 'getMetadata').yields(
            Object.assign(new Error('simulated downstream failure'),
                { statusCode: 500, retryable: true }));

        const retryWrapper = new BackbeatTask({
            maxRetries: 2,
            backoff: { min: 50, max: 200, jitter: 0, factor: 1.5 },
        });
        const testLog = new werelogs.Logger('test:lifecycle');

        consumer.on('error', () => {});
        consumer._queueProcessor = (kafkaEntry, cb) => {
            const entry = ColdStorageStatusQueueEntry
                .createFromKafkaEntry(kafkaEntry);
            const task = new LifecycleColdStatusArchiveTask(objectProcessor);
            retryWrapper.retry({
                actionDesc: 'process cold storage status entry',
                actionFunc: done =>
                    task.processEntry(coldLocation, entry, done),
                shouldRetryFunc: err => err.retryable,
                log: testLog,
            }, (err, commitInfo) => {
                processedCount++;
                cb(err, commitInfo);
            });
        };

        consumer._consumer.committed(
            [{ topic, partition: 0 }], 5000, (e1, base) => {
            assert.ifError(e1);
            const baseline = base[0].offset;

            consumer.subscribe();

            const messages = [];
            for (let i = 0; i < N; i++) {
                const messageBody = JSON.stringify({
                    op: 'archive',
                    bucketName: 'testBucket',
                    objectKey: `testObj${i}`,
                    objectVersion: 'testversion',
                    accountId: '834789881858',
                    archiveInfo: {
                        archiveId: `archive-${i}`,
                        archiveVersion: 5166759712787974,
                    },
                    requestId: `req-${i}`,
                });
                messages.push({ key: `k${i}`, message: messageBody });
            }
            producer.send(messages, e2 => assert.ifError(e2));

            const checkProcessed = setInterval(() => {
                if (processedCount < N) {
                    return;
                }
                clearInterval(checkProcessed);
                setTimeout(() => {
                    consumer._consumer.committed(
                        [{ topic, partition: 0 }], 5000, (e3, c) => {
                        assert.ifError(e3);
                        const observed = c[0].offset;
                        const expected = baseline + N;
                        assert.strictEqual(observed, expected,
                            'expected committed offset to advance from ' +
                            `${baseline} to ${expected} after ${N} entries ` +
                            'failed through LifecycleColdStatusArchiveTask ' +
                            `retry exhaustion, but it stayed at ${observed}`);
                        done();
                    });
                }, 7000);
            }, 100);
        });
    });
});
