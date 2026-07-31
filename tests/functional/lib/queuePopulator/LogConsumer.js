const assert = require('assert');
const async = require('async');
const werelogs = require('werelogs');

const BackbeatProducer = require('../../../../lib/BackbeatProducer');
const LogConsumer =
    require('../../../../lib/queuePopulator/KafkaLogConsumer/LogConsumer');

const producerKafkaConf = {
    hosts: process.env.KAFKA_TEST_HOSTS || 'localhost:9092',
};
const log = new werelogs.Logger('LogConsumer:test');

const changeStreamDocument = {
    ns: {
        db: 'metadata',
        coll: 'example-bucket',
    },
    documentKey: {
        _id: 'example-key',
    },
    operationType: 'insert',
    clusterTime: {
        $timestamp: {
            t: 1701270357,
            i: 1,
        },
    },
    fullDocument: {
        value: {
            field: 'value',
        },
    },
};

const messages = [
    { key: 'foo', message: JSON.stringify(changeStreamDocument) },
    { key: 'bar', message: JSON.stringify(changeStreamDocument) },
    { key: 'qux', message: JSON.stringify(changeStreamDocument) },
];

describe('LogConsumer rebalance tests', () => {
    const topic = 'backbeat-log-consumer-spec-rebalance';
    let groupId;
    let producer;
    let consumer;
    let consumer2;
    let pollers = [];
    // last offsets stored by the auto-storing poller, per partition
    let lastStored;

    // group joins and rebalance callbacks are only serviced during
    // consume() polls: emulate the LogReader's batch loop, including
    // storing the offsets once a batch is processed (autoStore)
    function startPolling(logConsumer, autoStore) {
        const timer = setInterval(() => {
            logConsumer.readRecords({ limit: messages.length }, (err, res) => {
                if (res && res.log) {
                    res.log.on('data', () => {});
                    res.log.on('error', () => {});
                }
                if (autoStore && logConsumer._hasUnprocessedMessages()) {
                    logConsumer._topicPartition.forEach(tp => {
                        lastStored.set(tp.partition, { ...tp });
                    });
                    logConsumer.storeOffsets();
                }
            });
        }, 300);
        pollers.push(timer);
    }

    function waitFor(predicate, timeoutMs, description, cb) {
        const deadline = Date.now() + timeoutMs;
        const check = () => {
            if (predicate()) {
                return cb();
            }
            if (Date.now() > deadline) {
                return cb(new Error(`timed out waiting for ${description}`));
            }
            return setTimeout(check, 200);
        };
        check();
    }

    beforeEach(function beforeEach(done) {
        this.timeout(60000);
        groupId = `log-consumer-group-${Math.random()}`;
        consumer2 = null;
        lastStored = new Map();
        producer = new BackbeatProducer({
            kafka: producerKafkaConf, topic,
            pollIntervalMs: 100,
            compressionType: 'none',
        });
        async.series([
            next => producer.on('ready', next),
            next => producer.send(messages, next),
        ], done);
    });

    afterEach(function afterEach(done) {
        this.timeout(60000);
        // keep the pollers running while closing: the leave-group
        // rebalance is only delivered through consume() polls
        async.parallel([
            next => producer.close(next),
            next => (consumer ? consumer.close(next) : next()),
            next => (consumer2 ? consumer2.close(next) : next()),
        ], () => {
            pollers.forEach(clearInterval);
            pollers = [];
            consumer = null;
            consumer2 = null;
            done();
        });
    });

    function setupConsumer(autoStore, cb) {
        consumer = new LogConsumer({
            hosts: producerKafkaConf.hosts,
            topic,
            consumerGroupId: groupId,
            maxPollIntervalMs: 45000,
        }, log);
        async.series([
            next => consumer.setup(next),
            next => {
                startPolling(consumer, autoStore);
                if (autoStore) {
                    return waitFor(() => lastStored.size > 0, 30000,
                        'a batch to be consumed and stored', next);
                }
                return waitFor(() => consumer._hasUnprocessedMessages(),
                    30000, 'a batch to be consumed', next);
            },
        ], cb);
    }

    it('should commit stored offsets, release partitions promptly on ' +
    'rebalance and keep consuming', function testPromptRelease(done) {
        this.timeout(60000);
        async.series([
            next => setupConsumer(true, next),
            next => {
                // pre-fix, the revoke below would stall for
                // maxPollIntervalMs - 1000 = 44s waiting for a commit
                // callback that librdkafka only delivers after
                // unassign: the 15s cap proves the drain no longer
                // gates on it
                const unassignDeadline = setTimeout(() => {
                    next(new Error('partitions were not released within ' +
                        '15s of the rebalance'));
                }, 15000);
                consumer.once('unassigned', () => {
                    clearTimeout(unassignDeadline);
                    next();
                });
                consumer2 = new LogConsumer({
                    hosts: producerKafkaConf.hosts,
                    topic,
                    consumerGroupId: groupId,
                }, log);
                consumer2.setup(() => startPolling(consumer2, true));
            },
            next => {
                assert.strictEqual(consumer._consumer.isConnected(), true,
                    'consumer should stay connected after releasing ' +
                    'partitions');
                consumer._consumer.committed(
                    [...lastStored.values()].map(tp =>
                        ({ topic: tp.topic, partition: tp.partition })),
                    10000, (err, committed) => {
                        if (err) {
                            return next(err);
                        }
                        lastStored.forEach(stored => {
                            const part = committed.find(
                                c => c.partition === stored.partition);
                            assert(part, 'no committed offset for ' +
                                `partition ${stored.partition}`);
                            assert.strictEqual(part.offset, stored.offset,
                                'committed offset should match the last ' +
                                'stored offset after the rebalance');
                        });
                        return next();
                    });
            },
            next => {
                // no zombie: once the second consumer leaves, the
                // first must take the partitions back and consume again
                consumer2.close(() => {
                    consumer2 = null;
                    waitFor(() => consumer._consumer.assignments().length > 0,
                        30000, 'partitions to be re-assigned', next);
                });
            },
            next => {
                const storedBefore = [...lastStored.values()]
                    .reduce((sum, tp) => sum + tp.offset, 0);
                producer.send([messages[0]], err => {
                    if (err) {
                        return next(err);
                    }
                    return waitFor(() => [...lastStored.values()]
                        .reduce((sum, tp) => sum + tp.offset, 0)
                            > storedBefore,
                        30000, 'a batch to be consumed after the rebalance',
                        next);
                });
            },
        ], done);
    });

    it('should time out and disconnect when the in-flight batch never ' +
    'completes', function testDrainTimeout(done) {
        this.timeout(150000);
        // LogConsumer pauses its polls while a batch is in flight, so
        // a broker revoke normally only reaches it through the very
        // poll that captured the batch. Reproduce that timing by
        // polling the raw consumer (the genuine librdkafka delivery
        // path, bypassing only the batch guard) while the batch is
        // never stored: the drain can then only end by timeout.
        // maxPollIntervalMs cannot go below session.timeout.ms (45s),
        // so the drain times out at the 30s DRAIN_TIMEOUT_MS cap.
        let revokedAt;
        async.series([
            next => setupConsumer(false, next),
            next => {
                const rawPoller = setInterval(
                    () => consumer._consumer.consume(1, () => {}), 300);
                pollers.push(rawPoller);
                revokedAt = Date.now();
                const unassignDeadline = setTimeout(() => {
                    next(new Error('drain timeout did not fire'));
                }, 50000);
                consumer.once('unassigned', () => {
                    clearTimeout(unassignDeadline);
                    next();
                });
                consumer2 = new LogConsumer({
                    hosts: producerKafkaConf.hosts,
                    topic,
                    consumerGroupId: groupId,
                }, log);
                consumer2.setup(() => startPolling(consumer2, true));
            },
            next => {
                const elapsed = Date.now() - revokedAt;
                assert(elapsed >= 25000,
                    `unassign fired after ${elapsed}ms, expected the 30s ` +
                    'drain timeout');
                // completing the batch after the forced release must be
                // harmless (offsets skipped, batch redelivered), and it
                // lets the disconnect's own final revoke drain instantly
                // instead of waiting out a second timeout
                consumer.storeOffsets();
                // the timeout path deliberately disconnects so the
                // healthcheck fails and the process gets restarted
                waitFor(() => !consumer._consumer.isConnected(), 60000,
                    'the consumer to disconnect', next);
            },
        ], done);
    });
});
