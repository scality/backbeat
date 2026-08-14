const assert = require('assert');
const async = require('async');
const { AdminClient, KafkaConsumer } = require('node-rdkafka');
const { ZenkoMetrics } = require('arsenal').metrics;

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const DeliveryWorker =
    require('../../../extensions/notification/deliveryWorker/DeliveryWorker');
const DeliveryTopicDrainer =
    require('../../../extensions/notification/deliveryWorker/DeliveryTopicDrainer');
const { buildDeliveryKey } =
    require('../../../extensions/notification/utils/deliveryKey');

const KAFKA_HOSTS = 'localhost:9092';
const CONNECT_TIMEOUT = 20000;
const METADATA_TIMEOUT = 10000;
const TIMEOUT = 60000;
const TOPIC_ALREADY_EXISTS = 36;
const BUCKET = 'poc-notification-bucket';
const CONFIG_ID = 'poc-notification-config';
// unique per run, so that a rerun never lands on the topics, the consumer
// groups or the metric labels left behind by the previous one
const RUN_ID = `${Date.now()}`;

const DELIVERED_METRIC = 's3_notification_delivery_worker_delivered_total';
const DROPPED_METRIC = 's3_notification_delivery_worker_dropped_total';

const kafkaConfig = { hosts: KAFKA_HOSTS };

/**
 * Runs fn against a connected consumer, then disconnects it
 *
 * @param {String} groupId - consumer group id, the offsets read and
 *   written by fn are those of that group
 * @param {Function} fn - fn(consumer, cb)
 * @param {Function} done - callback: done(err, result)
 * @return {undefined}
 */
function withConsumer(groupId, fn, done) {
    const consumer = new KafkaConsumer({
        'metadata.broker.list': KAFKA_HOSTS,
        'group.id': groupId,
        'enable.auto.commit': false,
        'enable.auto.offset.store': false,
    }, {});
    consumer.on('error', () => {});
    consumer.on('event.error', () => {});
    return consumer.connect({ timeout: CONNECT_TIMEOUT }, connectErr => {
        if (connectErr) {
            return consumer.disconnect(() => done(connectErr));
        }
        return fn(consumer, (err, result) =>
            consumer.disconnect(() => done(err, result)));
    });
}

function createTopics(topics, done) {
    const admin = AdminClient.create({ 'metadata.broker.list': KAFKA_HOSTS });
    return async.eachSeries(topics, (topic, next) => admin.createTopic({
        topic: topic.name,
        /* eslint-disable camelcase */
        num_partitions: topic.partitions,
        replication_factor: 1,
        /* eslint-enable camelcase */
    }, err => next(err && err.code !== TOPIC_ALREADY_EXISTS ? err : null)),
    err => {
        admin.disconnect();
        return done(err);
    });
}

/**
 * Waits until every partition of a topic has a leader, so that a producer
 * sending to it right away does not have to wait for a metadata refresh
 *
 * @param {Object[]} topics - topics, as { name, partitions }
 * @param {Function} done - callback
 * @return {undefined}
 */
function waitForTopics(topics, done) {
    const deadline = Date.now() + 20000;
    const check = () => withConsumer(`poc-meta-${RUN_ID}`, (consumer, cb) =>
        async.everySeries(topics, (topic, next) => consumer.getMetadata({
            topic: topic.name,
            timeout: METADATA_TIMEOUT,
        }, (err, metadata) => {
            if (err) {
                return next(null, false);
            }
            const found = metadata.topics.find(t => t.name === topic.name);
            return next(null, !!found &&
                found.partitions.length === topic.partitions &&
                found.partitions.every(p => p.leader >= 0));
        }), cb), (err, ready) => {
        if (!err && ready) {
            return done();
        }
        if (Date.now() >= deadline) {
            return done(err || new Error('timed out waiting for topics'));
        }
        return setTimeout(check, 500);
    });
    return check();
}

function produceRecords(topic, messages, done) {
    const producer = new BackbeatProducer({
        kafka: kafkaConfig,
        topic,
        pollIntervalMs: 100,
    });
    producer.once('error', done);
    return producer.once('ready', () => {
        producer.removeAllListeners('error');
        return producer.send(messages, sendErr =>
            producer.close(closeErr => done(sendErr || closeErr)));
    });
}

function committedOffsets(groupId, topic, partitions, done) {
    return withConsumer(groupId, (consumer, cb) => consumer.committed(
        partitions.map(partition => ({ topic, partition })),
        METADATA_TIMEOUT, (err, toppars) => {
            if (err) {
                return cb(err);
            }
            const offsets = {};
            (toppars || []).forEach(tp => {
                offsets[tp.partition] = tp.offset;
            });
            return cb(null, offsets);
        }), done);
}

/**
 * Sum of the offsets committed by a group, which is also the number of
 * records of the topic the group is done with
 *
 * @param {String} groupId - consumer group id
 * @param {String} topic - topic name
 * @param {Number} partitionCount - number of partitions of the topic
 * @param {Function} done - callback: done(err, total)
 * @return {undefined}
 */
function committedTotal(groupId, topic, partitionCount, done) {
    const partitions = [];
    for (let i = 0; i < partitionCount; i++) {
        partitions.push(i);
    }
    return committedOffsets(groupId, topic, partitions, (err, offsets) => {
        if (err) {
            return done(err);
        }
        // an unset offset is reported as a negative value
        return done(null, Object.values(offsets)
            .filter(offset => offset >= 0)
            .reduce((total, offset) => total + offset, 0));
    });
}

/**
 * Polls the committed offsets of a group until they add up to the expected
 * number of records. The consumer commits its stored offsets on a timer, so
 * a single look would only tell whether the last commit already happened.
 *
 * @param {String} groupId - consumer group id
 * @param {String} topic - topic name
 * @param {Number} partitionCount - number of partitions of the topic
 * @param {Number} expected - number of records the group has to be done with
 * @param {Number} timeoutMs - how long to wait for
 * @param {Function} done - callback
 * @return {undefined}
 */
function waitForCommittedTotal(groupId, topic, partitionCount, expected,
    timeoutMs, done) {
    const deadline = Date.now() + timeoutMs;
    let lastSeen = null;
    const check = () => committedTotal(groupId, topic, partitionCount,
        (err, total) => {
            if (!err) {
                lastSeen = total;
                if (total === expected) {
                    return done();
                }
            }
            if (Date.now() >= deadline) {
                return done(new Error(`timed out waiting for group ${groupId}` +
                    ` to commit ${expected} records, last seen ${lastSeen}`));
            }
            return setTimeout(check, 1000);
        });
    return check();
}

function commitOffsets(groupId, toppars, done) {
    return withConsumer(groupId, (consumer, cb) => {
        try {
            consumer.assign(toppars.map(tp =>
                ({ topic: tp.topic, partition: tp.partition })));
            consumer.commitSync(toppars);
        } catch (err) {
            return cb(err);
        }
        return cb();
    }, done);
}

/**
 * Reads a topic from its first offset, keeping the records it has seen so
 * far in a growing array a test can watch while a worker runs
 */
class TopicTailer {
    constructor(topic) {
        this.topic = topic;
        this.records = [];
        this._consumer = null;
        this._stopped = false;
        this._timer = null;
    }

    start(done) {
        this._consumer = new KafkaConsumer({
            'metadata.broker.list': KAFKA_HOSTS,
            'group.id': `poc-tailer-${this.topic}`,
            'enable.auto.commit': false,
            'enable.auto.offset.store': false,
        }, {});
        this._consumer.on('error', () => {});
        this._consumer.on('event.error', () => {});
        return this._consumer.connect({ timeout: CONNECT_TIMEOUT }, err => {
            if (err) {
                return done(err);
            }
            return this._consumer.getMetadata({
                topic: this.topic,
                timeout: METADATA_TIMEOUT,
            }, (mdErr, metadata) => {
                if (mdErr) {
                    return done(new Error(`metadata error: ${mdErr}`));
                }
                const found = metadata.topics.find(t => t.name === this.topic);
                if (!found) {
                    return done(new Error(`unknown topic ${this.topic}`));
                }
                this._consumer.assign(found.partitions.map(p => ({
                    topic: this.topic,
                    partition: p.id,
                    offset: 0,
                })));
                this._poll();
                return done();
            });
        });
    }

    _poll() {
        if (this._stopped) {
            return;
        }
        this._consumer.consume(100, (err, records) => {
            if (!err && records) {
                records.forEach(record => this.records.push({
                    partition: record.partition,
                    offset: record.offset,
                    key: record.key === null || record.key === undefined ?
                        null : record.key.toString(),
                    value: record.value.toString(),
                }));
            }
            if (this._stopped) {
                return;
            }
            this._timer = setTimeout(() => this._poll(), 100);
        });
    }

    stop(done) {
        this._stopped = true;
        clearTimeout(this._timer);
        this._timer = null;
        if (!this._consumer) {
            return process.nextTick(done);
        }
        return this._consumer.disconnect(() => done());
    }
}

function waitFor(what, predicate, timeoutMs, done) {
    const deadline = Date.now() + timeoutMs;
    const check = () => {
        if (predicate()) {
            return done();
        }
        if (Date.now() >= deadline) {
            return done(new Error(`timed out waiting for ${what}`));
        }
        return setTimeout(check, 100);
    };
    return check();
}

/**
 * Waits until a tailer stops seeing new records, so that a count assertion
 * cannot pass on a topic that is still receiving records
 *
 * @param {TopicTailer} tailer - tailer to watch
 * @param {Number} quietMs - how long the record count has to stay still
 * @param {Function} done - callback
 * @return {undefined}
 */
function waitUntilQuiet(tailer, quietMs, done) {
    let previous = -1;
    const check = () => {
        if (tailer.records.length === previous) {
            return done();
        }
        previous = tailer.records.length;
        return setTimeout(check, quietMs);
    };
    return check();
}

function readTopic(topic, minCount, timeoutMs, done) {
    const tailer = new TopicTailer(topic);
    return tailer.start(startErr => {
        if (startErr) {
            return done(startErr);
        }
        return async.series([
            next => waitFor(`${minCount} records on ${topic}`,
                () => tailer.records.length >= minCount, timeoutMs, next),
            next => waitUntilQuiet(tailer, 500, next),
        ], err => tailer.stop(() => done(err, tailer.records)));
    });
}

function readCounter(name, labels, done) {
    const metric = ZenkoMetrics.getMetric(name);
    if (!metric) {
        return process.nextTick(() => done(null, 0));
    }
    return metric.get().then(({ values }) => done(null, values
        .filter(v => Object.entries(labels)
            .every(([label, value]) => v.labels[label] === value))
        .reduce((total, v) => total + v.value, 0)), done);
}

function readCounterByReason(name, labels, done) {
    const metric = ZenkoMetrics.getMetric(name);
    if (!metric) {
        return process.nextTick(() => done(null, {}));
    }
    return metric.get().then(({ values }) => {
        const byReason = {};
        values
            .filter(v => Object.entries(labels)
                .every(([label, value]) => v.labels[label] === value))
            .forEach(v => {
                byReason[v.labels.reason] =
                    (byReason[v.labels.reason] || 0) + v.value;
            });
        return done(null, byReason);
    }, done);
}

/**
 * Polls a counter until it reaches the expected value
 *
 * @param {String} name - metric name
 * @param {Object} labels - labels the counter has to match
 * @param {Number} expected - value to wait for
 * @param {Number} timeoutMs - how long to wait for
 * @param {Function} done - callback
 * @return {undefined}
 */
function waitForCounter(name, labels, expected, timeoutMs, done) {
    const deadline = Date.now() + timeoutMs;
    let lastSeen = 0;
    const check = () => readCounter(name, labels, (err, value) => {
        if (!err) {
            lastSeen = value;
            if (value >= expected) {
                return done();
            }
        }
        if (Date.now() >= deadline) {
            return done(new Error(`timed out waiting for ${name} to reach ` +
                `${expected}, last seen ${lastSeen}`));
        }
        return setTimeout(check, 500);
    });
    return check();
}

/**
 * Builds a destination configuration pointing at a local topic.
 *
 * pollIntervalMs is not part of the destination schema, but the pool hands
 * it to the producer: without it every delivery report waits for the two
 * second default poll interval, which the tests cannot afford.
 *
 * @param {Object} params - resource, topic, host, port and spreadFactor
 * @return {Object} destination configuration
 */
function destinationConfig(params) {
    return {
        resource: params.resource,
        type: 'kafka',
        host: params.host || 'localhost',
        port: params.port || 9092,
        topic: params.topic,
        auth: {},
        spreadFactor: params.spreadFactor || 1,
        pollIntervalMs: 100,
    };
}

function deliveryPoolConfig(params) {
    return {
        enabled: true,
        topic: params.topic,
        groupId: params.groupId,
        deliveryTimeoutMs: params.deliveryTimeoutMs || 30000,
        producerIdleMs: 300000,
        maxProducers: 50,
        concurrency: params.concurrency,
        maxQueued: 1000,
    };
}

/**
 * Builds the record the populator publishes on the delivery topic for one
 * event: the notification payload, addressed with the destination and the
 * matching configuration id, under the shared delivery key
 *
 * @param {Object} params - destination, key, eventType and dateTime
 * @return {Object} kafka message, as { key, message }
 */
function addressedRecord(params) {
    const { destination, key, eventType, dateTime } = params;
    const message = {
        bucket: BUCKET,
        key,
        eventType,
        dateTime,
        versionId: null,
        size: '1024',
        region: 'us-east-1',
        schemaVersion: '5',
        destinationId: destination.resource,
        configurationId: CONFIG_ID,
    };
    return {
        // the populator publishes through QueuePopulatorExtension.publish(),
        // which url-encodes the record key
        key: encodeURIComponent(buildDeliveryKey(destination, BUCKET, key)),
        message: JSON.stringify(message),
    };
}

function legacyRecord(params) {
    const { key, eventType, dateTime } = params;
    return {
        key,
        message: JSON.stringify({
            bucket: BUCKET,
            key,
            eventType,
            dateTime,
            versionId: null,
            size: '1024',
            region: 'us-east-1',
            schemaVersion: '5',
        }),
    };
}

/**
 * Reads back one record delivered to an external destination topic
 *
 * @param {Object} record - record read from the destination topic
 * @return {Object} the fields of the S3 event the assertions look at
 */
function deliveredEvent(record) {
    const parsed = JSON.parse(record.value);
    assert.strictEqual(parsed.Records.length, 1,
        'a delivered message holds exactly one event');
    const [event] = parsed.Records;
    return {
        recordKey: record.key,
        partition: record.partition,
        offset: record.offset,
        bucket: event.s3.bucket.name,
        key: event.s3.object.key,
        eventName: event.eventName,
        eventTime: event.eventTime,
        configurationId: event.s3.configurationId,
    };
}

function uniqueObjectKeys(records) {
    return new Set(records.map(record => deliveredEvent(record).key));
}

function eventTime(index) {
    return new Date(Date.UTC(2026, 0, 1, 0, 0, index)).toISOString();
}

describe('notification delivery worker :: delivery to destinations',
function deliveryToDestinations() {
    this.timeout(TIMEOUT);

    const deliveryTopic = `poc-bn-delivery-${RUN_ID}`;
    const customerTopicA = `poc-bn-customer-a-${RUN_ID}`;
    const customerTopicB = `poc-bn-customer-b-${RUN_ID}`;
    const groupId = `poc-bn-delivery-group-${RUN_ID}`;
    const deliveryPartitions = 3;
    // three objects, each with the same put, put then delete sequence
    const objectKeys = ['object-alpha', 'object-beta', 'object-gamma'];
    const eventTypes = [
        's3:ObjectCreated:Put',
        's3:ObjectCreated:Put',
        's3:ObjectRemoved:Delete',
    ];
    const destinationA = destinationConfig({
        resource: `poc-dest-a-${RUN_ID}`,
        topic: customerTopicA,
        // spread over three delivery topic keys, so that the destination is
        // not pinned to a single partition and a single worker
        spreadFactor: 3,
    });
    const destinationB = destinationConfig({
        resource: `poc-dest-b-${RUN_ID}`,
        topic: customerTopicB,
    });
    const notifConfig = {
        destinations: [destinationA, destinationB],
        deliveryPool: deliveryPoolConfig({
            topic: deliveryTopic,
            groupId,
            concurrency: 10,
        }),
    };
    const expectedPerDestination = objectKeys.length * eventTypes.length;
    const totalRecords = expectedPerDestination * 2;

    let worker = null;
    let tailerA = null;
    let tailerB = null;

    before(done => {
        const topics = [
            { name: deliveryTopic, partitions: deliveryPartitions },
            { name: customerTopicA, partitions: 3 },
            { name: customerTopicB, partitions: 1 },
        ];
        // events of one object are produced in order, objects are
        // interleaved, so that the worker sees keys it has to keep apart
        const records = [];
        eventTypes.forEach((eventType, eventIndex) => {
            objectKeys.forEach(key => {
                [destinationA, destinationB].forEach(destination => {
                    records.push(addressedRecord({
                        destination,
                        key,
                        eventType,
                        dateTime: eventTime(eventIndex),
                    }));
                });
            });
        });
        return async.series([
            next => createTopics(topics, next),
            next => waitForTopics(topics, next),
            // produced before the worker exists: a worker joining with a
            // fresh group only sees them because it reads from the earliest
            // offset
            next => produceRecords(deliveryTopic, records, next),
            next => {
                tailerA = new TopicTailer(customerTopicA);
                return tailerA.start(next);
            },
            next => {
                tailerB = new TopicTailer(customerTopicB);
                return tailerB.start(next);
            },
            next => {
                worker = new DeliveryWorker(kafkaConfig, notifConfig);
                return worker.start(null, next);
            },
        ], done);
    });

    after(done => async.series([
        next => (worker ? worker.stop(() => next()) : next()),
        next => (tailerA ? tailerA.stop(next) : next()),
        next => (tailerB ? tailerB.stop(next) : next()),
    ], done));

    it('should deliver every record to its own destination topic, keyed by ' +
    'bucket and object key', done => async.series([
        next => waitFor('every record to reach its destination topic',
            () => tailerA.records.length >= expectedPerDestination &&
                tailerB.records.length >= expectedPerDestination,
            30000, next),
        next => waitUntilQuiet(tailerA, 500, next),
        next => waitUntilQuiet(tailerB, 500, next),
    ], err => {
        assert.ifError(err);
        [tailerA, tailerB].forEach(tailer => {
            assert.strictEqual(tailer.records.length, expectedPerDestination,
                `${tailer.topic} received an unexpected number of records`);
            tailer.records.map(deliveredEvent).forEach(event => {
                assert.strictEqual(event.recordKey, `${BUCKET}/${event.key}`);
                assert.strictEqual(event.bucket, BUCKET);
                assert.strictEqual(event.configurationId, CONFIG_ID);
                assert(objectKeys.includes(event.key),
                    `unexpected object key ${event.key}`);
            });
        });
        return done();
    }));

    it('should keep the events of one object in the order they were ' +
    'published', done => {
        [tailerA, tailerB].forEach(tailer => {
            const byObject = new Map();
            tailer.records.map(deliveredEvent).forEach(event => {
                if (!byObject.has(event.key)) {
                    byObject.set(event.key, []);
                }
                byObject.get(event.key).push(event);
            });
            assert.strictEqual(byObject.size, objectKeys.length,
                `${tailer.topic} did not receive every object`);
            byObject.forEach((events, key) => {
                // every event of one object carries the same record key, so
                // they all sit on one partition and their offsets order them
                const partitions = new Set(events.map(e => e.partition));
                assert.strictEqual(partitions.size, 1,
                    `events of ${key} were spread over several partitions`);
                const ordered = events.slice()
                    .sort((a, b) => a.offset - b.offset);
                assert.deepStrictEqual(ordered.map(e => e.eventName),
                    eventTypes, `events of ${key} were delivered out of order`);
                assert.deepStrictEqual(ordered.map(e => e.eventTime),
                    eventTypes.map((_, index) => eventTime(index)),
                    `events of ${key} were delivered out of order`);
            });
        });
        return done();
    });

    it('should count one delivery per destination and no drop', done =>
        async.series([
            next => readCounter(DELIVERED_METRIC,
                { target: destinationA.resource }, (err, value) => {
                    assert.ifError(err);
                    assert.strictEqual(value, expectedPerDestination);
                    return next();
                }),
            next => readCounter(DELIVERED_METRIC,
                { target: destinationB.resource }, (err, value) => {
                    assert.ifError(err);
                    assert.strictEqual(value, expectedPerDestination);
                    return next();
                }),
            next => async.eachSeries([destinationA, destinationB],
                (destination, destDone) => readCounter(DROPPED_METRIC,
                    { target: destination.resource }, (err, value) => {
                        assert.ifError(err);
                        assert.strictEqual(value, 0,
                            `${destination.resource} dropped a record`);
                        return destDone();
                    }), next),
        ], done));

    it('should advance the committed offsets of the delivery group past ' +
    'every record', done => waitForCommittedTotal(groupId, deliveryTopic,
        deliveryPartitions, totalRecords, 30000, err => {
            assert.ifError(err);
            return done();
        }));
});

describe('notification delivery worker :: at least once across a restart',
function atLeastOnceAcrossRestart() {
    this.timeout(TIMEOUT);

    const deliveryTopic = `poc-bn-restart-delivery-${RUN_ID}`;
    const customerTopic = `poc-bn-restart-customer-${RUN_ID}`;
    const groupId = `poc-bn-restart-group-${RUN_ID}`;
    const recordCount = 40;
    const destination = destinationConfig({
        resource: `poc-dest-restart-${RUN_ID}`,
        topic: customerTopic,
    });
    const notifConfig = {
        destinations: [destination],
        deliveryPool: deliveryPoolConfig({
            topic: deliveryTopic,
            groupId,
            // one delivery at a time, so that the first worker can be stopped
            // while it still has records left to deliver
            concurrency: 1,
        }),
    };
    const objectKeys = [];
    for (let i = 0; i < recordCount; i++) {
        objectKeys.push(`restart-object-${`${i}`.padStart(3, '0')}`);
    }

    let firstWorker = null;
    let secondWorker = null;
    let tailer = null;

    before(done => {
        const topics = [
            { name: deliveryTopic, partitions: 1 },
            { name: customerTopic, partitions: 1 },
        ];
        const records = objectKeys.map((key, index) => addressedRecord({
            destination,
            key,
            eventType: 's3:ObjectCreated:Put',
            dateTime: eventTime(index),
        }));
        return async.series([
            next => createTopics(topics, next),
            next => waitForTopics(topics, next),
            next => produceRecords(deliveryTopic, records, next),
            next => {
                tailer = new TopicTailer(customerTopic);
                return tailer.start(next);
            },
        ], done);
    });

    after(done => async.series([
        next => (firstWorker ? firstWorker.stop(() => next()) : next()),
        next => (secondWorker ? secondWorker.stop(() => next()) : next()),
        next => (tailer ? tailer.stop(next) : next()),
    ], done));

    it('should let a second worker finish what the first one did not ' +
    'deliver, without leaving a gap', done => {
        let deliveredByFirst = 0;
        return async.series([
            next => {
                firstWorker = new DeliveryWorker(kafkaConfig, notifConfig);
                return firstWorker.start(null, next);
            },
            next => waitFor('the first deliveries',
                () => tailer.records.length >= 3, 30000, next),
            // a graceful stop drains the delivery in flight and commits what
            // it finished, the rest is the second worker's problem
            next => firstWorker.stop(() => next()),
            next => waitUntilQuiet(tailer, 500, next),
            next => {
                deliveredByFirst = uniqueObjectKeys(tailer.records).size;
                assert(deliveredByFirst < recordCount,
                    'the first worker delivered every record, so the restart ' +
                    'proves nothing');
                return next();
            },
            next => {
                secondWorker = new DeliveryWorker(kafkaConfig, notifConfig);
                return secondWorker.start(null, next);
            },
            next => waitFor('the second worker to deliver the rest',
                () => uniqueObjectKeys(tailer.records).size === recordCount,
                40000, next),
        ], err => {
            assert.ifError(err);
            const delivered = uniqueObjectKeys(tailer.records);
            objectKeys.forEach(key => assert(delivered.has(key),
                `${key} never reached the destination`));
            // at least once: a record delivered twice is fine, a record
            // never delivered is not
            assert(tailer.records.length >= recordCount);
            assert(deliveredByFirst > 0,
                'the first worker delivered nothing, so nothing was resumed');
            return done();
        });
    });

    it('should end up with every record committed', done =>
        waitForCommittedTotal(groupId, deliveryTopic, 1, recordCount, 30000,
            err => {
                assert.ifError(err);
                return done();
            }));
});

describe('notification delivery worker :: unreachable destination',
function unreachableDestination() {
    // a producer to a destination that is not listening only fails once the
    // node-rdkafka connect timeout expires, thirty seconds after the first
    // record for that destination, so this suite needs a longer budget than
    // the others
    this.timeout(120000);

    const deliveryTopic = `poc-bn-drop-delivery-${RUN_ID}`;
    const customerTopic = `poc-bn-drop-customer-${RUN_ID}`;
    const groupId = `poc-bn-drop-group-${RUN_ID}`;
    const healthyCount = 6;
    const deadCount = 3;
    const healthyDestination = destinationConfig({
        resource: `poc-dest-healthy-${RUN_ID}`,
        topic: customerTopic,
    });
    const deadDestination = destinationConfig({
        resource: `poc-dest-dead-${RUN_ID}`,
        // nothing is listening there
        host: '127.0.0.1',
        port: 9099,
        topic: 'poc-unreachable-topic',
    });
    const notifConfig = {
        destinations: [healthyDestination, deadDestination],
        deliveryPool: deliveryPoolConfig({
            topic: deliveryTopic,
            groupId,
            // the joi minimum: a record that cannot be delivered expires
            // instead of holding its offset forever
            deliveryTimeoutMs: 6000,
            // enough lanes that the dead destination cannot starve the
            // healthy one
            concurrency: 10,
        }),
    };

    let worker = null;
    let tailer = null;

    before(done => {
        const topics = [
            { name: deliveryTopic, partitions: 1 },
            { name: customerTopic, partitions: 1 },
        ];
        const records = [];
        for (let i = 0; i < Math.max(healthyCount, deadCount); i++) {
            if (i < healthyCount) {
                records.push(addressedRecord({
                    destination: healthyDestination,
                    key: `healthy-object-${i}`,
                    eventType: 's3:ObjectCreated:Put',
                    dateTime: eventTime(i),
                }));
            }
            if (i < deadCount) {
                records.push(addressedRecord({
                    destination: deadDestination,
                    key: `dead-object-${i}`,
                    eventType: 's3:ObjectCreated:Put',
                    dateTime: eventTime(i),
                }));
            }
        }
        return async.series([
            next => createTopics(topics, next),
            next => waitForTopics(topics, next),
            next => produceRecords(deliveryTopic, records, next),
            next => {
                tailer = new TopicTailer(customerTopic);
                return tailer.start(next);
            },
            next => {
                worker = new DeliveryWorker(kafkaConfig, notifConfig);
                return worker.start(null, next);
            },
        ], done);
    });

    after(done => async.series([
        next => (worker ? worker.stop(() => next()) : next()),
        next => (tailer ? tailer.stop(next) : next()),
    ], done));

    it('should keep delivering to the healthy destination while the other ' +
    'one is unreachable', done => async.series([
        next => waitFor('the healthy destination to receive every record',
            () => tailer.records.length >= healthyCount, 30000, next),
        next => waitUntilQuiet(tailer, 500, next),
    ], err => {
        assert.ifError(err);
        assert.strictEqual(tailer.records.length, healthyCount);
        tailer.records.map(deliveredEvent).forEach(event =>
            assert(event.key.startsWith('healthy-object-'),
                `unexpected record ${event.key} on the healthy destination`));
        return done();
    }));

    it('should count the undeliverable records as dropped', done =>
        async.series([
            next => waitForCounter(DROPPED_METRIC,
                { target: deadDestination.resource }, deadCount, 90000, next),
            next => readCounterByReason(DROPPED_METRIC,
                { target: deadDestination.resource }, (err, byReason) => {
                    assert.ifError(err);
                    const reasons = Object.keys(byReason);
                    reasons.forEach(reason => assert(
                        ['producer_error', 'delivery_error', 'delivery_timeout']
                            .includes(reason),
                        `unexpected drop reason ${reason}`));
                    const total = reasons.reduce(
                        (sum, reason) => sum + byReason[reason], 0);
                    assert.strictEqual(total, deadCount);
                    return next();
                }),
            next => readCounter(DELIVERED_METRIC,
                { target: deadDestination.resource }, (err, value) => {
                    assert.ifError(err);
                    assert.strictEqual(value, 0,
                        'the unreachable destination cannot have delivered');
                    return next();
                }),
            next => readCounter(DROPPED_METRIC,
                { target: healthyDestination.resource }, (err, value) => {
                    assert.ifError(err);
                    assert.strictEqual(value, 0,
                        'the healthy destination dropped a record');
                    return next();
                }),
        ], done));

    it('should commit past the dropped records', done =>
        waitForCommittedTotal(groupId, deliveryTopic, 1,
            healthyCount + deadCount, 30000, err => {
                assert.ifError(err);
                return done();
            }));
});

describe('notification delivery replay :: draining an old internal topic',
function drainOldInternalTopic() {
    this.timeout(TIMEOUT);

    const oldTopic = `poc-bn-internal-${RUN_ID}`;
    const deliveryTopic = `poc-bn-replay-delivery-${RUN_ID}`;
    const deliveryPartitions = 2;
    const processorGroupId = `poc-bn-processor-${RUN_ID}`;
    const destination = destinationConfig({
        resource: `poc-dest-replay-${RUN_ID}`,
        topic: `poc-bn-replay-customer-${RUN_ID}`,
        // two record keys, so the replayed records land on both partitions
        // of the delivery topic
        spreadFactor: 2,
    });
    destination.internalTopic = oldTopic;
    const oldGroupId = `${processorGroupId}-${destination.resource}`;
    const oldPartitions = 2;
    const legacyCount = 12;
    const notifConfig = {
        topic: oldTopic,
        queueProcessor: { groupId: processorGroupId, concurrency: 10 },
        deliveryPool: deliveryPoolConfig({
            topic: deliveryTopic,
            groupId: `poc-bn-replay-group-${RUN_ID}`,
            concurrency: 10,
        }),
        destinations: [destination],
    };
    // only creations are configured for this destination, so the removal
    // events of the old topic have nothing to deliver and have to be
    // skipped rather than replayed
    const bnConfigManager = {
        getConfig: (bucket, cb) => process.nextTick(() => cb(null, {
            bucket,
            notificationConfiguration: {
                queueConfig: [{
                    id: CONFIG_ID,
                    events: ['s3:ObjectCreated:*'],
                    queueArn: `arn:scality:bucketnotif:::${destination.resource}`,
                    filterRules: [],
                }],
            },
        })),
    };

    // what the old consumer group had not processed yet, and what of that is
    // still deliverable, both resolved from the layout the broker chose
    const seededOffsets = [];
    let expectedDrained = 0;
    let expectedKeys = new Set();

    function runDrainer(replayId, done) {
        const drainer = new DeliveryTopicDrainer({
            kafkaConfig,
            notifConfig,
            bnConfigManager,
            replayId,
            batchSize: 10,
            emptyBatchSleepMs: 100,
            maxEmptyBatches: 30,
        });
        return async.waterfall([
            next => drainer.start(startErr => next(startErr)),
            next => drainer.run(next),
        ], (err, totals) => drainer.stop(() => done(err, totals)));
    }

    before(done => {
        const topics = [
            { name: oldTopic, partitions: oldPartitions },
            { name: deliveryTopic, partitions: deliveryPartitions },
        ];
        const records = [];
        for (let i = 0; i < legacyCount; i++) {
            records.push(legacyRecord({
                key: `replay-object-${`${i}`.padStart(3, '0')}`,
                // every third record is a removal, which this destination is
                // not configured for
                eventType: i % 3 === 2 ?
                    's3:ObjectRemoved:Delete' : 's3:ObjectCreated:Put',
                dateTime: eventTime(i),
            }));
        }
        return async.waterfall([
            next => createTopics(topics, next),
            next => waitForTopics(topics, next),
            next => produceRecords(oldTopic, records, next),
            next => readTopic(oldTopic, legacyCount, 30000, next),
            (written, next) => {
                const byPartition = new Map();
                written.forEach(record => {
                    if (!byPartition.has(record.partition)) {
                        byPartition.set(record.partition, []);
                    }
                    byPartition.get(record.partition).push(record);
                });
                const notProcessed = [];
                byPartition.forEach((partitionRecords, partition) => {
                    partitionRecords.sort((a, b) => a.offset - b.offset);
                    // pretend the old queue processor stopped halfway
                    const index = Math.floor(partitionRecords.length / 2);
                    seededOffsets.push({
                        topic: oldTopic,
                        partition,
                        offset: partitionRecords[index].offset,
                    });
                    partitionRecords.slice(index)
                        .forEach(record => notProcessed.push(record));
                });
                expectedDrained = notProcessed.length;
                expectedKeys = new Set(notProcessed
                    .map(record => JSON.parse(record.value))
                    .filter(entry => entry.eventType.startsWith('s3:ObjectCreated'))
                    .map(entry => entry.key));
                assert(expectedDrained > 0 && expectedKeys.size > 0,
                    'the drain plan has to leave something to replay');
                return next();
            },
            // a throwaway consumer that commits where the old queue processor
            // would have stopped, then leaves
            next => commitOffsets(oldGroupId, seededOffsets, next),
        ], done);
    });

    it('should replay exactly the records the old group had not processed',
    done => runDrainer(`first-${RUN_ID}`, (err, totals) => {
        assert.ifError(err);
        assert.strictEqual(totals.drained, expectedDrained);
        assert.strictEqual(totals.produced, expectedKeys.size);
        assert.strictEqual(totals.skipped, expectedDrained - expectedKeys.size);
        return readTopic(deliveryTopic, expectedKeys.size, 30000,
            (readErr, replayed) => {
                assert.ifError(readErr);
                assert.strictEqual(replayed.length, expectedKeys.size);
                const keys = new Set();
                replayed.forEach(record => {
                    const entry = JSON.parse(record.value);
                    assert.strictEqual(entry.destinationId, destination.resource);
                    assert.strictEqual(entry.configurationId, CONFIG_ID);
                    assert.strictEqual(entry.bucket, BUCKET);
                    assert.strictEqual(record.key, encodeURIComponent(
                        buildDeliveryKey(destination, BUCKET, entry.key)),
                    'the replayed record key has to match what the populator ' +
                    'would have published');
                    keys.add(entry.key);
                });
                assert.deepStrictEqual(keys, expectedKeys);
                return done();
            });
    }));

    it('should produce duplicates and no gap when it is run again', done =>
        runDrainer(`second-${RUN_ID}`, (err, totals) => {
            assert.ifError(err);
            assert.strictEqual(totals.drained, expectedDrained);
            assert.strictEqual(totals.produced, expectedKeys.size);
            return readTopic(deliveryTopic, expectedKeys.size * 2, 30000,
                (readErr, replayed) => {
                    assert.ifError(readErr);
                    assert.strictEqual(replayed.length, expectedKeys.size * 2,
                        'a rerun replays the same records once more');
                    const keys = new Set(replayed
                        .map(record => JSON.parse(record.value).key));
                    assert.deepStrictEqual(keys, expectedKeys,
                        'a rerun must not replay anything else');
                    return done();
                });
        }));

    it('should leave the offsets of the old consumer group untouched', done =>
        committedOffsets(oldGroupId, oldTopic,
            seededOffsets.map(tp => tp.partition), (err, offsets) => {
                assert.ifError(err);
                seededOffsets.forEach(tp => assert.strictEqual(
                    offsets[tp.partition], tp.offset,
                    `the replay moved the old group on partition ${tp.partition}`));
                return done();
            }));
});
