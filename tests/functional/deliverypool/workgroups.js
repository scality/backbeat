const assert = require('assert');
const async = require('async');
const os = require('os');
const path = require('path');
const { AdminClient, KafkaConsumer } = require('node-rdkafka');
const { ZenkoMetrics } = require('arsenal').metrics;

const werelogs = require('werelogs');

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const ZookeeperManager = require('../../../lib/clients/ZookeeperManager');
const DeliveryWorker =
    require('../../../extensions/notification/deliveryWorker/DeliveryWorker');
const WorkgroupConfigLoader =
    require('../../../extensions/notification/deliveryWorker/WorkgroupConfigLoader');
const { buildDeliveryKey } =
    require('../../../extensions/notification/utils/deliveryKey');
const {
    buildGroupId,
    createSliceFilter,
    validateWorkgroupsDoc,
    workgroupIdForDestination,
    CONFIG_VERSION,
} = require('../../../extensions/notification/utils/workgroups');

const KAFKA_HOSTS = 'localhost:9092';
// bare connection string, with no chroot: the workgroups path is a path on
// the client, never appended to the connection string
const ZOOKEEPER_HOSTS = 'localhost:2181';
const CONNECT_TIMEOUT = 20000;
const STOP_TIMEOUT = 20000;
const METADATA_TIMEOUT = 10000;
const TOPIC_ALREADY_EXISTS = 36;
const BUCKET = 'poc-workgroups-bucket';
const CONFIG_ID = 'poc-workgroups-config';
// unique per run, so that a rerun never lands on the topics, the consumer
// groups, the zookeeper nodes or the metric labels of the previous one
const RUN_ID = `${Date.now()}`;

const ZK_BASE = `/poc-workgroups-${RUN_ID}`;

const DELIVERED_METRIC = 's3_notification_delivery_worker_delivered_total';
const DROPPED_METRIC = 's3_notification_delivery_worker_dropped_total';
const SKIPPED_METRIC = 's3_notification_delivery_worker_skipped_total';

const kafkaConfig = { hosts: KAFKA_HOSTS };

// how long a freshly created topic is given to become visible cluster wide,
// and how many consecutive clean looks at it are needed
const TOPIC_PROPAGATION_TIMEOUT = 60000;
const TOPIC_PROPAGATION_POLL_MS = 1000;
const STABLE_METADATA_CHECKS = 3;

const suiteLog = new werelogs.Logger('WorkgroupsFunctionalSuite');

// every observation a gate makes, so the run output carries the numbers the
// write up quotes rather than only pass or fail
const OBSERVED = {};

// every time a gate had to be run a second time because its workers consumed
// nothing at all, which is the pre-existing wedge of
// design/06-backbeatconsumer-wedge.md and not a workgroups failure
const WEDGES = [];

/**
 * Records a number a gate observed, and puts it in the run output
 *
 * @param {String} name - observation name
 * @param {*} value - observed value
 * @return {*} the value, so a caller can record and use it in one expression
 */
function record(name, value) {
    OBSERVED[name] = value;
    suiteLog.info('workgroups gate observation', { observation: name, value });
    return value;
}

const A_CUSTOMER_COUNT = 6;

/**
 * Every topic of the run, created once before anything consumes. A broker
 * still answering "unknown topic" for a topic that was just created makes
 * librdkafka drop it from the subscription, which is why deployments
 * pre-create the delivery topic before any worker starts. This suite mirrors
 * the existing delivery pool suite rather than racing topic creation.
 */
const TOPICS = {
    aDelivery: { name: `poc-wg-a-delivery-${RUN_ID}`, partitions: 12 },
    aWhaleCustomer: { name: `poc-wg-a-whale-customer-${RUN_ID}`,
        partitions: 1 },
};
for (let i = 0; i < A_CUSTOMER_COUNT; i++) {
    TOPICS[`aCustomer${i}`] = {
        name: `poc-wg-a-customer-${i}-${RUN_ID}`,
        partitions: 1,
    };
}

/**
 * Runs fn against a connected consumer, then disconnects it
 *
 * @param {String} groupId - consumer group id, the offsets read by fn are
 *   those of that group
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

/**
 * Runs a kafka call, handing a synchronous throw to the callback instead of
 * letting it escape into whichever test happens to be running
 *
 * @param {Function} cb - callback to fail
 * @param {Function} fn - call to make
 * @return {undefined}
 */
function callOrFail(cb, fn) {
    try {
        return fn();
    } catch (err) {
        return cb(err);
    }
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
 * Waits until every topic is visible cluster wide with a leader on each of
 * its partitions, several looks in a row.
 *
 * @param {Object[]} topics - topics, as { name, partitions }
 * @param {Function} done - callback
 * @return {undefined}
 */
function waitForTopics(topics, done) {
    return withConsumer(`poc-wg-meta-${RUN_ID}`, (consumer, cb) => {
        const deadline = Date.now() + TOPIC_PROPAGATION_TIMEOUT;
        let stableChecks = 0;
        let missing = topics.map(topic => topic.name);
        const check = () => callOrFail(cb, () =>
            consumer.getMetadata({ timeout: METADATA_TIMEOUT },
            (err, metadata) => {
                if (!err) {
                    missing = topics.filter(topic => {
                        const found = metadata.topics.find(
                            t => t.name === topic.name);
                        return !found ||
                            found.partitions.length !== topic.partitions ||
                            !found.partitions.every(p => p.leader >= 0);
                    }).map(topic => topic.name);
                }
                stableChecks = !err && missing.length === 0 ?
                    stableChecks + 1 : 0;
                if (stableChecks >= STABLE_METADATA_CHECKS) {
                    return cb();
                }
                if (Date.now() >= deadline) {
                    return cb(new Error('timed out waiting for topics to ' +
                        `propagate, still incomplete: ${missing.join(', ')}`));
                }
                return setTimeout(check, TOPIC_PROPAGATION_POLL_MS);
            }));
        return check();
    }, done);
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

/**
 * Polls the offsets committed by a group until they add up to the expected
 * number of records. On a topic that starts empty the committed offset of a
 * partition is the number of records that partition holds, so the sum over
 * every partition is the number of records the group is done with.
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
    const toppars = [];
    for (let i = 0; i < partitionCount; i++) {
        toppars.push({ topic, partition: i });
    }
    return withConsumer(groupId, (consumer, cb) => {
        const deadline = Date.now() + timeoutMs;
        let lastSeen = null;
        const check = () => callOrFail(cb, () =>
            consumer.committed(toppars, METADATA_TIMEOUT,
            (err, committed) => {
                if (!err) {
                    // an unset offset is reported as a negative value
                    lastSeen = (committed || [])
                        .map(tp => tp.offset)
                        .filter(offset => offset >= 0)
                        .reduce((total, offset) => total + offset, 0);
                    if (lastSeen === expected) {
                        return cb();
                    }
                }
                if (Date.now() >= deadline) {
                    return cb(new Error('timed out waiting for group ' +
                        `${groupId} to commit ${expected} records, last ` +
                        `seen ${lastSeen}`));
                }
                return setTimeout(check, 1000);
            }));
        return check();
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
            'group.id': `poc-wg-tailer-${this.topic}`,
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
        try {
            this._consume();
        } catch {
            // the client was torn down under the call, stop reading
            this._stopped = true;
        }
    }

    _consume() {
        this._consumer.consume(100, (err, records) => {
            if (!err && records) {
                records.forEach(record_ => this.records.push({
                    partition: record_.partition,
                    offset: record_.offset,
                    key: record_.key === null || record_.key === undefined ?
                        null : record_.key.toString(),
                    value: record_.value.toString(),
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
            const label = typeof what === 'function' ? what() : what;
            return done(new Error(`timed out waiting for ${label}`));
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

/**
 * Every sample of a metric whose labels match, so a test can assert on the
 * label set itself and not only on a total
 *
 * @param {String} name - metric name
 * @param {Object} labels - labels the sample has to match
 * @param {Function} done - callback: done(err, values)
 * @return {undefined}
 */
function readSeries(name, labels, done) {
    const metric = ZenkoMetrics.getMetric(name);
    if (!metric) {
        return process.nextTick(() => done(null, []));
    }
    return metric.get().then(({ values }) => done(null, values
        .filter(v => Object.entries(labels)
            .every(([label, value]) => v.labels[label] === value))), done);
}

function readCounter(name, labels, done) {
    return readSeries(name, labels, (err, values) => {
        if (err) {
            return done(err);
        }
        return done(null, values.reduce((total, v) => total + v.value, 0));
    });
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

/**
 * Reads back one record delivered to an external destination topic
 *
 * @param {Object} rec - record read from the destination topic
 * @return {Object} the fields of the S3 event the assertions look at
 */
function deliveredEvent(rec) {
    const parsed = JSON.parse(rec.value);
    assert.strictEqual(parsed.Records.length, 1,
        'a delivered message holds exactly one event');
    const [event] = parsed.Records;
    return {
        recordKey: rec.key,
        partition: rec.partition,
        offset: rec.offset,
        bucket: event.s3.bucket.name,
        key: event.s3.object.key,
        eventName: event.eventName,
        eventTime: event.eventTime,
        configurationId: event.s3.configurationId,
    };
}

/**
 * Stops a worker, giving up after a while.
 *
 * BackbeatConsumer.close() waits for the revoke callback of a rebalance it
 * cannot time out on its own, so an unbounded stop would hang the suite
 * rather than fail the test that is at fault.
 *
 * @param {DeliveryWorker} worker - worker to stop, may be null
 * @param {Function} done - callback
 * @return {undefined}
 */
function stopWorker(worker, done) {
    if (!worker) {
        return process.nextTick(done);
    }
    let called = false;
    const finish = () => {
        if (!called) {
            called = true;
            done();
        }
    };
    const timer = setTimeout(finish, STOP_TIMEOUT);
    return worker.stop(() => {
        clearTimeout(timer);
        finish();
    });
}

function eventTime(index) {
    return new Date(Date.UTC(2026, 0, 1, 0, 0, index)).toISOString();
}

/**
 * Runs fn against a connected zookeeper client, then closes it
 *
 * @param {Function} fn - fn(client, cb)
 * @param {Function} done - callback: done(err, result)
 * @return {undefined}
 */
function withZkClient(fn, done) {
    const client = new ZookeeperManager(ZOOKEEPER_HOSTS, {
        autoCreateNamespace: false,
    }, new werelogs.Logger('WorkgroupsFunctionalSuite:zookeeper'));
    let settled = false;
    client.once('error', err => {
        if (!settled) {
            settled = true;
            client.close();
            done(err);
        }
    });
    return client.once('ready', () => {
        settled = true;
        return fn(client, (err, result) => {
            client.close();
            return done(err, result);
        });
    });
}

/**
 * Writes a workgroups document to the real zookeeper, the way the cutover
 * tool does: one setOrCreate of the whole document on one node
 *
 * @param {String} zkPath - znode path
 * @param {Object} doc - workgroups document
 * @param {Function} done - callback
 * @return {undefined}
 */
function writeWorkgroupsDocument(zkPath, doc, done) {
    return withZkClient((client, cb) => client.setOrCreate(zkPath,
        Buffer.from(JSON.stringify(doc)), err => cb(err)), done);
}

function cachePathFor(workgroupId) {
    return path.join(os.tmpdir(),
        `poc-workgroups-${RUN_ID}-${workgroupId}.json`);
}

/**
 * Loads the workgroups document with the real loader and boots the delivery
 * worker of one workgroup with the third constructor argument.
 *
 * Nothing here is a stand in: the document comes back out of zookeeper, the
 * filter is the real slice filter and the group id is the real derivation.
 *
 * @param {Object} params - zkPath, workgroupId, baseGroupId, notifConfig
 * @param {Function} done - callback: done(err, runtime)
 * @return {undefined}
 */
function startWorkgroup(params, done) {
    const loader = new WorkgroupConfigLoader({
        zkConfig: {
            connectionString: ZOOKEEPER_HOSTS,
            autoCreateNamespace: false,
        },
        workgroupsConfig: {
            zookeeperPath: params.zkPath,
            cachePath: cachePathFor(params.workgroupId),
        },
        topic: params.notifConfig.deliveryPool.topic,
        workgroupId: params.workgroupId,
        logger: new werelogs.Logger('WorkgroupConfigLoader:ft'),
    });
    return loader.load((loadErr, loaded) => {
        if (loadErr) {
            return done(loadErr);
        }
        assert.strictEqual(loaded.source, 'zookeeper',
            'the document has to come from zookeeper, not from the cache');
        const doc = loaded.doc;
        const workgroup = {
            id: params.workgroupId,
            generation: doc.generation,
            groupId: buildGroupId(params.baseGroupId, params.workgroupId,
                doc.generation),
            filter: createSliceFilter({ doc, workgroupId: params.workgroupId }),
        };
        const worker = new DeliveryWorker(kafkaConfig, params.notifConfig,
            workgroup);
        return worker.start(null, startErr => done(startErr, {
            loader,
            worker,
            workgroup,
            doc,
        }));
    });
}

function stopWorkgroup(runtime, done) {
    if (!runtime) {
        return process.nextTick(done);
    }
    return stopWorker(runtime.worker, () => runtime.loader.stop(done));
}

/**
 * Workgroup id of one attempt. A retry runs under fresh ids so that its
 * consumer groups and its metric series never carry anything the wedged
 * attempt left behind.
 *
 * @param {String} base - workgroup id of the first attempt
 * @param {Number} attempt - attempt number, one based
 * @return {String} workgroup id for that attempt
 */
function attemptId(base, attempt) {
    return attempt === 1 ? base : `${base}-r${attempt}`;
}

/**
 * Runs a phase, and runs it a second time when the first attempt failed
 * having consumed nothing at all.
 *
 * That signature, a consumer that holds its partitions and never consumes
 * while it reports ready, is the pre-existing BackbeatConsumer wedge written
 * up in design/06-backbeatconsumer-wedge.md. It is not caused by workgroups
 * and is not fixed here. A phase that delivered something and then failed is
 * a real failure and is never retried.
 *
 * @param {Object} params - label, run, progress and cleanup
 * @param {Function} done - callback
 * @return {undefined}
 */
function withWedgeRetry(params, done) {
    return params.run(1, firstErr => {
        if (!firstErr) {
            return done();
        }
        return params.progress(1, (progressErr, delivered) => {
            if (progressErr || delivered > 0) {
                return done(firstErr);
            }
            WEDGES.push({
                phase: params.label,
                attempt: 1,
                error: firstErr.message,
            });
            suiteLog.warn('a phase consumed nothing at all, retrying it once ' +
                'and recording the occurrence as the pre-existing consumer ' +
                'wedge', {
                phase: params.label,
                error: firstErr.message,
            });
            return params.cleanup(1, () => params.run(2, done));
        });
    });
}

/**
 * Picks destination names that the document's own ownership function assigns
 * to the workgroup a gate wants them in, so a gate never depends on where
 * one run's names happen to hash.
 *
 * Only the candidate names are proposed by the hash; every assertion is made
 * against what the workers actually delivered.
 *
 * @param {Object} doc - validated workgroups document
 * @param {String} prefix - destination name prefix
 * @param {Object[]} wanted - [{ workgroupId, count }]
 * @return {Map} workgroup id to the destination names picked for it
 */
function selectDestinations(doc, prefix, wanted) {
    const remaining = wanted.map(entry => ({ ...entry }));
    const picked = new Map(wanted.map(entry => [entry.workgroupId, []]));
    for (let i = 0; i < 500 && remaining.some(e => e.count > 0); i++) {
        const resource = `${prefix}-${i}-${RUN_ID}`;
        const owner = workgroupIdForDestination(doc, resource);
        const slot = remaining.find(e => e.workgroupId === owner &&
            e.count > 0);
        if (slot) {
            picked.get(owner).push(resource);
            slot.count -= 1;
        }
    }
    assert(remaining.every(e => e.count === 0),
        `could not find ${prefix} names for every workgroup`);
    return picked;
}

/**
 * Picks object keys so that every sub key of a spread destination is
 * exercised, rather than trusting a random draw to cover all of them
 *
 * @param {Object} destination - destination configuration
 * @param {String} prefix - object key prefix
 * @param {Number} perSubKey - how many object keys to take per sub key
 * @return {String[]} object keys
 */
function keysCoveringEverySubKey(destination, prefix, perSubKey) {
    const bySubKey = new Map();
    let taken = 0;
    const wanted = destination.spreadFactor * perSubKey;
    for (let i = 0; i < 5000 && taken < wanted; i++) {
        const key = `${prefix}-${`${i}`.padStart(4, '0')}`;
        const deliveryKey = buildDeliveryKey(destination, BUCKET, key);
        const subKey = deliveryKey.slice(deliveryKey.indexOf('|') + 1);
        if (!bySubKey.has(subKey)) {
            bySubKey.set(subKey, []);
        }
        const keys = bySubKey.get(subKey);
        if (keys.length < perSubKey) {
            keys.push(key);
            taken += 1;
        }
    }
    assert.strictEqual(bySubKey.size, destination.spreadFactor,
        `could not cover every sub key of ${destination.resource}`);
    const ordered = [];
    bySubKey.forEach(keys => keys.forEach(key => ordered.push(key)));
    return ordered;
}

/**
 * Builds and validates a workgroups document with two hashmod workgroups
 * over modulo 2 and one static workgroup owning the whale
 *
 * @param {Object} params - topic, generation, ids and whaleResource
 * @return {Object} validated document
 */
function buildSlicedDocument(params) {
    const { error, value } = validateWorkgroupsDoc({
        configVersion: CONFIG_VERSION,
        generation: params.generation,
        topic: params.topic,
        updatedAt: new Date().toISOString(),
        workgroups: [
            { id: params.ids.zero,
                rule: { type: 'hashmod', modulo: 2, remainders: [0] } },
            { id: params.ids.one,
                rule: { type: 'hashmod', modulo: 2, remainders: [1] } },
            { id: params.ids.whale,
                rule: { type: 'static',
                    destinationIds: [params.whaleResource] } },
        ],
    });
    assert.ifError(error);
    return value;
}

// mocha root hook: every topic of the run exists and has propagated before
// the first consumer of the run is built
before(function createEveryTopic(done) {
    this.timeout(TOPIC_PROPAGATION_TIMEOUT + 60000);
    const topics = Object.values(TOPICS);
    record('run.id', RUN_ID);
    record('run.topics', topics.map(topic =>
        `${topic.name}(P=${topic.partitions})`));
    return async.series([
        next => createTopics(topics, next),
        next => waitForTopics(topics, next),
    ], done);
});

after(() => record('run.wedgeOccurrences', WEDGES));

describe('GATE W-A :: workgroups deliver only their own slice',
function gateSliceEnforcement() {
    this.timeout(600000);

    const deliveryTopic = TOPICS.aDelivery.name;
    const deliveryPartitions = TOPICS.aDelivery.partitions;
    const baseGroupId = `poc-wg-a-group-${RUN_ID}`;
    const zkBase = `${ZK_BASE}/gate-a`;
    const recordsPerDestination = 6;
    const whaleKeysPerSubKey = 4;

    const baseIds = { zero: 'wga-zero', one: 'wga-one', whale: 'wga-whale' };
    const activeIds = { ...baseIds };

    const whaleResource = `poc-wg-a-whale-dest-${RUN_ID}`;
    const whale = destinationConfig({
        resource: whaleResource,
        topic: TOPICS.aWhaleCustomer.name,
        // six sub keys on a twelve partition delivery topic, which the
        // pinned crc32 law puts on at least four distinct partitions
        spreadFactor: 6,
    });

    // the plan document only exists to propose destination names: remainder
    // membership does not move when a retry renames the workgroups
    const planDoc = buildSlicedDocument({
        topic: deliveryTopic,
        generation: 1,
        ids: baseIds,
        whaleResource,
    });
    const selected = selectDestinations(planDoc, 'poc-wg-a-dest', [
        { workgroupId: baseIds.zero, count: A_CUSTOMER_COUNT / 2 },
        { workgroupId: baseIds.one, count: A_CUSTOMER_COUNT / 2 },
    ]);
    const zeroResources = selected.get(baseIds.zero);
    const oneResources = selected.get(baseIds.one);

    const zeroDestinations = zeroResources.map((resource, index) =>
        destinationConfig({
            resource,
            topic: TOPICS[`aCustomer${index}`].name,
        }));
    const oneDestinations = oneResources.map((resource, index) =>
        destinationConfig({
            resource,
            topic: TOPICS[`aCustomer${index + A_CUSTOMER_COUNT / 2}`].name,
        }));
    const plainDestinations = zeroDestinations.concat(oneDestinations);
    const allDestinations = plainDestinations.concat([whale]);

    const notifConfig = {
        destinations: allDestinations,
        deliveryPool: deliveryPoolConfig({
            topic: deliveryTopic,
            groupId: baseGroupId,
            concurrency: 10,
        }),
    };

    const whaleKeys = keysCoveringEverySubKey(whale, 'whale-obj',
        whaleKeysPerSubKey);
    const whaleCount = whaleKeys.length;
    const plainCount = plainDestinations.length * recordsPerDestination;
    const totalRecords = plainCount + whaleCount;
    const zeroOwned = zeroDestinations.length * recordsPerDestination;
    const oneOwned = oneDestinations.length * recordsPerDestination;

    const tailers = new Map();
    let zeroRuntime = null;
    let oneRuntime = null;
    let whaleRuntime = null;
    let phaseOne = null;

    function groupIdOf(workgroupId) {
        return buildGroupId(baseGroupId, workgroupId, 1);
    }

    function writeDocument(zkPath, done) {
        return writeWorkgroupsDocument(zkPath, buildSlicedDocument({
            topic: deliveryTopic,
            generation: 1,
            ids: activeIds,
            whaleResource,
        }), done);
    }

    function tailerOf(destination) {
        return tailers.get(destination.topic);
    }

    before(done => {
        const records = [];
        // events of one destination are produced together, destinations are
        // interleaved, so no worker ever sees one destination's records as
        // one uninterrupted run
        for (let i = 0; i < recordsPerDestination; i++) {
            plainDestinations.forEach((destination, index) =>
                records.push(addressedRecord({
                    destination,
                    key: `a${index}-obj-${`${i}`.padStart(3, '0')}`,
                    eventType: 's3:ObjectCreated:Put',
                    dateTime: eventTime(i),
                })));
        }
        whaleKeys.forEach((key, index) => records.push(addressedRecord({
            destination: whale,
            key,
            eventType: 's3:ObjectCreated:Put',
            dateTime: eventTime(index),
        })));
        assert.strictEqual(records.length, totalRecords);
        record('W-A.destinations.zero', zeroResources);
        record('W-A.destinations.one', oneResources);
        record('W-A.destinations.whale', whaleResource);
        record('W-A.records.total', totalRecords);
        record('W-A.records.ownedByZero', zeroOwned);
        record('W-A.records.ownedByOne', oneOwned);
        record('W-A.records.ownedByWhale', whaleCount);
        return async.series([
            // produced before any worker exists: a worker joining with a
            // fresh group only sees them because it reads from the earliest
            // offset
            next => produceRecords(deliveryTopic, records, next),
            next => async.eachSeries(allDestinations, (destination, tailDone) => {
                const tailer = new TopicTailer(destination.topic);
                tailers.set(destination.topic, tailer);
                return tailer.start(tailDone);
            }, next),
            // the empirical layout the broker chose, which is the only thing
            // the whale spread assertion is allowed to be made against
            next => readTopic(deliveryTopic, totalRecords, 60000,
                (err, written) => {
                    if (err) {
                        return next(err);
                    }
                    assert.strictEqual(written.length, totalRecords);
                    const whalePrefix =
                        `${encodeURIComponent(whaleResource)}%7C`;
                    const whalePartitions = new Set(written
                        .filter(r => r.key && r.key.startsWith(whalePrefix))
                        .map(r => r.partition));
                    record('W-A.whale.subKeys', new Set(written
                        .filter(r => r.key && r.key.startsWith(whalePrefix))
                        .map(r => r.key.slice(whalePrefix.length))).size);
                    record('W-A.whale.deliveryPartitions',
                        [...whalePartitions].sort((a, b) => a - b));
                    return next();
                }),
        ], done);
    });

    after(done => async.series([
        next => stopWorkgroup(zeroRuntime, next),
        next => stopWorkgroup(oneRuntime, next),
        next => stopWorkgroup(whaleRuntime, next),
        next => async.eachSeries([...tailers.values()],
            (tailer, tailDone) => tailer.stop(tailDone), next),
    ], done));

    it('should let the first workgroup drain its own destinations, skip ' +
    'every other record and touch nothing else', done => withWedgeRetry({
        label: 'W-A phase 1',
        run: (attempt, cb) => {
            activeIds.zero = attemptId(baseIds.zero, attempt);
            const zkPath = `${zkBase}/phase1-attempt-${attempt}`;
            return async.waterfall([
                next => writeDocument(zkPath, err => next(err)),
                next => startWorkgroup({
                    zkPath,
                    workgroupId: activeIds.zero,
                    baseGroupId,
                    notifConfig,
                }, next),
            ], (err, runtime) => {
                zeroRuntime = runtime || zeroRuntime;
                if (err) {
                    return cb(err);
                }
                // the whole topic has to be consumed before the skip counter
                // can be compared with anything
                return waitForCommittedTotal(runtime.workgroup.groupId,
                    deliveryTopic, deliveryPartitions, totalRecords, 120000,
                    cb);
            });
        },
        progress: (attempt, cb) => readCounter(DELIVERED_METRIC,
            { workgroup: attemptId(baseIds.zero, attempt) }, cb),
        cleanup: (attempt, cb) => stopWorkgroup(zeroRuntime, () => {
            zeroRuntime = null;
            return cb();
        }),
    }, err => {
        assert.ifError(err);
        return async.series([
            next => async.eachSeries(zeroDestinations,
                (destination, destDone) => waitUntilQuiet(
                    tailerOf(destination), 500, destDone), next),
            next => {
                zeroDestinations.forEach(destination => {
                    const tailer = tailerOf(destination);
                    assert.strictEqual(tailer.records.length,
                        recordsPerDestination,
                        `${destination.resource} did not drain to its exact ` +
                        'count');
                    tailer.records.map(deliveredEvent).forEach(event => {
                        assert.strictEqual(event.bucket, BUCKET);
                        assert.strictEqual(event.configurationId, CONFIG_ID);
                    });
                });
                oneDestinations.concat([whale]).forEach(destination =>
                    assert.strictEqual(tailerOf(destination).records.length, 0,
                        `${destination.resource} belongs to another ` +
                        'workgroup and must have received nothing'));
                phaseOne = {
                    workgroupId: activeIds.zero,
                    groupId: groupIdOf(activeIds.zero),
                };
                record('W-A.phase1.workgroup', activeIds.zero);
                return next();
            },
            next => readCounter(DELIVERED_METRIC,
                { workgroup: activeIds.zero }, (err2, value) => {
                    assert.ifError(err2);
                    record('W-A.phase1.delivered', value);
                    assert.strictEqual(value, zeroOwned);
                    return next();
                }),
            next => readCounter(SKIPPED_METRIC,
                { workgroup: activeIds.zero, reason: 'not_in_slice' },
                (err2, value) => {
                    assert.ifError(err2);
                    record('W-A.phase1.skippedNotInSlice', value);
                    assert.strictEqual(value, oneOwned + whaleCount,
                        'the first workgroup has to skip exactly the records ' +
                        'the other two own');
                    return next();
                }),
            next => readCounter(DROPPED_METRIC,
                { workgroup: activeIds.zero }, (err2, value) => {
                    assert.ifError(err2);
                    record('W-A.phase1.dropped', value);
                    assert.strictEqual(value, 0,
                        'a record of another workgroup is not mine, it is ' +
                        'not undeliverable');
                    return next();
                }),
        ], done);
    }));

    it('should deliver every destination to its exact count once the other ' +
    'workgroups join, and spread the whale over several partitions',
    done => withWedgeRetry({
        label: 'W-A phase 2',
        run: (attempt, cb) => {
            activeIds.one = attemptId(baseIds.one, attempt);
            activeIds.whale = attemptId(baseIds.whale, attempt);
            const zkPath = `${zkBase}/phase2-attempt-${attempt}`;
            return async.waterfall([
                next => writeDocument(zkPath, err => next(err)),
                next => startWorkgroup({
                    zkPath,
                    workgroupId: activeIds.one,
                    baseGroupId,
                    notifConfig,
                }, next),
                (runtime, next) => {
                    oneRuntime = runtime;
                    return startWorkgroup({
                        zkPath,
                        workgroupId: activeIds.whale,
                        baseGroupId,
                        notifConfig,
                    }, next);
                },
            ], (err, runtime) => {
                whaleRuntime = runtime || whaleRuntime;
                if (err) {
                    return cb(err);
                }
                return async.eachSeries([activeIds.one, activeIds.whale],
                    (workgroupId, next) => waitForCommittedTotal(
                        groupIdOf(workgroupId), deliveryTopic,
                        deliveryPartitions, totalRecords, 120000, next), cb);
            });
        },
        progress: (attempt, cb) => async.map(
            [attemptId(baseIds.one, attempt),
                attemptId(baseIds.whale, attempt)],
            (workgroupId, next) => readCounter(DELIVERED_METRIC,
                { workgroup: workgroupId }, next),
            (err, values) => cb(err, (values || [])
                .reduce((total, value) => total + value, 0))),
        cleanup: (attempt, cb) => async.series([
            next => stopWorkgroup(oneRuntime, () => {
                oneRuntime = null;
                return next();
            }),
            next => stopWorkgroup(whaleRuntime, () => {
                whaleRuntime = null;
                return next();
            }),
        ], () => cb()),
    }, err => {
        assert.ifError(err);
        return async.series([
            next => async.eachSeries(allDestinations,
                (destination, destDone) => waitUntilQuiet(
                    tailerOf(destination), 500, destDone), next),
            next => {
                plainDestinations.forEach(destination =>
                    assert.strictEqual(tailerOf(destination).records.length,
                        recordsPerDestination,
                        `${destination.resource} did not drain to its exact ` +
                        'count'));
                assert.strictEqual(tailerOf(whale).records.length, whaleCount,
                    'the whale did not drain to its exact count');
                record('W-A.phase2.workgroups',
                    [activeIds.one, activeIds.whale]);
                return next();
            },
            next => readCounter(DELIVERED_METRIC,
                { workgroup: activeIds.one }, (err2, value) => {
                    assert.ifError(err2);
                    record('W-A.phase2.deliveredByOne', value);
                    assert.strictEqual(value, oneOwned);
                    return next();
                }),
            next => readCounter(DELIVERED_METRIC,
                { workgroup: activeIds.whale }, (err2, value) => {
                    assert.ifError(err2);
                    record('W-A.phase2.deliveredByWhale', value);
                    assert.strictEqual(value, whaleCount);
                    return next();
                }),
            next => readCounter(SKIPPED_METRIC,
                { workgroup: activeIds.one, reason: 'not_in_slice' },
                (err2, value) => {
                    assert.ifError(err2);
                    record('W-A.phase2.skippedByOne', value);
                    assert.strictEqual(value, zeroOwned + whaleCount);
                    return next();
                }),
            next => readCounter(SKIPPED_METRIC,
                { workgroup: activeIds.whale, reason: 'not_in_slice' },
                (err2, value) => {
                    assert.ifError(err2);
                    record('W-A.phase2.skippedByWhale', value);
                    assert.strictEqual(value, zeroOwned + oneOwned);
                    return next();
                }),
            next => readCounter(DROPPED_METRIC, {}, (err2, value) => {
                assert.ifError(err2);
                record('W-A.phase2.droppedTotal', value);
                assert.strictEqual(value, 0);
                return next();
            }),
            next => {
                // the delivery topic layout was read before any worker ran,
                // so this is the empirical spread and not a computed one
                const partitions = OBSERVED['W-A.whale.deliveryPartitions'];
                assert(partitions.length >= 2,
                    'a spreadFactor 6 destination has to occupy more than ' +
                    `one partition, it occupied ${partitions.length}`);
                return next();
            },
        ], done);
    }));

    it('should attribute every delivery of a workgroup to a destination ' +
    'that workgroup owns', done => async.eachSeries(
        [activeIds.zero, activeIds.one, activeIds.whale],
        (workgroupId, next) => readSeries(DELIVERED_METRIC,
            { workgroup: workgroupId }, (err, values) => {
                assert.ifError(err);
                assert(values.length > 0,
                    `${workgroupId} delivered nothing at all`);
                const doc = buildSlicedDocument({
                    topic: deliveryTopic,
                    generation: 1,
                    ids: activeIds,
                    whaleResource,
                });
                values.forEach(v => assert.strictEqual(
                    workgroupIdForDestination(doc, v.labels.target),
                    workgroupId,
                    `${workgroupId} delivered to ${v.labels.target}, which ` +
                    'it does not own'));
                return next();
            }), err => {
        assert.ifError(err);
        assert.deepStrictEqual(phaseOne.groupId,
            `${baseGroupId}-${activeIds.zero}-gen1`,
            'the consumer group id has to be the pinned derivation');
        return done();
    }));
});
