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
    ASSUME_DESTINATION,
    CONFIG_VERSION,
    assumedDestinationFor,
    buildGroupId,
    createSliceFilter,
    validateWorkgroupsDoc,
    workgroupIdForDestination,
} = require('../../../extensions/notification/utils/workgroups');

const KAFKA_HOSTS = 'localhost:9092';
// bare connection string, with no chroot: the workgroups path is a path on
// the client, never appended to the connection string
const ZOOKEEPER_HOSTS = 'localhost:2181';
const CONNECT_TIMEOUT = 20000;
const STOP_TIMEOUT = 20000;
const METADATA_TIMEOUT = 10000;
const TOPIC_ALREADY_EXISTS = 36;
const CONFIG_ID = 'poc-assume-config';
// unique per run, so that a rerun never lands on the topics, the consumer
// groups, the zookeeper nodes or the metric labels of the previous one
const RUN_ID = `${Date.now()}`;
const ZK_BASE = `/poc-assume-${RUN_ID}`;

const DELIVERED_METRIC = 's3_notification_delivery_worker_delivered_total';
const DROPPED_METRIC = 's3_notification_delivery_worker_dropped_total';
const SKIPPED_METRIC = 's3_notification_delivery_worker_skipped_total';
const ASSUMED_METRIC = 's3_notification_delivery_worker_assumed_destination';

const kafkaConfig = { hosts: KAFKA_HOSTS };

const TOPIC_PROPAGATION_TIMEOUT = 60000;
const TOPIC_PROPAGATION_POLL_MS = 1000;
const STABLE_METADATA_CHECKS = 3;

const suiteLog = new werelogs.Logger('AssumeDestinationFunctionalSuite');

// every observation a gate makes, so the run output carries the numbers the
// write up quotes rather than only pass or fail
const OBSERVED = {};

// every time a phase had to be run a second time because its workers
// consumed nothing at all, which is the pre-existing wedge of
// design/06-backbeatconsumer-wedge.md and not an assume-destination failure
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
    suiteLog.info('assume-destination gate observation',
        { observation: name, value });
    return value;
}

// GATE AD-A: two destination arns collapsed onto one assumed destination,
// with a third destination owned by the hashmod workgroup next to them
const A_LEGACY_KEYS = 3;
const A_EVENTS_PER_KEY = 4;
const A_SHARED_EVENTS_PER_ARN = 3;
const A_AUTO_KEYS = 3;
const A_AUTO_EVENTS_PER_KEY = 2;

// GATE AD-B: the shared legacy topic, whose records carry no destination at
// all, drained by an assume-destination workgroup sitting next to two
// hashmod ones
const B_BUCKETS = 4;
const B_KEYS_PER_BUCKET = 5;
const B_ADDRESSED_KEYS = 3;
const B_ADDRESSED_EVENTS_PER_KEY = 2;

// the delivery topic of each gate, produced once and read from the earliest
// offset by every attempt. Destination topics are not here: each attempt
// creates its own, see attemptSetup
const DELIVERY_TOPICS = {
    a: { name: `poc-ad-a-delivery-${RUN_ID}`, partitions: 4 },
    b: { name: `poc-ad-b-delivery-${RUN_ID}`, partitions: 4 },
};

// wide enough to outlast a wedged consumer being evicted from its group.
// BackbeatConsumer.close() waits for a revoke callback a wedged consumer
// never delivers, so stopWorker gives up on its own timeout while that
// client is still a live member of the group. The replacement joining the
// same group makes it two members, one of which never rejoins, and the
// group sits in PreparingRebalance consuming nothing until the broker
// evicts the wedged member on its poll interval, which was measured at
// about five minutes in design/09-workgroups-observations.md
const COMMIT_TIMEOUT_MS = 420000;
const WORKER_SETTLE_MS = 1200;

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
 * its partitions, several looks in a row
 *
 * @param {Object[]} topics - topics, as { name, partitions }
 * @param {Function} done - callback
 * @return {undefined}
 */
function waitForTopics(topics, done) {
    return withConsumer(`poc-ad-meta-${RUN_ID}`, (consumer, cb) => {
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

/**
 * Creates topics and waits for them to propagate, before anything builds a
 * consumer or a producer over them
 *
 * @param {String} label - what these topics are, for the run output
 * @param {Object[]} topics - topics, as { name, partitions }
 * @param {Function} done - callback
 * @return {undefined}
 */
function createAndWaitForTopics(label, topics, done) {
    record(`run.topics.${label}`, topics.map(topic =>
        `${topic.name}(P=${topic.partitions})`));
    return async.series([
        next => createTopics(topics, next),
        next => waitForTopics(topics, next),
    ], done);
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
            'group.id': `poc-ad-tailer-${this.topic}`,
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
                records.forEach(rec => this.records.push({
                    partition: rec.partition,
                    offset: rec.offset,
                    key: rec.key === null || rec.key === undefined ?
                        null : rec.key.toString(),
                    value: rec.value.toString(),
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
        process.nextTick(() => done(null, []));
        return undefined;
    }
    // deliberately not returned: a promise handed back to mocha from an it()
    // is reported as "resolution method is overspecified", and an assertion
    // thrown inside a then() would reject a promise nobody holds, so the
    // case would time out instead of saying what was wrong
    metric.get().then(({ values }) => {
        const matched = values.filter(v => Object.entries(labels)
            .every(([label, value]) => v.labels[label] === value));
        process.nextTick(() => done(null, matched));
    }, err => process.nextTick(() => done(err)));
    return undefined;
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
        pollIntervalMs: params.pollIntervalMs || 100,
    };
}

function deliveryPoolConfig(params) {
    return {
        enabled: true,
        topic: params.topic,
        groupId: params.groupId,
        deliveryTimeoutMs: 30000,
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
 * @param {Object} params - destination, bucket, key, eventType and dateTime
 * @return {Object} kafka message, as { key, message }
 */
function addressedRecord(params) {
    const { destination, bucket, key, dateTime } = params;
    const message = {
        bucket,
        key,
        eventType: 's3:ObjectCreated:Put',
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
        key: encodeURIComponent(buildDeliveryKey(destination, bucket, key)),
        message: JSON.stringify(message),
    };
}

/**
 * Builds a record shaped like the ones on today's shared notification topic:
 * keyed by bucket and object so that one object stays on one partition, and
 * carrying no destination and no configuration id at all, because the topic
 * itself was the address.
 *
 * NotificationQueuePopulator._publishLegacyEntries is the original of this.
 *
 * @param {Object} params - bucket, key and dateTime
 * @return {Object} kafka message, as { key, message }
 */
function legacyRecord(params) {
    const { bucket, key, dateTime } = params;
    const message = {
        bucket,
        key,
        eventType: 's3:ObjectCreated:Put',
        dateTime,
        versionId: null,
        size: '1024',
        region: 'us-east-1',
        schemaVersion: '5',
    };
    return {
        key: encodeURIComponent(`${bucket}/${key}`),
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
    }, new werelogs.Logger('AssumeDestinationFunctionalSuite:zookeeper'));
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
        `poc-assume-${RUN_ID}-${workgroupId}.json`);
}

/**
 * Loads the workgroups document with the real loader and boots the delivery
 * worker of one workgroup.
 *
 * Nothing here is a stand in: the document comes back out of zookeeper, the
 * filter is the real slice filter, the group id is the real derivation, and
 * the assumed destination is read off the document by the same function the
 * worker task uses.
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
            assumedDestination: assumedDestinationFor(doc, params.workgroupId),
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

/**
 * Starts one workgroup per id, each through the real loader, confirming the
 * delivery topic's metadata is stable immediately before every join and
 * leaving a gap between them.
 *
 * The reconfirm is per join. A topic created and verified minutes earlier is
 * not verified at join time: the broker intermittently answers "unknown
 * topic or partition" for it, which drops it out of the effective
 * subscription and starts the rebalance loop of
 * design/06-backbeatconsumer-wedge.md. This is the mitigation
 * design/09-workgroups-observations.md found to work.
 *
 * @param {Object} params - zkPath, workgroupIds, baseGroupId, notifConfig
 *   and started, the array runtimes are appended to
 * @param {Function} done - callback
 * @return {undefined}
 */
function startWorkgroups(params, done) {
    return async.eachSeries(params.workgroupIds, (workgroupId, next) =>
        waitForTopics([{
            name: params.notifConfig.deliveryPool.topic,
            partitions: params.deliveryPartitions,
        }], topicErr => {
            if (topicErr) {
                return next(topicErr);
            }
            return startWorkgroup({
                zkPath: params.zkPath,
                workgroupId,
                baseGroupId: params.baseGroupId,
                notifConfig: params.notifConfig,
            }, (err, runtime) => {
                if (runtime) {
                    params.started.push(runtime);
                }
                if (err) {
                    return next(err);
                }
                return setTimeout(next, WORKER_SETTLE_MS);
            });
        }), done);
}

function stopWorkgroup(runtime, done) {
    if (!runtime) {
        return process.nextTick(done);
    }
    return stopWorker(runtime.worker, () => runtime.loader.stop(done));
}

function stopWorkgroups(runtimes, done) {
    return async.eachSeries(runtimes, (runtime, next) =>
        stopWorkgroup(runtime, next), () => done());
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
 * The smallest number of records any of these workgroups accounted for,
 * whether by delivering, skipping or dropping it.
 *
 * The wedge worth retrying is a consumer that consumed nothing at all, and
 * a workgroup whose records are all dropped or all skipped by design never
 * delivers one, so a delivery count cannot tell the two apart. Taking the
 * minimum rather than the sum keeps one working workgroup from covering for
 * a wedged one.
 *
 * @param {String[]} workgroupIds - workgroups that all had to consume
 * @param {Function} done - callback: done(err, accounted)
 * @return {undefined}
 */
function accountedFor(workgroupIds, done) {
    return async.map(workgroupIds, (workgroupId, next) => async.map(
        [DELIVERED_METRIC, SKIPPED_METRIC, DROPPED_METRIC],
        (metric, counted) => readCounter(metric, { workgroup: workgroupId },
            counted),
        (err, values) => next(err,
            err ? 0 : values.reduce((total, value) => total + value, 0))),
    (err, totals) => done(err, err ? 0 : Math.min(...totals)));
}

function settleOnce(done) {
    let settled = false;
    return err => {
        if (settled) {
            suiteLog.warn('a gate called back after its case had already ' +
                'been reported, dropping the result',
                { error: err && err.message });
            return;
        }
        settled = true;
        done(err);
    };
}

function runGuarded(done, body) {
    return body(settleOnce(done));
}

/**
 * Runs a phase, and runs it a second time when the first attempt failed
 * having consumed nothing at all.
 *
 * That signature, a consumer that holds its partitions and never consumes
 * while it reports ready, is the pre-existing BackbeatConsumer wedge written
 * up in design/06-backbeatconsumer-wedge.md. It is not caused by workgroups,
 * has nothing to do with the submode under test, and is not fixed here. A
 * phase that delivered something and then failed is a real failure and is
 * never retried.
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
                'wedge', { phase: params.label, error: firstErr.message });
            return params.cleanup(1, () => params.run(2, done));
        });
    });
}

// BackbeatConsumer raises "Local: Erroneous state" synchronously out of
// node-rdkafka from three call sites, all of them reached from a callback
// nobody can catch, and all of them when an entry or a consume retry lands
// while its consumer is between assignments or closing:
//
//   onEntryCommittable -> isPaused -> KafkaConsumer.subscription
//   onEntryCommittable -> KafkaConsumer.offsetsStore
//   _tryConsume error branch -> isPaused -> KafkaConsumer.subscription
//
// The first two are the commit path written up in
// design/09-workgroups-observations.md, where the guard raises the very
// exception it exists to avoid. The third is the same guard on the consume
// path, reached when the topic metadata query answers "unknown topic" while
// the client is closing, which is the wedge teardown of
// design/06-backbeatconsumer-wedge.md.
//
// In a service each of these takes the process down. Here they would land on
// whichever mocha case is running and pre-empt this suite's own wedge
// handling. Anything that comes through those frames is therefore taken out
// of mocha's hands and counted; every other uncaught exception goes straight
// back to the listeners mocha installed. Pre-existing and out of scope:
// worked around, never hidden, every occurrence is reported in
// run.consumerPathThrows.
const CONSUMER_THROWS = [];
let mochaUncaught = [];

/**
 * Which of the three pre-existing throw sites raised this, or null when the
 * exception did not come through any of them
 *
 * @param {String} stack - stack of the uncaught exception
 * @return {String|null} route name
 */
function preExistingThrowRoute(stack) {
    if (stack.includes('BackbeatConsumer.onEntryCommittable')) {
        return stack.includes('KafkaConsumer.offsetsStore') ?
            'commit_offsetsStore' : 'commit_isPaused';
    }
    if (stack.includes('BackbeatConsumer.isPaused')) {
        return 'consume_isPaused';
    }
    return null;
}

function installUncaughtFilter() {
    mochaUncaught = process.listeners('uncaughtException');
    process.removeAllListeners('uncaughtException');
    process.on('uncaughtException', (err, origin) => {
        const stack = (err && err.stack) || '';
        const via = preExistingThrowRoute(stack);
        if (via) {
            CONSUMER_THROWS.push({ error: err.message, via });
            suiteLog.error('a pre-existing throw escaped the consumer, ' +
                'counted rather than failing the case',
                { error: err.message, via });
            return;
        }
        if (mochaUncaught.length === 0) {
            // nothing was captured to hand it back to, so this filter must
            // not become the place uncaught exceptions go to disappear
            throw err;
        }
        mochaUncaught.forEach(listener => listener(err, origin));
    });
}

before(() => {
    installUncaughtFilter();
    record('run.id', RUN_ID);
});

afterEach(function logFailure() {
    const test = this.currentTest;
    if (!test || test.state !== 'failed') {
        return;
    }
    suiteLog.error('an assume-destination gate case failed', {
        title: test.title,
        error: test.err && test.err.message,
        stack: test.err && test.err.stack,
    });
});

after(() => {
    record('run.wedgeOccurrences', WEDGES);
    const byRoute = {};
    CONSUMER_THROWS.forEach(thrown => {
        byRoute[thrown.via] = (byRoute[thrown.via] || 0) + 1;
    });
    record('run.consumerPathThrows', byRoute);
});

describe('GATE AD-A :: several arns collapse onto one assumed destination',
function gateAssumedDelivery() {
    this.timeout(1800000);

    const delivery = DELIVERY_TOPICS.a;
    const baseGroupId = `poc-ad-a-group-${RUN_ID}`;
    const zkBase = `${ZK_BASE}/gate-a`;
    const bucket = `poc-ad-a-bucket-${RUN_ID}`;

    const baseIds = { auto: 'ad-auto', legacy: 'ad-legacy' };
    const activeIds = { ...baseIds };

    // destination names are stable across attempts: the records are produced
    // once, stamped with these, and the document routes by them
    const legacyAResource = `poc-ad-legacy-a-${RUN_ID}`;
    const legacyBResource = `poc-ad-legacy-b-${RUN_ID}`;
    const autoResource = `poc-ad-auto-c-${RUN_ID}`;
    const assumedResource = `poc-ad-assumed-${RUN_ID}`;

    /**
     * The destinations, topics and configuration of one attempt.
     *
     * Every attempt gets destination topics of its own. A wedged consumer
     * never delivers the revoke callback close() waits for, so the wedge
     * recovery cannot stop the previous attempt's worker, and an attempt
     * sharing its predecessor's topics reads that worker's late deliveries
     * as its own. Attempt one is the normal case and pays one topic
     * creation for it.
     *
     * @param {Number} attempt - attempt number, one based
     * @return {Object} destinations, topics and notifConfig of the attempt
     */
    function attemptSetup(attempt) {
        const suffix = `${RUN_ID}-a${attempt}`;
        // the two legacy global arns, each with a destination of its own in
        // the registry. Those registry topics have to stay empty: an assumed
        // delivery that went through the registry would land on them
        const legacyA = destinationConfig({
            resource: legacyAResource,
            topic: `poc-ad-a-legacy-a-own-${suffix}`,
        });
        const legacyB = destinationConfig({
            resource: legacyBResource,
            topic: `poc-ad-a-legacy-b-own-${suffix}`,
        });
        // the account scoped destination next door, owned by the hashmod
        // workgroup and delivered to the destination it names
        const autoC = destinationConfig({
            resource: autoResource,
            topic: `poc-ad-a-auto-${suffix}`,
        });
        // declared on the workgroup, deliberately absent from the registry
        const assumed = destinationConfig({
            resource: assumedResource,
            topic: `poc-ad-a-assumed-${suffix}`,
        });
        return {
            legacyA,
            legacyB,
            autoC,
            assumed,
            destinations: [legacyA, legacyB, autoC, assumed],
            topics: [
                { name: assumed.topic, partitions: 3 },
                { name: legacyA.topic, partitions: 1 },
                { name: legacyB.topic, partitions: 1 },
                { name: autoC.topic, partitions: 1 },
            ],
            notifConfig: {
                destinations: [legacyA, legacyB, autoC],
                deliveryPool: deliveryPoolConfig({
                    topic: delivery.name,
                    groupId: baseGroupId,
                    concurrency: 10,
                }),
            },
        };
    }

    function buildDocument(ids, setup) {
        const { error, value } = validateWorkgroupsDoc({
            configVersion: CONFIG_VERSION,
            generation: 1,
            topic: delivery.name,
            updatedAt: new Date().toISOString(),
            workgroups: [
                { id: ids.auto,
                    rule: { type: 'hashmod', modulo: 1, remainders: [0] } },
                { id: ids.legacy,
                    submode: ASSUME_DESTINATION,
                    rule: { type: 'static',
                        destinationIds: [legacyAResource, legacyBResource] },
                    assumedDestination: {
                        resource: setup.assumed.resource,
                        type: setup.assumed.type,
                        host: setup.assumed.host,
                        port: setup.assumed.port,
                        topic: setup.assumed.topic,
                    } },
            ],
        });
        assert.ifError(error);
        return value;
    }

    const legacyKeysOf = arn => {
        const keys = [];
        for (let i = 0; i < A_LEGACY_KEYS; i++) {
            keys.push(`${arn}-obj-${`${i}`.padStart(3, '0')}`);
        }
        return keys;
    };
    const aKeys = legacyKeysOf('a');
    const bKeys = legacyKeysOf('b');
    // one object key stamped for both arns, which is the case the collapse
    // creates and nothing in the design promises anything about
    const sharedKey = 'shared-obj-000';
    const autoKeys = [];
    for (let i = 0; i < A_AUTO_KEYS; i++) {
        autoKeys.push(`auto-obj-${`${i}`.padStart(3, '0')}`);
    }

    const legacyOwned = 2 * A_LEGACY_KEYS * A_EVENTS_PER_KEY +
        2 * A_SHARED_EVENTS_PER_ARN;
    const autoOwned = A_AUTO_KEYS * A_AUTO_EVENTS_PER_KEY;
    const totalRecords = legacyOwned + autoOwned;

    // the attempt whose assertions are made: its setup and its tailers
    let active = null;
    let tailers = new Map();
    let runtimes = [];

    function tailerOf(destination) {
        return tailers.get(destination.topic);
    }

    function startTailers(setup, done) {
        tailers = new Map();
        return async.eachSeries(setup.destinations, (destination, next) => {
            const tailer = new TopicTailer(destination.topic);
            tailers.set(destination.topic, tailer);
            return tailer.start(next);
        }, done);
    }

    function stopTailers(done) {
        return async.eachSeries([...tailers.values()],
            (tailer, next) => tailer.stop(next), () => done());
    }

    /**
     * Every event delivered for one object key, in the order the broker
     * holds them
     *
     * @param {String} objectKey - object key
     * @return {Object[]} delivered events
     */
    function deliveredFor(objectKey) {
        return tailerOf(active.assumed).records
            .map(deliveredEvent)
            .filter(event => event.key === objectKey)
            .sort((left, right) => (left.partition - right.partition) ||
                (left.offset - right.offset));
    }

    before(done => {
        const plan = attemptSetup(1);
        const records = [];
        for (let i = 0; i < A_EVENTS_PER_KEY; i++) {
            aKeys.forEach(key => records.push(addressedRecord({
                destination: plan.legacyA, bucket, key, dateTime: eventTime(i),
            })));
            bKeys.forEach(key => records.push(addressedRecord({
                destination: plan.legacyB, bucket, key, dateTime: eventTime(i),
            })));
        }
        for (let i = 0; i < A_SHARED_EVENTS_PER_ARN; i++) {
            // interleaved on purpose: the two arns collapse onto one
            // physical destination and onto one kafka key here
            records.push(addressedRecord({
                destination: plan.legacyA, bucket, key: sharedKey,
                dateTime: eventTime(2 * i),
            }));
            records.push(addressedRecord({
                destination: plan.legacyB, bucket, key: sharedKey,
                dateTime: eventTime(2 * i + 1),
            }));
        }
        for (let i = 0; i < A_AUTO_EVENTS_PER_KEY; i++) {
            autoKeys.forEach(key => records.push(addressedRecord({
                destination: plan.autoC, bucket, key, dateTime: eventTime(i),
            })));
        }
        assert.strictEqual(records.length, totalRecords);
        record('AD-A.destinations.routedToAssumed',
            [legacyAResource, legacyBResource]);
        record('AD-A.destinations.assumed', assumedResource);
        record('AD-A.destinations.auto', autoResource);
        record('AD-A.records.total', totalRecords);
        record('AD-A.records.ownedByLegacy', legacyOwned);
        record('AD-A.records.ownedByAuto', autoOwned);
        // the routing of the collapsed arns is the document's, not a
        // coincidence of this run's names
        const planDoc = buildDocument(baseIds, plan);
        [legacyAResource, legacyBResource].forEach(resource =>
            assert.strictEqual(workgroupIdForDestination(planDoc, resource),
                baseIds.legacy));
        assert.strictEqual(workgroupIdForDestination(planDoc, autoResource),
            baseIds.auto);
        return async.series([
            next => createAndWaitForTopics('a-delivery', [delivery], next),
            next => produceRecords(delivery.name, records, next),
        ], done);
    });

    after(done => async.series([
        next => stopWorkgroups(runtimes, next),
        next => stopTailers(next),
    ], done));

    it('should deliver every record it owns to the assumed destination and ' +
    'nothing else anywhere', done => runGuarded(done, finish => withWedgeRetry({
        label: 'AD-A',
        run: (attempt, cb) => {
            activeIds.auto = attemptId(baseIds.auto, attempt);
            activeIds.legacy = attemptId(baseIds.legacy, attempt);
            const setup = attemptSetup(attempt);
            active = setup;
            const zkPath = `${zkBase}/attempt-${attempt}`;
            return async.series([
                next => createAndWaitForTopics(`a-destinations-${attempt}`,
                    setup.topics, next),
                next => startTailers(setup, next),
                next => writeWorkgroupsDocument(zkPath,
                    buildDocument(activeIds, setup), err => next(err)),
                next => startWorkgroups({
                    zkPath,
                    // the assuming workgroup joins first, so that a wedge
                    // hits the workgroup under test rather than its neighbour
                    workgroupIds: [activeIds.legacy, activeIds.auto],
                    baseGroupId,
                    notifConfig: setup.notifConfig,
                    deliveryPartitions: delivery.partitions,
                    started: runtimes,
                }, next),
                next => async.eachSeries([activeIds.legacy, activeIds.auto],
                    (workgroupId, waited) => waitForCommittedTotal(
                        buildGroupId(baseGroupId, workgroupId, 1),
                        delivery.name, delivery.partitions, totalRecords,
                        COMMIT_TIMEOUT_MS, waited), next),
            ], err => cb(err));
        },
        progress: (attempt, cb) => accountedFor(
            [attemptId(baseIds.legacy, attempt),
                attemptId(baseIds.auto, attempt)], cb),
        cleanup: (attempt, cb) => stopWorkgroups(runtimes, () => {
            runtimes = [];
            return stopTailers(cb);
        }),
    }, err => {
        if (err) {
            return finish(err);
        }
        const { legacyA, legacyB, autoC, assumed } = active;
        return async.series([
            next => async.eachSeries([...tailers.values()],
                (tailer, tailDone) => waitUntilQuiet(tailer, 500, tailDone),
                next),
            next => {
                record('AD-A.assumedTopic.delivered',
                    tailerOf(assumed).records.length);
                record('AD-A.registryTopics.delivered', {
                    [legacyA.resource]: tailerOf(legacyA).records.length,
                    [legacyB.resource]: tailerOf(legacyB).records.length,
                });
                record('AD-A.autoTopic.delivered',
                    tailerOf(autoC).records.length);
                assert.strictEqual(tailerOf(assumed).records.length,
                    legacyOwned,
                    'the assumed destination did not receive exactly the ' +
                    'records the workgroup owns');
                assert.strictEqual(tailerOf(legacyA).records.length, 0,
                    'a record reached the destination its arn names, so the ' +
                    'stamp was not ignored');
                assert.strictEqual(tailerOf(legacyB).records.length, 0,
                    'a record reached the destination its arn names, so the ' +
                    'stamp was not ignored');
                assert.strictEqual(tailerOf(autoC).records.length, autoOwned,
                    'the account scoped destination next door did not drain');
                return next();
            },
            next => {
                // the message format is the one every other destination
                // gets: the submode is a routing override, not a second
                // message shape
                tailerOf(assumed).records.map(deliveredEvent)
                    .forEach(event => {
                        assert.strictEqual(event.bucket, bucket);
                        assert.strictEqual(event.configurationId, CONFIG_ID);
                        assert.strictEqual(event.eventName,
                            's3:ObjectCreated:Put');
                    });
                return next();
            },
            next => {
                // per object ordering, per arn: the delivered kafka key is
                // bucket/object, so one object is one partition, and the
                // ordering key of the consumer keeps its events in sequence
                aKeys.concat(bKeys).forEach(objectKey => {
                    const events = deliveredFor(objectKey);
                    assert.strictEqual(events.length, A_EVENTS_PER_KEY,
                        `${objectKey} was not delivered exactly once per ` +
                        'event');
                    assert.strictEqual(
                        new Set(events.map(e => e.partition)).size, 1,
                        `${objectKey} was spread over more than one ` +
                        'partition of the assumed destination');
                    const times = events.map(e => e.eventTime);
                    assert.deepStrictEqual(times, [...times].sort(),
                        `${objectKey} was delivered out of order: ` +
                        `${times.join(', ')}`);
                });
                record('AD-A.perKeyOrder.keysChecked',
                    aKeys.length + bKeys.length);
                return next();
            },
            next => {
                // measured, not asserted: two arns collapsed onto one
                // destination share a kafka key but not an ordering key, so
                // the design promises nothing here. The number says whether
                // it held in practice on this rig
                const events = deliveredFor(sharedKey);
                const times = events.map(e => e.eventTime);
                record('AD-A.sharedObjectKey.delivered', events.length);
                record('AD-A.sharedObjectKey.partitions',
                    [...new Set(events.map(e => e.partition))]);
                record('AD-A.sharedObjectKey.orderHeld',
                    JSON.stringify(times) ===
                        JSON.stringify([...times].sort()));
                record('AD-A.sharedObjectKey.eventTimes', times);
                assert.strictEqual(events.length,
                    2 * A_SHARED_EVENTS_PER_ARN,
                    'the collapsed object key lost or gained an event');
                return next();
            },
            next => readCounter(DELIVERED_METRIC, {
                workgroup: activeIds.legacy,
                target: assumed.resource,
                assumed: 'true',
            }, (err2, value) => {
                assert.ifError(err2);
                record('AD-A.metrics.deliveredUnderAssumption', value);
                assert.strictEqual(value, legacyOwned,
                    'the assumed deliveries are not attributed to the ' +
                    'assumed destination under an assumed label');
                return next();
            }),
            next => readSeries(DELIVERED_METRIC,
                { workgroup: activeIds.legacy }, (err2, values) => {
                    assert.ifError(err2);
                    record('AD-A.metrics.legacyTargets',
                        values.map(v => v.labels.target));
                    assert.deepStrictEqual(
                        [...new Set(values.map(v => v.labels.target))],
                        [assumed.resource],
                        'the assuming workgroup attributed a delivery to a ' +
                        'destination it never delivered to');
                    return next();
                }),
            next => readCounter(SKIPPED_METRIC, {
                workgroup: activeIds.legacy,
                reason: 'not_in_slice',
                assumed: 'true',
            }, (err2, value) => {
                assert.ifError(err2);
                record('AD-A.metrics.legacySkipped', value);
                assert.strictEqual(value, autoOwned,
                    'the slice filter no longer decides what an assuming ' +
                    'workgroup consumes');
                return next();
            }),
            next => readSeries(DELIVERED_METRIC,
                { workgroup: activeIds.auto, target: autoC.resource },
                (err2, values) => {
                    assert.ifError(err2);
                    assert.strictEqual(values.length, 1);
                    record('AD-A.metrics.autoDelivered', values[0].value);
                    assert.strictEqual(values[0].value, autoOwned);
                    assert.strictEqual(values[0].labels.assumed, undefined,
                        'a workgroup that delivers where the record says so ' +
                        'carried the assumed label');
                    return next();
                }),
            next => async.map([activeIds.legacy, activeIds.auto],
                (workgroupId, counted) => readCounter(DROPPED_METRIC,
                    { workgroup: workgroupId }, counted),
                (err2, values) => {
                    assert.ifError(err2);
                    const dropped = values.reduce((a, b) => a + b, 0);
                    record('AD-A.metrics.dropped', dropped);
                    assert.strictEqual(dropped, 0,
                        'a record was dropped: under assumption there is no ' +
                        'registry lookup to fail');
                    return next();
                }),
            next => readSeries(ASSUMED_METRIC,
                { workgroup: activeIds.legacy, target: assumed.resource },
                (err2, values) => {
                    assert.ifError(err2);
                    assert.strictEqual(values.length, 1,
                        'the redirection is not in the metrics');
                    assert.strictEqual(values[0].value, 1);
                    record('AD-A.metrics.assumedGauge',
                        `${values[0].labels.workgroup} -> ` +
                        `${values[0].labels.target}`);
                    return next();
                }),
        ], finish);
    })));
});

describe('GATE AD-B :: what an assume-destination workgroup does with the ' +
'shared legacy topic', function gateSharedLegacyTopic() {
    this.timeout(1800000);

    const delivery = DELIVERY_TOPICS.b;
    const baseGroupId = `poc-ad-b-group-${RUN_ID}`;
    const zkBase = `${ZK_BASE}/gate-b`;

    const baseIds = {
        legacy: 'adb-legacy',
        autoZero: 'adb-auto-0',
        autoOne: 'adb-auto-1',
    };
    const activeIds = { ...baseIds };
    const autoIdsOf = ids => [ids.autoZero, ids.autoOne];

    const legacyCResource = `poc-ad-legacy-c-${RUN_ID}`;
    const assumedResource = `poc-ad-b-assumed-${RUN_ID}`;

    function attemptSetup(attempt) {
        const suffix = `${RUN_ID}-a${attempt}`;
        const legacyC = destinationConfig({
            resource: legacyCResource,
            topic: `poc-ad-b-legacy-c-own-${suffix}`,
        });
        const assumed = destinationConfig({
            resource: assumedResource,
            topic: `poc-ad-b-assumed-${suffix}`,
        });
        return {
            legacyC,
            assumed,
            destinations: [legacyC, assumed],
            topics: [
                { name: assumed.topic, partitions: 2 },
                { name: legacyC.topic, partitions: 1 },
            ],
            notifConfig: {
                destinations: [legacyC],
                deliveryPool: deliveryPoolConfig({
                    topic: delivery.name,
                    groupId: baseGroupId,
                    concurrency: 10,
                }),
            },
        };
    }

    function buildDocument(ids, setup) {
        const { error, value } = validateWorkgroupsDoc({
            configVersion: CONFIG_VERSION,
            generation: 1,
            topic: delivery.name,
            updatedAt: new Date().toISOString(),
            workgroups: [
                { id: ids.autoZero,
                    rule: { type: 'hashmod', modulo: 2, remainders: [0] } },
                { id: ids.autoOne,
                    rule: { type: 'hashmod', modulo: 2, remainders: [1] } },
                { id: ids.legacy,
                    submode: ASSUME_DESTINATION,
                    rule: { type: 'static',
                        destinationIds: [legacyCResource] },
                    assumedDestination: {
                        resource: setup.assumed.resource,
                        type: setup.assumed.type,
                        host: setup.assumed.host,
                        port: setup.assumed.port,
                        topic: setup.assumed.topic,
                    } },
            ],
        });
        assert.ifError(error);
        return value;
    }

    // several buckets, because the key of a legacy record is bucket/object
    // and that is the only thing a worker can route it by
    const legacyBuckets = [];
    for (let i = 0; i < B_BUCKETS; i++) {
        legacyBuckets.push(`poc-ad-b-bucket-${i}-${RUN_ID}`);
    }
    const legacyObjectKeys = [];
    for (let i = 0; i < B_KEYS_PER_BUCKET; i++) {
        legacyObjectKeys.push(`legacy-obj-${`${i}`.padStart(3, '0')}`);
    }
    const addressedBucket = `poc-ad-b-addressed-${RUN_ID}`;
    const addressedKeys = [];
    for (let i = 0; i < B_ADDRESSED_KEYS; i++) {
        addressedKeys.push(`addressed-obj-${`${i}`.padStart(3, '0')}`);
    }

    const unstampedCount = B_BUCKETS * B_KEYS_PER_BUCKET;
    const addressedCount = B_ADDRESSED_KEYS * B_ADDRESSED_EVENTS_PER_KEY;
    const totalRecords = unstampedCount + addressedCount;

    let active = null;
    let tailers = new Map();
    let runtimes = [];

    /**
     * The workgroup the document's own ownership function gives each
     * unstamped record to. A legacy record carries no destination, so the
     * routing token is its whole bucket/object key, and the total coverage
     * rule is the only thing that gives it an owner at all.
     *
     * @param {Object} doc - validated document
     * @return {Object} owner id to number of unstamped records
     */
    function predictUnstampedOwners(doc) {
        const counts = {};
        legacyBuckets.forEach(bucket => legacyObjectKeys.forEach(key => {
            const owner = workgroupIdForDestination(doc, `${bucket}/${key}`);
            counts[owner] = (counts[owner] || 0) + 1;
        }));
        return counts;
    }

    function tailerOf(destination) {
        return tailers.get(destination.topic);
    }

    function startTailers(setup, done) {
        tailers = new Map();
        return async.eachSeries(setup.destinations, (destination, next) => {
            const tailer = new TopicTailer(destination.topic);
            tailers.set(destination.topic, tailer);
            return tailer.start(next);
        }, done);
    }

    function stopTailers(done) {
        return async.eachSeries([...tailers.values()],
            (tailer, next) => tailer.stop(next), () => done());
    }

    before(done => {
        const plan = attemptSetup(1);
        const records = [];
        legacyBuckets.forEach(bucket => legacyObjectKeys.forEach((key, i) =>
            records.push(legacyRecord({
                bucket, key, dateTime: eventTime(i),
            }))));
        for (let i = 0; i < B_ADDRESSED_EVENTS_PER_KEY; i++) {
            addressedKeys.forEach(key => records.push(addressedRecord({
                destination: plan.legacyC,
                bucket: addressedBucket,
                key,
                dateTime: eventTime(i),
            })));
        }
        assert.strictEqual(records.length, totalRecords);
        record('AD-B.records.unstamped', unstampedCount);
        record('AD-B.records.addressedToLegacyArn', addressedCount);
        record('AD-B.records.total', totalRecords);
        record('AD-B.predictedUnstampedOwners',
            predictUnstampedOwners(buildDocument(baseIds, plan)));
        return async.series([
            next => createAndWaitForTopics('b-delivery', [delivery], next),
            next => produceRecords(delivery.name, records, next),
        ], done);
    });

    after(done => async.series([
        next => stopWorkgroups(runtimes, next),
        next => stopTailers(next),
    ], done));

    it('should leave every unstamped record to the hashmod fallback and ' +
    'deliver none of them to the assumed destination',
    done => runGuarded(done, finish => withWedgeRetry({
        label: 'AD-B',
        run: (attempt, cb) => {
            Object.keys(baseIds).forEach(role => {
                activeIds[role] = attemptId(baseIds[role], attempt);
            });
            const setup = attemptSetup(attempt);
            active = setup;
            const order = [activeIds.legacy, ...autoIdsOf(activeIds)];
            const zkPath = `${zkBase}/attempt-${attempt}`;
            return async.series([
                next => createAndWaitForTopics(`b-destinations-${attempt}`,
                    setup.topics, next),
                next => startTailers(setup, next),
                next => writeWorkgroupsDocument(zkPath,
                    buildDocument(activeIds, setup), err => next(err)),
                next => startWorkgroups({
                    zkPath,
                    workgroupIds: order,
                    baseGroupId,
                    notifConfig: setup.notifConfig,
                    deliveryPartitions: delivery.partitions,
                    started: runtimes,
                }, next),
                next => async.eachSeries(order, (workgroupId, waited) =>
                    waitForCommittedTotal(
                        buildGroupId(baseGroupId, workgroupId, 1),
                        delivery.name, delivery.partitions, totalRecords,
                        COMMIT_TIMEOUT_MS, waited), next),
            ], err => cb(err));
        },
        progress: (attempt, cb) => accountedFor(
            Object.keys(baseIds).map(role =>
                attemptId(baseIds[role], attempt)), cb),
        cleanup: (attempt, cb) => stopWorkgroups(runtimes, () => {
            runtimes = [];
            return stopTailers(cb);
        }),
    }, err => {
        if (err) {
            return finish(err);
        }
        const { legacyC, assumed } = active;
        const predicted = predictUnstampedOwners(
            buildDocument(activeIds, active));
        return async.series([
            next => async.eachSeries([...tailers.values()],
                (tailer, tailDone) => waitUntilQuiet(tailer, 500, tailDone),
                next),
            next => {
                record('AD-B.assumedTopic.delivered',
                    tailerOf(assumed).records.length);
                record('AD-B.registryTopic.delivered',
                    tailerOf(legacyC).records.length);
                assert.strictEqual(tailerOf(assumed).records.length,
                    addressedCount,
                    'the assumed destination received something other than ' +
                    'the records addressed to the arn it was given');
                tailerOf(assumed).records.map(deliveredEvent)
                    .forEach(event => assert.strictEqual(event.bucket,
                        addressedBucket,
                        `an unstamped record from ${event.bucket} reached ` +
                        'the assumed destination'));
                assert.strictEqual(tailerOf(legacyC).records.length, 0);
                return next();
            },
            next => readCounter(SKIPPED_METRIC, {
                workgroup: activeIds.legacy,
                reason: 'not_in_slice',
                assumed: 'true',
            }, (err2, value) => {
                assert.ifError(err2);
                record('AD-B.metrics.legacySkippedUnstamped', value);
                assert.strictEqual(value, unstampedCount,
                    'the assuming workgroup did not skip exactly the ' +
                    'unstamped records');
                return next();
            }),
            next => readCounter(DELIVERED_METRIC, {
                workgroup: activeIds.legacy,
                target: assumed.resource,
                assumed: 'true',
            }, (err2, value) => {
                assert.ifError(err2);
                record('AD-B.metrics.legacyDelivered', value);
                assert.strictEqual(value, addressedCount);
                return next();
            }),
            next => async.eachSeries(autoIdsOf(activeIds),
                (workgroupId, counted) => readCounter(DROPPED_METRIC, {
                    workgroup: workgroupId,
                    reason: 'unknown_destination',
                    target: 'unknown',
                }, (err2, value) => {
                    assert.ifError(err2);
                    record(`AD-B.metrics.droppedBy.${workgroupId}`, value);
                    assert.strictEqual(value, predicted[workgroupId] || 0,
                        `${workgroupId} did not drop exactly the unstamped ` +
                        'records the ownership rules give it');
                    return counted();
                }), next),
            next => async.map(autoIdsOf(activeIds),
                (workgroupId, counted) => readCounter(DROPPED_METRIC,
                    { workgroup: workgroupId }, counted),
                (err2, values) => {
                    assert.ifError(err2);
                    const dropped = values.reduce((a, b) => a + b, 0);
                    record('AD-B.metrics.droppedByFallbackTotal', dropped);
                    assert.strictEqual(dropped, unstampedCount,
                        'the unstamped records were not all accounted for ' +
                        'by the fallback workgroups');
                    return next();
                }),
            next => async.map(autoIdsOf(activeIds),
                (workgroupId, counted) => readCounter(DELIVERED_METRIC,
                    { workgroup: workgroupId }, counted),
                (err2, values) => {
                    assert.ifError(err2);
                    record('AD-B.metrics.deliveredByFallbackTotal',
                        values.reduce((a, b) => a + b, 0));
                    assert.strictEqual(values.reduce((a, b) => a + b, 0), 0,
                        'a fallback workgroup delivered an unstamped record ' +
                        'somewhere');
                    return next();
                }),
            next => {
                // the two configurations an operator would reach for to
                // drain a topic of unaddressed records through one assumed
                // destination, both refused at load. That is the answer to
                // the migration question, and it is a rule rather than an
                // accident of this run
                const assumedDestination = {
                    resource: assumed.resource,
                    type: assumed.type,
                    host: assumed.host,
                    port: assumed.port,
                    topic: assumed.topic,
                };
                const { error: onlyAssuming } = validateWorkgroupsDoc({
                    configVersion: CONFIG_VERSION,
                    generation: 1,
                    topic: delivery.name,
                    workgroups: [{
                        id: activeIds.legacy,
                        submode: ASSUME_DESTINATION,
                        rule: { type: 'static',
                            destinationIds: [legacyCResource] },
                        assumedDestination,
                    }],
                });
                const { error: assumingFallback } = validateWorkgroupsDoc({
                    configVersion: CONFIG_VERSION,
                    generation: 1,
                    topic: delivery.name,
                    workgroups: [{
                        id: activeIds.autoZero,
                        submode: ASSUME_DESTINATION,
                        rule: { type: 'hashmod', modulo: 1, remainders: [0] },
                        assumedDestination,
                    }],
                });
                record('AD-B.refusedConfigurations', {
                    onlyAnAssumingWorkgroup: onlyAssuming.message,
                    anAssumingFallback: assumingFallback.message,
                });
                assert(onlyAssuming instanceof Error);
                assert(assumingFallback instanceof Error);
                return next();
            },
        ], finish);
    })));
});
