const assert = require('assert');
const async = require('async');
const fs = require('fs');
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
const WorkgroupCutover =
    require('../../../extensions/notification/deliveryWorker/WorkgroupCutover');
const { assertSeededOffsets } =
    require('../../../extensions/notification/deliveryWorker/seededOffsets');
const { buildDeliveryKey } =
    require('../../../extensions/notification/utils/deliveryKey');
const {
    buildGroupId,
    buildOwnershipIndex,
    createSliceFilter,
    destinationTokenFromKey,
    isBarrierKey,
    ownerOfToken,
    validateWorkgroupsDoc,
    workgroupIdForDestination,
    CONFIG_VERSION,
} = require('../../../extensions/notification/utils/workgroups');

const KAFKA_HOSTS = process.env.KAFKA_HOSTS || 'localhost:9092';
// bare connection string, with no chroot: the workgroups path is a path on
// the client, never appended to the connection string
const ZOOKEEPER_HOSTS = process.env.ZOOKEEPER_HOSTS || 'localhost:2181';
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
const DELAY_METRIC =
    's3_notification_delivery_worker_delivery_delay_seconds';
const BARRIER_METRIC =
    's3_notification_delivery_worker_barrier_seen_total';

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

// every workgroup whose phase actually passed, with the document it ran, so
// the observability gate can check the label on it. Registration is explicit
// rather than a side effect of starting a worker: a workgroup that wedged
// and was retried started but never ran
const RAN_WORKGROUPS = [];

/**
 * Records that a workgroup ran a phase through to its assertions
 *
 * @param {Object} runtime - runtime returned by startWorkgroup
 * @return {undefined}
 */
function registerWorkgroup(runtime) {
    RAN_WORKGROUPS.push({ id: runtime.workgroup.id, doc: runtime.doc });
}

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
const C_CUSTOMER_COUNT = 4;

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
    bDelivery: { name: `poc-wg-b-delivery-${RUN_ID}`, partitions: 6 },
    bZeroCustomer: { name: `poc-wg-b-zero-customer-${RUN_ID}`, partitions: 1 },
    bOneCustomer0: { name: `poc-wg-b-one-customer-0-${RUN_ID}`, partitions: 1 },
    bOneCustomer1: { name: `poc-wg-b-one-customer-1-${RUN_ID}`, partitions: 1 },
    bWhaleCustomer: { name: `poc-wg-b-whale-customer-${RUN_ID}`,
        partitions: 1 },
    cDelivery: { name: `poc-wg-c-delivery-${RUN_ID}`, partitions: 4 },
};
for (let i = 0; i < C_CUSTOMER_COUNT; i++) {
    TOPICS[`cCustomer${i}`] = {
        name: `poc-wg-c-customer-${i}-${RUN_ID}`,
        partitions: 1,
    };
}
for (let i = 0; i < A_CUSTOMER_COUNT; i++) {
    TOPICS[`aCustomer${i}`] = {
        name: `poc-wg-a-customer-${i}-${RUN_ID}`,
        partitions: 1,
    };
}

// Every partition of a delivery topic carries a barrier of its own, so the
// reshard gate holds its delivery topics at the partition count gate W-C
// used: a cutover has to be shown crossing more than one or two of them
const E_DELIVERY_PARTITIONS = 4;

// GATE W-E gives each of its scenarios a delivery topic and a customer topic
// per destination of its own. Sharing them would make one scenario's records
// readable by the next one's assertions, and these scenarios deliberately
// leave records undelivered
const E_CUSTOMER_COUNTS = { e1: 7, e2: 4, e3: 4, e4: 2, e5: 0, e6: 4 };
Object.keys(E_CUSTOMER_COUNTS).forEach(scenario => {
    TOPICS[`${scenario}Delivery`] = {
        name: `poc-wg-${scenario}-delivery-${RUN_ID}`,
        partitions: E_DELIVERY_PARTITIONS,
    };
    for (let i = 0; i < E_CUSTOMER_COUNTS[scenario]; i++) {
        TOPICS[`${scenario}Customer${i}`] = {
            name: `poc-wg-${scenario}-customer-${i}-${RUN_ID}`,
            partitions: 1,
        };
    }
});

/**
 * The topics of one gate. Every key of TOPICS starts with the letter of the
 * gate that owns it.
 *
 * @param {String} prefix - gate letter
 * @return {Object[]} topics, as { name, partitions }
 */
function topicsOf(prefix) {
    return Object.keys(TOPICS)
        .filter(key => key.startsWith(prefix))
        .map(key => TOPICS[key]);
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

/**
 * Creates the topics of one gate and waits for them to propagate, before
 * that gate builds any consumer.
 *
 * Scoped to the gate rather than done once for the whole run: the root hook
 * used to create all 18 topics of every gate whatever `--grep` selected, so
 * iterating on one gate left the broker carrying the other two gates' topics
 * as well. Several hundred of those accumulate quickly, and topic count is
 * one of the things that feeds the metadata churn behind the pre-existing
 * consumer wedge. Each gate's topics still exist, and are still confirmed
 * stable, before that gate builds a consumer.
 *
 * @param {String} prefix - gate letter
 * @param {Function} done - callback
 * @return {undefined}
 */
function createGateTopics(prefix, done) {
    const topics = topicsOf(prefix);
    record(`run.topics.${prefix}`, topics.map(topic =>
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
        process.nextTick(() => done(null, []));
        return undefined;
    }
    // deliberately not returned: a promise handed back to mocha from an it()
    // is reported as "resolution method is overspecified"
    //
    // the callback is handed on through nextTick rather than called from
    // inside the promise reaction. Almost every assertion in this suite is
    // made in a callback that starts here, and an assertion thrown inside a
    // then() rejects a promise nobody is holding: mocha never sees it, the
    // case simply times out, and the message that says what was wrong is
    // gone. One gate spent its whole twenty minute timeout that way
    metric.get().then(({ values }) => {
        const matched = values.filter(v => Object.entries(labels)
            .every(([label, value]) => v.labels[label] === value));
        process.nextTick(() => done(null, matched));
    }, err => process.nextTick(() => done(err)));
    return undefined;
}

/**
 * The smallest number of records any of these workgroups delivered.
 *
 * The consumer wedge is a property of one consumer, so a phase where one
 * workgroup delivered everything and another delivered nothing is still a
 * wedge of that second workgroup, and summing the two would hide it.
 *
 * @param {String[]} workgroupIds - workgroups that all had to deliver
 * @param {Function} done - callback: done(err, delivered)
 * @return {undefined}
 */
function minDelivered(workgroupIds, done) {
    return async.map(workgroupIds, (workgroupId, next) =>
        readCounter(DELIVERED_METRIC, { workgroup: workgroupId }, next),
        (err, values) => done(err, err ? 0 : Math.min(...values)));
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
 * Produces batches on one open producer, on a timer, so a gate can run a
 * cutover while records are still arriving on the delivery topic
 */
class RecordStream {
    constructor(params) {
        this.topic = params.topic;
        this.batches = params.batches;
        this.gapMs = params.gapMs;
        this.finished = false;
        this.error = null;
        this.sent = 0;
        this._producer = null;
        this._index = 0;
    }

    start(done) {
        this._producer = new BackbeatProducer({
            kafka: kafkaConfig,
            topic: this.topic,
            pollIntervalMs: 100,
        });
        this._producer.once('error', done);
        return this._producer.once('ready', () => {
            this._producer.removeAllListeners('error');
            // BackbeatProducer emits error from its delivery report path, and
            // an unhandled error event would take the whole process down
            this._producer.on('error', () => {});
            this._sendNext();
            return done();
        });
    }

    _sendNext() {
        if (this._index >= this.batches.length) {
            this.finished = true;
            return;
        }
        const batch = this.batches[this._index];
        this._index += 1;
        this._producer.send(batch, err => {
            if (err) {
                this.error = err;
                this.finished = true;
                return;
            }
            this.sent += batch.length;
            setTimeout(() => this._sendNext(), this.gapMs);
        });
    }

    close(done) {
        const producer = this._producer;
        if (!producer) {
            return process.nextTick(done);
        }
        // closing twice is a real possibility: the gate closes the stream
        // when it finishes and the after hook closes it again
        this._producer = null;
        return producer.close(() => done());
    }
}

/**
 * Waits for something, and when the wait fails having made no progress at
 * all, restarts the workers behind it and waits once more.
 *
 * A fresh worker on the same consumer group id resumes from that group's
 * committed offset, so a pre-seeded generation never has to be seeded again.
 * See design/06-backbeatconsumer-wedge.md for what this works around.
 *
 * @param {Object} params - label, wait, progress and restart
 * @param {Function} done - callback
 * @return {undefined}
 */
function restartOnWedge(params, done) {
    const before = params.progress();
    return params.wait(err => {
        if (!err) {
            return done();
        }
        if (params.progress() > before) {
            return done(err);
        }
        WEDGES.push({
            phase: params.label,
            attempt: 1,
            error: err.message,
        });
        suiteLog.warn('a phase made no progress at all, restarting its ' +
            'workers once and recording the occurrence as the pre-existing ' +
            'consumer wedge', { phase: params.label, error: err.message });
        return params.restart(restartErr => {
            if (restartErr) {
                return done(restartErr);
            }
            return params.wait(done);
        });
    });
}

/**
 * The bucket upper bound the given quantile of a histogram's samples falls
 * in, aggregated over every label set that matches.
 *
 * Prometheus histograms carry cumulative buckets, so this resolves to a
 * bucket bound rather than to an interpolated value, which is also what an
 * operator reading the panel gets.
 *
 * @param {String} name - metric name
 * @param {Object} labels - labels the samples have to match
 * @param {Number} quantile - quantile to resolve, as 0.99
 * @param {Function} done - callback: done(err, { count, bound })
 * @return {undefined}
 */
function histogramBound(name, labels, quantile, done) {
    return readSeries(name, labels, (err, values) => {
        if (err) {
            return done(err);
        }
        const byBound = new Map();
        values.filter(v => v.labels.le !== undefined).forEach(v => {
            const bound = v.labels.le === '+Inf' ? Infinity :
                Number(v.labels.le);
            byBound.set(bound, (byBound.get(bound) || 0) + v.value);
        });
        const ordered = [...byBound.entries()]
            .map(([bound, count]) => ({ bound, count }))
            .sort((a, b) => a.bound - b.bound);
        const count = ordered.length === 0 ?
            0 : ordered[ordered.length - 1].count;
        if (count === 0) {
            return done(null, { count: 0, bound: null });
        }
        const hit = ordered.find(entry => entry.count >= quantile * count);
        return done(null, { count, bound: hit.bound });
    });
}

/**
 * Reads how far one consumer group is behind the end of a topic, from a
 * single client kept open for a whole observation window: connecting one
 * client per sample would leave dozens of kafka clients behind.
 *
 * The end of each partition is read once, at start: this gate produces
 * everything before its workers join, so the high watermark does not move
 * while the window is open. Reading it per sample instead cost about five
 * seconds a sample, which is far too coarse to say anything about a lag
 * trajectory.
 *
 * The client only ever calls committed() and queryWatermarkOffsets(), never
 * subscribe, assign or commit, so it does not join the group it reads and
 * the running worker keeps its partitions.
 */
class LagSampler {
    constructor(groupId, topic, partitionCount) {
        this.groupId = groupId;
        this.topic = topic;
        this._toppars = [];
        for (let i = 0; i < partitionCount; i++) {
            this._toppars.push({ topic, partition: i });
        }
        this._consumer = null;
        this._marks = new Map();
    }

    start(done) {
        this._consumer = new KafkaConsumer({
            'metadata.broker.list': KAFKA_HOSTS,
            'group.id': this.groupId,
            'enable.auto.commit': false,
            'enable.auto.offset.store': false,
        }, {});
        this._consumer.on('error', () => {});
        this._consumer.on('event.error', () => {});
        return this._consumer.connect({ timeout: CONNECT_TIMEOUT }, err => {
            if (err) {
                return done(err);
            }
            // the watermarks are read once, so they have to be right the
            // first time: a client that has not yet learned the topic
            // answers with zeros, and a zero end of partition would make
            // every later sample read as no lag at all
            return callOrFail(done, () => this._consumer.getMetadata({
                topic: this.topic,
                timeout: METADATA_TIMEOUT,
            }, metaErr => {
                if (metaErr) {
                    return done(metaErr);
                }
                return async.eachSeries(this._toppars, (tp, next) =>
                    callOrFail(next, () =>
                        this._consumer.queryWatermarkOffsets(this.topic,
                            tp.partition, METADATA_TIMEOUT, (wErr, marks) => {
                                if (wErr) {
                                    return next(wErr);
                                }
                                this._marks.set(tp.partition, marks);
                                return next();
                            })), eachErr => {
                    if (eachErr) {
                        return done(eachErr);
                    }
                    const end = [...this._marks.values()]
                        .reduce((total, marks) => total + marks.highOffset, 0);
                    if (end <= 0) {
                        return done(new Error('the end of every partition of' +
                            ` ${this.topic} read as ${end}, so no lag could ` +
                            'ever be observed against it'));
                    }
                    return done();
                });
            }));
        });
    }

    /**
     * @param {Function} done - callback: done(err, lag)
     * @return {undefined}
     */
    sample(done) {
        return callOrFail(done, () => this._consumer.committed(this._toppars,
            METADATA_TIMEOUT, (err, committed) => {
                if (err) {
                    return done(err);
                }
                const offsets = {};
                (committed || []).forEach(tp => {
                    offsets[tp.partition] = tp.offset;
                });
                const lag = this._toppars.reduce((total, tp) => {
                    const marks = this._marks.get(tp.partition);
                    // an unset offset means the group has delivered nothing
                    // of that partition
                    const seen = offsets[tp.partition] >= 0 ?
                        offsets[tp.partition] : marks.lowOffset;
                    return total + Math.max(0, marks.highOffset - seen);
                }, 0);
                return done(null, lag);
            }));
    }

    stop(done) {
        if (!this._consumer) {
            return process.nextTick(done);
        }
        return this._consumer.disconnect(() => done());
    }
}

/**
 * Samples a group's lag on a timer and keeps every sample, so a gate can
 * assert on the whole trajectory rather than on one lucky look
 */
class LagTrace {
    constructor(sampler, intervalMs) {
        this.samples = [];
        this.errors = 0;
        this._sampler = sampler;
        this._intervalMs = intervalMs;
        this._stopped = false;
        this._timer = null;
        this._inFlight = false;
        this._onIdle = null;
    }

    start() {
        this._tick();
    }

    _tick() {
        if (this._stopped) {
            return;
        }
        this._inFlight = true;
        this._sampler.sample((err, lag) => {
            this._inFlight = false;
            if (err) {
                this.errors += 1;
            } else {
                this.samples.push(lag);
            }
            if (this._stopped) {
                const idle = this._onIdle;
                this._onIdle = null;
                if (idle) {
                    idle();
                }
                return;
            }
            this._timer = setTimeout(() => this._tick(), this._intervalMs);
        });
    }

    /**
     * Stops sampling and waits for the sample in flight, if any.
     *
     * Two overlapping calls on one node-rdkafka client do not both come
     * back, so nothing else may read this sampler's client until the tick
     * that is already running has finished.
     *
     * @param {Function} done - callback
     * @return {undefined}
     */
    stop(done) {
        this._stopped = true;
        clearTimeout(this._timer);
        this._timer = null;
        if (!this._inFlight) {
            return process.nextTick(done);
        }
        this._onIdle = done;
        return undefined;
    }
}

/**
 * Polls a counter until it reaches the expected value
 *
 * @param {String} name - metric name
 * @param {Object} labels - labels the counter has to match
 * @param {Number} expected - value to wait for
 * @param {Number} timeoutMs - how long to wait for
 * @param {Number} pollMs - how often to look
 * @param {Function} done - callback
 * @return {undefined}
 */
function waitForCounter(name, labels, expected, timeoutMs, pollMs, done) {
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
            return done(new Error(`timed out waiting for ${name} ` +
                `${JSON.stringify(labels)} to reach ${expected}, last seen ` +
                `${lastSeen}`));
        }
        return setTimeout(check, pollMs);
    });
    return check();
}

/**
 * Builds a destination configuration pointing at a local topic.
 *
 * pollIntervalMs is not part of the destination schema, but the pool hands
 * it to the producer: without it every delivery report waits for the two
 * second default poll interval, which the tests cannot afford. A gate that
 * needs a generation to fall behind on purpose raises it instead, which
 * slows delivery without making any destination unreachable.
 *
 * @param {Object} params - resource, topic, host, port, spreadFactor and
 *   pollIntervalMs
 * @return {Object} destination configuration
 */
function destinationConfig(params) {
    return {
        resource: params.resource,
        type: 'kafka',
        host: params.host || KAFKA_HOSTS.split(':')[0],
        port: params.port || Number(KAFKA_HOSTS.split(':')[1]),
        topic: params.topic,
        auth: {},
        spreadFactor: params.spreadFactor || 1,
        pollIntervalMs: params.pollIntervalMs || 100,
    };
}

function deliveryPoolConfig(params) {
    return {
        enabled: true,
        // these suites exercise the addressed delivery topic
        source: 'delivery',
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
 * Wraps a mocha done so a chain that is still running after mocha has
 * already reported its case cannot throw into the next one.
 *
 * These gates hold kafka clients that keep calling back after a case times
 * out, and an assertion raised on one of those late callbacks lands on
 * whichever case is running by then. That happened: an isolation gate
 * failure surfaced inside the cutover gate's before hook and took it down
 * with it.
 *
 * @param {Function} done - mocha callback
 * @return {Function} callback that drops everything after the first call
 */
function runGuarded(done, body) {
    return body(settleOnce(done));
}

function settleOnce(done) {
    let settled = false;
    return err => {
        if (settled) {
            suiteLog.warn('a gate called back after its case had already ' +
                'been reported, dropping the result', {
                error: err && err.message,
            });
            return;
        }
        settled = true;
        done(err);
    };
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
// each gate creates its own topics, so this hook only arms the uncaught
// filter and names the run
before(() => {
    installUncaughtFilter();
    record('run.id', RUN_ID);
});

// lib/BackbeatConsumer.js:850-853 is the commit path, and it throws out of
// two different places when an entry finishes while its consumer is between
// assignments or closing. Both are "Local: Erroneous state" raised
// synchronously by node-rdkafka:
//
//   onEntryCommittable -> isPaused -> KafkaConsumer.subscription
//   onEntryCommittable -> KafkaConsumer.offsetsStore
//
// The second is guarded by the first, and the guard raises the very
// exception it exists to avoid. The source comment there already flags it.
//
// In a service this takes the process down. Here it lands on whichever mocha
// case is running, which pre-empts this suite's own wedge handling: in one
// run the retry fired seven seconds after the case had already been failed.
// Anything that comes through onEntryCommittable is therefore taken out of
// mocha's hands and counted; every other uncaught exception goes straight
// back to the listeners mocha installed.
//
// Pre-existing and out of scope, of a piece with
// design/06-backbeatconsumer-wedge.md. Worked around, never hidden: every
// occurrence is reported in run.commitPathThrows.
const OFFSET_STORE_THROWS = [];
let mochaUncaught = [];

function installUncaughtFilter() {
    mochaUncaught = process.listeners('uncaughtException');
    process.removeAllListeners('uncaughtException');
    process.on('uncaughtException', (err, origin) => {
        const stack = (err && err.stack) || '';
        if (stack.includes('BackbeatConsumer.onEntryCommittable')) {
            OFFSET_STORE_THROWS.push({
                error: err.message,
                // which of the two raised it, so the report can tell them
                // apart without the whole stack
                via: stack.includes('KafkaConsumer.offsetsStore') ?
                    'offsetsStore' : 'isPaused',
            });
            suiteLog.error('a pre-existing throw escaped the consumer commit ' +
                'path, counted rather than failing the case', {
                error: err.message,
            });
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

// a gate that fails has to say why in the run output rather than only in
// mocha's epilogue: these runs are long, and a later case hanging would
// otherwise take the reason for the earlier failure with it
afterEach(function logFailure() {
    const test = this.currentTest;
    if (!test || test.state !== 'failed') {
        return;
    }
    suiteLog.error('a workgroups gate case failed', {
        title: test.title,
        error: test.err && test.err.message,
        stack: test.err && test.err.stack,
    });
});

after(() => {
    record('run.wedgeOccurrences', WEDGES);
    record('run.commitPathThrows', OFFSET_STORE_THROWS);
});

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
            next => createGateTopics('a', next),
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
    'every other record and touch nothing else', done => runGuarded(done, finish => withWedgeRetry({
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
        if (err) {
            return finish(err);
        }
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
                    registerWorkgroup(zeroRuntime);
                    return next();
                }),
        ], finish);
    })));

    it('should deliver every destination to its exact count once the other ' +
    'workgroups join, and spread the whale over several partitions',
    done => runGuarded(done, finish => withWedgeRetry({
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
        progress: (attempt, cb) => minDelivered(
            [attemptId(baseIds.one, attempt),
                attemptId(baseIds.whale, attempt)], cb),
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
        if (err) {
            return finish(err);
        }
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
                registerWorkgroup(oneRuntime);
                registerWorkgroup(whaleRuntime);
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
        ], finish);
    })));

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

describe('GATE W-B :: a blocked workgroup does not touch the others',
function gateIsolation() {
    this.timeout(600000);

    const deliveryTopic = TOPICS.bDelivery.name;
    const deliveryPartitions = TOPICS.bDelivery.partitions;
    const baseGroupId = `poc-wg-b-group-${RUN_ID}`;
    const zkBase = `${ZK_BASE}/gate-b`;
    const healthyCount = 6;
    // fewer records than the pool has lanes: the blocked destination has to
    // hold offsets, not saturate the worker. A saturated worker stops
    // polling and the broker evicts it, which is a different failure and
    // not the one this gate is about
    const blackholeCount = 6;
    const whaleKeysPerSubKey = 2;
    const LAG_SAMPLE_MS = 500;
    const WORKER_SETTLE_MS = 1500;
    const MIN_WINDOW_MS = 10000;
    // the pooled producer to an unroutable host gives up on the thirty
    // second node-rdkafka connect timeout, so the window ends well inside it
    const MAX_DRAIN_WAIT_MS = 20000;

    const baseIds = { zero: 'wgb-zero', one: 'wgb-one', whale: 'wgb-whale' };
    const activeIds = { ...baseIds };

    const whaleResource = `poc-wg-b-whale-dest-${RUN_ID}`;
    const whale = destinationConfig({
        resource: whaleResource,
        topic: TOPICS.bWhaleCustomer.name,
        spreadFactor: 6,
    });

    const planDoc = buildSlicedDocument({
        topic: deliveryTopic,
        generation: 1,
        ids: baseIds,
        whaleResource,
    });
    const selected = selectDestinations(planDoc, 'poc-wg-b-dest', [
        { workgroupId: baseIds.zero, count: 2 },
        { workgroupId: baseIds.one, count: 2 },
    ]);
    const [zeroHealthyResource, blackholeResource] =
        selected.get(baseIds.zero);
    const oneResources = selected.get(baseIds.one);

    const zeroHealthy = destinationConfig({
        resource: zeroHealthyResource,
        topic: TOPICS.bZeroCustomer.name,
    });
    const blackhole = destinationConfig({
        resource: blackholeResource,
        // TEST-NET-1, so the packets go nowhere and the producer only fails
        // when the node-rdkafka connect timeout expires
        host: '192.0.2.1',
        port: 9092,
        topic: `poc-wg-b-blackhole-topic-${RUN_ID}`,
    });
    const oneDestinations = oneResources.map((resource, index) =>
        destinationConfig({
            resource,
            topic: TOPICS[`bOneCustomer${index}`].name,
        }));
    const unblocked = oneDestinations.concat([whale]);
    const allDestinations = [zeroHealthy, blackhole]
        .concat(oneDestinations).concat([whale]);

    const notifConfig = {
        destinations: allDestinations,
        deliveryPool: deliveryPoolConfig({
            topic: deliveryTopic,
            groupId: baseGroupId,
            // the joi minimum: a record that cannot be delivered expires
            // instead of holding its offset forever
            deliveryTimeoutMs: 6000,
            concurrency: 10,
        }),
    };

    const whaleKeys = keysCoveringEverySubKey(whale, 'b-whale-obj',
        whaleKeysPerSubKey);
    const whaleCount = whaleKeys.length;
    const oneOwned = oneDestinations.length * healthyCount;

    const tailers = new Map();
    let zeroRuntime = null;
    let oneRuntime = null;
    let whaleRuntime = null;
    let blockedSampler = null;
    let drainedSampler = null;
    let trace = null;

    function tailerOf(destination) {
        return tailers.get(destination.topic);
    }

    function writeDocument(zkPath, done) {
        return writeWorkgroupsDocument(zkPath, buildSlicedDocument({
            topic: deliveryTopic,
            generation: 1,
            ids: activeIds,
            whaleResource,
        }), done);
    }

    function unblockedDrained() {
        return oneDestinations.every(destination =>
            tailerOf(destination).records.length >= healthyCount) &&
            tailerOf(whale).records.length >= whaleCount;
    }

    before(done => {
        const records = [];
        const rounds = Math.max(healthyCount, blackholeCount);
        for (let i = 0; i < rounds; i++) {
            if (i < healthyCount) {
                [zeroHealthy].concat(oneDestinations).forEach(
                    (destination, index) => records.push(addressedRecord({
                        destination,
                        key: `b${index}-obj-${`${i}`.padStart(3, '0')}`,
                        eventType: 's3:ObjectCreated:Put',
                        dateTime: eventTime(i),
                    })));
            }
            if (i < blackholeCount) {
                records.push(addressedRecord({
                    destination: blackhole,
                    key: `b-black-obj-${`${i}`.padStart(3, '0')}`,
                    eventType: 's3:ObjectCreated:Put',
                    dateTime: eventTime(i),
                }));
            }
        }
        whaleKeys.forEach((key, index) => records.push(addressedRecord({
            destination: whale,
            key,
            eventType: 's3:ObjectCreated:Put',
            dateTime: eventTime(index),
        })));
        record('W-B.destinations.zeroHealthy', zeroHealthyResource);
        record('W-B.destinations.blackhole', blackholeResource);
        record('W-B.destinations.one', oneResources);
        record('W-B.destinations.whale', whaleResource);
        record('W-B.records.total', records.length);
        record('W-B.records.blackhole', blackholeCount);
        record('W-B.records.ownedByOne', oneOwned);
        record('W-B.records.ownedByWhale', whaleCount);
        return async.series([
            next => createGateTopics('b', next),
            next => produceRecords(deliveryTopic, records, next),
            next => async.eachSeries(
                [zeroHealthy].concat(unblocked), (destination, tailDone) => {
                    const tailer = new TopicTailer(destination.topic);
                    tailers.set(destination.topic, tailer);
                    return tailer.start(tailDone);
                }, next),
        ], done);
    });

    after(done => async.series([
        next => (trace ? trace.stop(next) : next()),
        next => (blockedSampler ? blockedSampler.stop(next) : next()),
        next => (drainedSampler ? drainedSampler.stop(next) : next()),
        next => stopWorkgroup(zeroRuntime, next),
        next => stopWorkgroup(oneRuntime, next),
        next => stopWorkgroup(whaleRuntime, next),
        next => async.eachSeries([...tailers.values()],
            (tailer, tailDone) => tailer.stop(tailDone), next),
    ], done));

    /**
     * Restarts any unblocked workgroup that delivered nothing at all, on the
     * consumer group it already has.
     *
     * A wedged consumer committed nothing, so a fresh worker on that same
     * group reads from the earliest offset and delivers that workgroup's
     * records exactly once. Renaming the workgroup instead, which is what
     * the generic retry does, would make a workgroup that had already
     * delivered do so a second time and break the exact counts this gate is
     * stated in.
     *
     * @param {Function} done - callback
     * @return {undefined}
     */
    function restartWedgedWorkgroups(done) {
        const zkPath = `${zkBase}/attempt-1`;
        return async.eachSeries([
            { id: activeIds.one, get: () => oneRuntime,
                set: runtime => { oneRuntime = runtime; } },
            { id: activeIds.whale, get: () => whaleRuntime,
                set: runtime => { whaleRuntime = runtime; } },
        ], (entry, next) => readCounter(DELIVERED_METRIC,
            { workgroup: entry.id }, (err, delivered) => {
                if (err) {
                    return next(err);
                }
                if (delivered > 0) {
                    return next();
                }
                WEDGES.push({
                    phase: `W-B ${entry.id}`,
                    attempt: 1,
                    error: 'the workgroup delivered nothing at all',
                });
                suiteLog.warn('a workgroup consumed nothing at all, ' +
                    'restarting it on its own group and recording the ' +
                    'occurrence as the pre-existing consumer wedge', {
                    workgroup: entry.id,
                });
                return stopWorkgroup(entry.get(), () => startWorkgroup({
                    zkPath,
                    workgroupId: entry.id,
                    baseGroupId,
                    notifConfig,
                }, (startErr, runtime) => {
                    entry.set(runtime);
                    return next(startErr);
                }));
            }), done);
    }

    it('should drain the unblocked workgroups while the blocked one keeps ' +
    'lag on the delivery topic', done => runGuarded(done, finish => withWedgeRetry({
        label: 'W-B',
        run: (attempt, cb) => {
            const zkPath = `${zkBase}/attempt-1`;
            let windowStart = 0;
            return async.waterfall([
                // the delivery topic was created and verified in the root
                // hook, a minute of wall clock before these workers join.
                // The broker intermittently answers "unknown topic or
                // partition" for it anyway, which drops it out of the
                // effective subscription and starts the rebalance loop of
                // design/06-backbeatconsumer-wedge.md, so its metadata is
                // confirmed stable again immediately before the join
                next => waitForTopics([TOPICS.bDelivery], err => next(err)),
                next => writeDocument(zkPath, err => next(err)),
                next => startWorkgroup({
                    zkPath,
                    workgroupId: activeIds.zero,
                    baseGroupId,
                    notifConfig,
                }, next),
                (runtime, next) => {
                    zeroRuntime = runtime;
                    // a deployment does not start every worker in the same
                    // millisecond, and three joins at once widen the
                    // metadata churn window the pre-existing wedge lives in
                    return setTimeout(next, WORKER_SETTLE_MS);
                },
                next => startWorkgroup({
                    zkPath,
                    workgroupId: activeIds.one,
                    baseGroupId,
                    notifConfig,
                }, next),
                (runtime, next) => {
                    oneRuntime = runtime;
                    return setTimeout(next, WORKER_SETTLE_MS);
                },
                next => startWorkgroup({
                    zkPath,
                    workgroupId: activeIds.whale,
                    baseGroupId,
                    notifConfig,
                }, next),
            ], (err, runtime) => {
                whaleRuntime = runtime || whaleRuntime;
                if (err) {
                    return cb(err);
                }
                return async.series([
                    // the blocked destination holds its offsets only while
                    // its producer connect is outstanding, about thirty
                    // seconds from when the blocked worker started, so the
                    // window has to open promptly and stay short. The
                    // sampler is connected before the clock starts: its
                    // startup reads are slow enough to matter
                    next => {
                        blockedSampler = new LagSampler(
                            zeroRuntime.workgroup.groupId, deliveryTopic,
                            deliveryPartitions);
                        return blockedSampler.start(next);
                    },
                    next => {
                        trace = new LagTrace(blockedSampler, LAG_SAMPLE_MS);
                        windowStart = Date.now();
                        trace.start();
                        return next();
                    },
                    next => waitFor(() => 'the unblocked workgroups to ' +
                        `drain (${oneDestinations.map(d =>
                            tailerOf(d).records.length).join(', ')} and ` +
                        `${tailerOf(whale).records.length})`,
                        unblockedDrained, MAX_DRAIN_WAIT_MS,
                        drainErr => {
                            if (!drainErr) {
                                return next();
                            }
                            return restartWedgedWorkgroups(restartErr => {
                                if (restartErr) {
                                    return next(restartErr);
                                }
                                return waitFor(() => 'the restarted ' +
                                    'workgroups to drain (' +
                                    `${oneDestinations.map(d =>
                                        tailerOf(d).records.length)
                                        .join(', ')} and ` +
                                    `${tailerOf(whale).records.length})`,
                                    unblockedDrained, MAX_DRAIN_WAIT_MS,
                                    next);
                            });
                        }),
                    // the guarantee is about the whole window, not about one
                    // lucky look, so the window has a floor of its own
                    next => setTimeout(next, Math.max(0,
                        MIN_WINDOW_MS - (Date.now() - windowStart))),
                    next => {
                        record('W-B.window.ms', Date.now() - windowStart);
                        // the chosen-moment reads use the same clients, so
                        // the trace has to be off and idle first
                        return trace.stop(next);
                    },
                    // connected only now: its startup reads would otherwise
                    // sit inside the window and push it past the hold
                    next => {
                        drainedSampler = new LagSampler(
                            oneRuntime.workgroup.groupId, deliveryTopic,
                            deliveryPartitions);
                        return drainedSampler.start(next);
                    },
                    // both groups read at the same moment, which is what
                    // makes per-workgroup lag independently readable
                    next => blockedSampler.sample((sampleErr, lag) => {
                        record('W-B.lag.blockedAtChosenMoment', lag);
                        return next(sampleErr);
                    }),
                    next => drainedSampler.sample((sampleErr, lag) => {
                        record('W-B.lag.drainedAtChosenMoment', lag);
                        return next(sampleErr);
                    }),
                ], seriesErr => trace.stop(() => cb(seriesErr)));
            });
        },
        // the blocked workgroup is meant not to drain, so only the two
        // unblocked ones say whether a consumer wedged
        progress: (attempt, cb) => minDelivered(
            [attemptId(baseIds.one, attempt),
                attemptId(baseIds.whale, attempt)], cb),
        cleanup: (attempt, cb) => async.series([
                next => (trace ? trace.stop(next) : next()),
                next => (blockedSampler ? blockedSampler.stop(next) : next()),
                next => (drainedSampler ? drainedSampler.stop(next) : next()),
                next => stopWorkgroup(zeroRuntime, () => {
                    zeroRuntime = null;
                    return next();
                }),
                next => stopWorkgroup(oneRuntime, () => {
                    oneRuntime = null;
                    return next();
                }),
                next => stopWorkgroup(whaleRuntime, () => {
                    whaleRuntime = null;
                    return next();
                }),
        ], () => {
            blockedSampler = null;
            drainedSampler = null;
            trace = null;
            return cb();
        }),
    }, err => {
        if (err) {
            return finish(err);
        }
        return async.series([
            next => async.eachSeries(unblocked, (destination, destDone) =>
                waitUntilQuiet(tailerOf(destination), 500, destDone), next),
            next => {
                oneDestinations.forEach(destination =>
                    assert.strictEqual(tailerOf(destination).records.length,
                        healthyCount,
                        `${destination.resource} did not drain to its exact ` +
                        'count while another workgroup was blocked'));
                assert.strictEqual(tailerOf(whale).records.length, whaleCount,
                    'the whale did not drain to its exact count while ' +
                    'another workgroup was blocked');
                record('W-B.lag.trajectory', trace.samples);
                record('W-B.lag.samples', trace.samples.length);
                record('W-B.lag.min', Math.min(...trace.samples));
                record('W-B.lag.max', Math.max(...trace.samples));
                record('W-B.lag.first', trace.samples[0]);
                record('W-B.lag.last',
                    trace.samples[trace.samples.length - 1]);
                assert(trace.samples.length >= 8,
                    'the lag trajectory is too short to say anything, ' +
                    `${trace.samples.length} samples`);
                assert(trace.samples.every(lag => lag > 0),
                    'the blocked workgroup drained during the window, so ' +
                    'nothing was observed about isolation');
                return next();
            },
            // the blocked workgroup is blocked, not wedged: it is still
            // classifying and committing the records it does not own
            next => readCounter(SKIPPED_METRIC,
                { workgroup: activeIds.zero, reason: 'not_in_slice' },
                (err2, value) => {
                    assert.ifError(err2);
                    record('W-B.blocked.skippedNotInSlice', value);
                    assert(value > 0,
                        'the blocked workgroup consumed nothing at all, ' +
                        'which is a wedge rather than a block');
                    return next();
                }),
            next => readCounter(DELIVERED_METRIC,
                { target: zeroHealthyResource }, (err2, value) => {
                    assert.ifError(err2);
                    record('W-B.blocked.healthyDelivered', value);
                    [zeroRuntime, oneRuntime, whaleRuntime]
                        .forEach(registerWorkgroup);
                    return next();
                }),
        ], finish);
    })));

    it('should deliver nothing at all to the blackholed destination',
    done => {
        readCounter(DELIVERED_METRIC, { target: blackholeResource },
            (err, value) => {
                assert.ifError(err);
                record('W-B.blackhole.delivered', value);
                assert.strictEqual(value, 0,
                    'a destination that cannot be reached cannot have ' +
                    'delivered');
                return done();
            });
    });

    it('should keep the unblocked workgroups under a second at the 99th ' +
    'percentile of their delivery delay', done => async.eachSeries(
        [activeIds.one, activeIds.whale], (workgroupId, next) =>
            histogramBound(DELAY_METRIC, { workgroup: workgroupId }, 0.99,
                (err, result) => {
                    assert.ifError(err);
                    record(`W-B.p99.${workgroupId}.count`, result.count);
                    record(`W-B.p99.${workgroupId}.bound`, result.bound);
                    assert(result.count > 0,
                        `${workgroupId} recorded no delivery delay at all`);
                    assert(result.bound <= 1,
                        `${workgroupId} p99 delivery delay fell in the ` +
                        `${result.bound} second bucket`);
                    return next();
                }), done));
});

describe('GATE W-C :: a real cutover from the single pool to generation 1',
function gateGenerationSwap() {
    this.timeout(900000);

    const deliveryTopic = TOPICS.cDelivery.name;
    const deliveryPartitions = TOPICS.cDelivery.partitions;
    const baseGroupId = `poc-wg-c-group-${RUN_ID}`;
    const zkPath = `${ZK_BASE}/gate-c`;
    const cachePath = cachePathFor('gate-c-cutover');
    const objectsPerDestination = 6;
    const BATCH_SIZE = 3;
    const BATCH_GAP_MS = 400;
    const CUTOVER_DELAY_MS = 2000;
    const DRAIN_POLL_MS = 2000;
    const DRAIN_TIMEOUT_MS = 60000;

    const ids = { zero: 'wgc-zero', one: 'wgc-one' };
    const eventTypes = ['s3:ObjectCreated:Put', 's3:ObjectCreated:Put',
        's3:ObjectRemoved:Delete'];
    const roundTimes = eventTypes.map((_, index) => eventTime(index));

    // this document is never written: it only proposes destination names, so
    // that the cutover the tool actually runs puts two of them in each
    // workgroup
    const { error: planError, value: planDoc } = validateWorkgroupsDoc({
        configVersion: CONFIG_VERSION,
        generation: 1,
        topic: deliveryTopic,
        workgroups: [
            { id: ids.zero,
                rule: { type: 'hashmod', modulo: 2, remainders: [0] } },
            { id: ids.one,
                rule: { type: 'hashmod', modulo: 2, remainders: [1] } },
        ],
    });
    assert.ifError(planError);
    const selected = selectDestinations(planDoc, 'poc-wg-c-dest', [
        { workgroupId: ids.zero, count: 2 },
        { workgroupId: ids.one, count: 2 },
    ]);
    const destinations = selected.get(ids.zero).concat(selected.get(ids.one))
        .map((resource, index) => destinationConfig({
            resource,
            topic: TOPICS[`cCustomer${index}`].name,
        }));

    const poolNotifConfig = {
        destinations,
        deliveryPool: deliveryPoolConfig({
            topic: deliveryTopic,
            groupId: baseGroupId,
            concurrency: 10,
        }),
    };
    // the same pool config, plus the block that turns workgroups on. The
    // flag-off worker above never sees it, which is what makes its series
    // the flag-off ones
    const cutoverNotifConfig = {
        destinations,
        deliveryPool: {
            ...deliveryPoolConfig({
                topic: deliveryTopic,
                groupId: baseGroupId,
                concurrency: 10,
            }),
            workgroups: { zookeeperPath: zkPath, cachePath },
        },
    };

    const objectKeysOf = index => {
        const keys = [];
        for (let i = 0; i < objectsPerDestination; i++) {
            keys.push(`c${index}-obj-${`${i}`.padStart(3, '0')}`);
        }
        return keys;
    };

    const produced = new Set();
    const primingRecords = [];
    const streamBatches = [];
    eventTypes.forEach((eventType, round) => {
        const roundRecords = [];
        destinations.forEach((destination, index) =>
            objectKeysOf(index).forEach(key => {
                produced.add(`${destination.topic}|${key}|${roundTimes[round]}`);
                roundRecords.push(addressedRecord({
                    destination,
                    key,
                    eventType,
                    dateTime: roundTimes[round],
                }));
            }));
        if (round === 0) {
            // the topic is not empty when the pool worker joins, which is
            // what the lab looks like and what the wedge write up says is
            // the friendlier of the two orders
            primingRecords.push(...roundRecords);
            return;
        }
        for (let i = 0; i < roundRecords.length; i += BATCH_SIZE) {
            streamBatches.push(roundRecords.slice(i, i + BATCH_SIZE));
        }
    });
    const totalProduced = produced.size;

    const tailers = new Map();
    let poolWorker = null;
    let stream = null;
    let cutoverResult = null;
    let verifier = null;
    let boundary = new Map();
    let zeroRuntime = null;
    let oneRuntime = null;
    const drainPolls = [];
    let seededError = null;
    let generationZeroDelivered = 0;
    let drainReport = null;
    const restartedGenerationOne = new Set();

    function distinctDelivered() {
        return new Set(deliveredByGeneration()
            .map(r => `${r.topic}|${r.key}|${roundTimes[r.round]}`)).size;
    }

    function deliveredCount() {
        return [...tailers.values()]
            .reduce((total, tailer) => total + tailer.records.length, 0);
    }

    /**
     * Every delivered record, tagged with the generation that delivered it.
     * The two generations never ran at the same time, so on each partition
     * of each customer topic the boundary offset separates them.
     *
     * @return {Object[]} delivered records with a generation
     */
    function deliveredByGeneration() {
        const all = [];
        tailers.forEach((tailer, topic) => {
            const marks = boundary.get(topic) || new Map();
            tailer.records.forEach(raw => {
                const event = deliveredEvent(raw);
                const mark = marks.get(raw.partition);
                all.push({
                    topic,
                    partition: raw.partition,
                    offset: raw.offset,
                    key: event.key,
                    round: roundTimes.indexOf(event.eventTime),
                    generation: mark !== undefined && raw.offset <= mark ? 0 : 1,
                });
            });
        });
        return all;
    }

    function snapshotBoundary() {
        const snapshot = new Map();
        tailers.forEach((tailer, topic) => {
            const byPartition = new Map();
            tailer.records.forEach(raw => {
                const seen = byPartition.get(raw.partition);
                if (seen === undefined || raw.offset > seen) {
                    byPartition.set(raw.partition, raw.offset);
                }
            });
            snapshot.set(topic, byPartition);
        });
        return snapshot;
    }

    /**
     * Restarts any generation 1 workgroup that has not seen a barrier on
     * every partition, on the consumer group it already has.
     *
     * Measuring generation 1 as a whole cannot see this: when one of the two
     * workgroups wedges and the other runs, the pair has made progress while
     * one worker is deaf. That is what happened, and the barrier wait then
     * timed out with one workgroup stuck at 1 of 4 and no wedge recorded.
     * The consumer wedge is a property of one consumer, so it is looked for
     * one workgroup at a time. See design/06-backbeatconsumer-wedge.md.
     *
     * @param {Function} done - callback
     * @return {undefined}
     */
    function restartWedgedGenerationOne(done) {
        return async.eachSeries([
            { id: ids.zero, get: () => zeroRuntime,
                set: runtime => { zeroRuntime = runtime; } },
            { id: ids.one, get: () => oneRuntime,
                set: runtime => { oneRuntime = runtime; } },
        ], (entry, next) => readCounter(BARRIER_METRIC,
            { workgroup: entry.id, match: 'current' }, (err, seen) => {
                if (err) {
                    return next(err);
                }
                if (seen >= deliveryPartitions) {
                    return next();
                }
                WEDGES.push({
                    phase: `W-C generation 1 ${entry.id}`,
                    attempt: 1,
                    error: `saw ${seen} of ${deliveryPartitions} barriers`,
                });
                restartedGenerationOne.add(entry.id);
                suiteLog.warn('a generation 1 workgroup did not reach its ' +
                    'barriers, restarting it on its own group and recording ' +
                    'the occurrence as the pre-existing consumer wedge', {
                    workgroup: entry.id,
                    barriersSeen: seen,
                });
                return stopWorkgroup(entry.get(), () => startWorkgroup({
                    zkPath,
                    workgroupId: entry.id,
                    baseGroupId,
                    notifConfig: cutoverNotifConfig,
                }, (startErr, runtime) => {
                    entry.set(runtime);
                    return next(startErr);
                }));
            }), done);
    }

    /**
     * Waits until both generation 1 workgroups have seen a barrier on every
     * partition and have redelivered what follows them
     *
     * @param {Function} done - callback
     * @return {undefined}
     */
    function generationOneReady(done) {
        return async.series([
            step => waitForCounter(BARRIER_METRIC,
                { workgroup: ids.zero, match: 'current' },
                deliveryPartitions, 90000, 500, step),
            step => waitForCounter(BARRIER_METRIC,
                { workgroup: ids.one, match: 'current' },
                deliveryPartitions, 90000, 500, step),
            step => waitFor(() => 'generation 1 to redeliver what follows ' +
                `its barriers (${deliveredCount() - generationZeroDelivered}` +
                ' so far)',
                () => deliveredCount() > generationZeroDelivered, 60000, step),
            step => waitFor(() => 'the produced set to be covered ' +
                `(${distinctDelivered()} of ${totalProduced})`,
                () => distinctDelivered() === totalProduced, 30000, step),
        ], err => done(err));
    }

    /**
     * Records what each generation 1 workgroup had done when the wait gave
     * up, so a partial wedge can be told apart from a total one
     *
     * @param {Function} done - callback, never called with an error
     * @return {undefined}
     */
    function recordGenerationOneCounters(done) {
        return async.mapSeries([ids.zero, ids.one],
            (workgroupId, next) => async.parallel({
                delivered: step => readCounter(DELIVERED_METRIC,
                    { workgroup: workgroupId }, step),
                barriers: step => readCounter(BARRIER_METRIC,
                    { workgroup: workgroupId, match: 'current' }, step),
                skipped: step => readCounter(SKIPPED_METRIC,
                    { workgroup: workgroupId }, step),
            }, next),
            (err, counters) => {
                record('W-C.generation1.countersAtTimeout',
                    err ? err.message : {
                        [ids.zero]: counters[0],
                        [ids.one]: counters[1],
                    });
                return done();
            });
    }

    function waitForDrain(startedAt, done) {
        const deadline = Date.now() + DRAIN_TIMEOUT_MS;
        const attempt = () => verifier.verify((err, report) => {
            if (err) {
                return done(err);
            }
            drainPolls.push({
                atMs: Date.now() - startedAt,
                remaining: report.rows
                    .reduce((total, row) => total + row.remaining, 0),
                drained: report.drained,
            });
            if (report.drained) {
                return done(null, report);
            }
            if (Date.now() >= deadline) {
                return done(new Error('the single pool never committed past ' +
                    'every barrier, so the cutover could not proceed'));
            }
            return setTimeout(attempt, DRAIN_POLL_MS);
        });
        return attempt();
    }

    before(done => {
        record('W-C.destinations', destinations.map(d => d.resource));
        record('W-C.records.produced', totalProduced);
        record('W-C.stream.batches', streamBatches.length);
        let cutoverStartedAt = 0;
        return async.series([
            next => createGateTopics('c', next),
            next => async.eachSeries(destinations, (destination, tailDone) => {
                const tailer = new TopicTailer(destination.topic);
                tailers.set(destination.topic, tailer);
                return tailer.start(tailDone);
            }, next),
            next => produceRecords(deliveryTopic, primingRecords, next),
            next => {
                poolWorker = new DeliveryWorker(kafkaConfig, poolNotifConfig);
                return poolWorker.start(null, next);
            },
            // the pool has to be consuming before the cutover, otherwise the
            // barriers would be the first thing it ever sees. Counted on
            // this gate's own customer topics: the delivered counter is
            // process wide and the earlier gates already moved it
            next => restartOnWedge({
                label: 'W-C single pool',
                wait: cb => waitFor(() => 'the single pool to deliver its ' +
                    `first record (${deliveredCount()} so far)`,
                    () => deliveredCount() > 0, 60000, cb),
                progress: () => deliveredCount(),
                restart: cb => stopWorker(poolWorker, () => {
                    poolWorker = new DeliveryWorker(kafkaConfig,
                        poolNotifConfig);
                    return poolWorker.start(null, cb);
                }),
            }, next),
            next => {
                stream = new RecordStream({
                    topic: deliveryTopic,
                    batches: streamBatches,
                    gapMs: BATCH_GAP_MS,
                });
                return stream.start(next);
            },
            next => setTimeout(next, CUTOVER_DELAY_MS),
            next => {
                cutoverStartedAt = Date.now();
                const cutover = new WorkgroupCutover({
                    kafkaConfig,
                    zkConfig: {
                        connectionString: ZOOKEEPER_HOSTS,
                        autoCreateNamespace: false,
                    },
                    notifConfig: cutoverNotifConfig,
                    options: {
                        modulo: 2,
                        workgroup: [`${ids.zero}:0`, `${ids.one}:1`],
                        timeout: 10000,
                    },
                    logger: new werelogs.Logger('WorkgroupCutover:ft'),
                });
                return cutover.cutover((err, result) => {
                    cutoverResult = result;
                    record('W-C.cutover.ms', Date.now() - cutoverStartedAt);
                    record('W-C.cutover.recordsStreamedSoFar', stream.sent);
                    return cutover.close(() => next(err));
                });
            },
            next => waitFor(() => `the record stream to finish (${stream.sent}` +
                ` of ${streamBatches.length * BATCH_SIZE})`,
                () => stream.finished, 60000, next),
            next => stream.close(next),
            next => {
                assert.ifError(stream.error);
                verifier = new WorkgroupCutover({
                    kafkaConfig,
                    zkConfig: {
                        connectionString: ZOOKEEPER_HOSTS,
                        autoCreateNamespace: false,
                    },
                    notifConfig: cutoverNotifConfig,
                    options: { timeout: 10000 },
                    logger: new werelogs.Logger('WorkgroupCutover:verify'),
                });
                return next();
            },
            // the drain only finishes if the single pool is still consuming,
            // so it gets the same one-shot restart as the other waits
            next => restartOnWedge({
                label: 'W-C drain of the single pool',
                wait: cb => waitForDrain(Date.now(), (err, report) => {
                    drainReport = report;
                    return cb(err);
                }),
                progress: () => deliveredCount(),
                restart: cb => stopWorker(poolWorker, () => {
                    poolWorker = new DeliveryWorker(kafkaConfig,
                        poolNotifConfig);
                    return poolWorker.start(null, cb);
                }),
            }, err => {
                if (err) {
                    return next(err);
                }
                record('W-C.drain.polls', drainPolls);
                record('W-C.drain.finalRemaining', drainReport.rows
                    .reduce((total, row) => total + row.remaining, 0));
                return next();
            }),
            // only now may the previous generation be stopped
            next => stopWorker(poolWorker, () => {
                poolWorker = null;
                return next();
            }),
            next => async.eachSeries([...tailers.values()],
                (tailer, quietDone) => waitUntilQuiet(tailer, 500, quietDone),
                next),
            next => {
                boundary = snapshotBoundary();
                generationZeroDelivered = deliveredCount();
                record('W-C.generation0.delivered', generationZeroDelivered);
                return next();
            },
            // the guard rail, on the groups the cutover really seeded
            next => async.eachSeries(cutoverResult.groupIds, (groupId, cb) =>
                assertSeededOffsets({
                    kafkaConfig,
                    topic: deliveryTopic,
                    groupId,
                    barriers: cutoverResult.doc.barriers,
                    logger: new werelogs.Logger('seededOffsets:ft'),
                }, cb), next),
            // and the same guard rail refusing a group nobody seeded
            next => assertSeededOffsets({
                kafkaConfig,
                topic: deliveryTopic,
                groupId: `poc-wg-c-never-seeded-${RUN_ID}`,
                logger: new werelogs.Logger('seededOffsets:ft'),
            }, err => {
                seededError = err;
                return next();
            }),
            next => startWorkgroup({
                zkPath,
                workgroupId: ids.zero,
                baseGroupId,
                notifConfig: cutoverNotifConfig,
            }, (err, runtime) => {
                zeroRuntime = runtime;
                return next(err);
            }),
            next => startWorkgroup({
                zkPath,
                workgroupId: ids.one,
                baseGroupId,
                notifConfig: cutoverNotifConfig,
            }, (err, runtime) => {
                oneRuntime = runtime;
                return next(err);
            }),
            // the previous generation had already delivered every produced
            // record before it was stopped, so covering the produced set
            // says nothing about generation 1. Its own barriers, one per
            // partition, and the records that follow them are what say it
            // ran. A workgroup that has not reached its barriers is
            // restarted one workgroup at a time, because a wedge of one of
            // the two is invisible in anything measured over the pair
            next => generationOneReady(waitErr => {
                if (!waitErr) {
                    return next();
                }
                return recordGenerationOneCounters(() =>
                    restartWedgedGenerationOne(restartErr => {
                        if (restartErr) {
                            return next(restartErr);
                        }
                        return generationOneReady(next);
                    }));
            }),
            next => async.eachSeries([...tailers.values()],
                (tailer, quietDone) => waitUntilQuiet(tailer, 500, quietDone),
                next),
            next => {
                [zeroRuntime, oneRuntime].forEach(registerWorkgroup);
                return next();
            },
        ], done);
    });

    after(done => async.series([
        next => (stream ? stream.close(next) : next()),
        next => stopWorker(poolWorker, next),
        next => stopWorkgroup(zeroRuntime, next),
        next => stopWorkgroup(oneRuntime, next),
        next => (verifier ? verifier.close(next) : next()),
        next => async.eachSeries([...tailers.values()],
            (tailer, tailDone) => tailer.stop(tailDone), next),
    ], done));

    it('should write generation 1 with a barrier on every partition and the ' +
    'groups it replaces', done => {
        const doc = cutoverResult.doc;
        record('W-C.doc.generation', doc.generation);
        record('W-C.doc.previousGroups', doc.previousGroups);
        record('W-C.doc.barriers', doc.barriers);
        record('W-C.doc.groupIds', cutoverResult.groupIds);
        assert.strictEqual(doc.generation, 1);
        assert.strictEqual(doc.topic, deliveryTopic);
        assert.deepStrictEqual(doc.previousGroups, [baseGroupId],
            'the document has to record the single pool as the group it ' +
            'replaces, so a later verify does not have to derive it');
        assert.strictEqual(Object.keys(doc.barriers).length,
            deliveryPartitions,
            'every partition needs a barrier or the new generation has a ' +
            'partition nobody seeded');
        assert.deepStrictEqual(cutoverResult.groupIds.slice().sort(),
            [buildGroupId(baseGroupId, ids.zero, 1),
                buildGroupId(baseGroupId, ids.one, 1)].sort());
        return done();
    });

    it('should hold the cutover until the single pool has committed past ' +
    'every barrier', done => {
        const last = drainPolls[drainPolls.length - 1];
        assert(drainReport, 'the drain report never came back');
        record('W-C.drain.pollCount', drainPolls.length);
        record('W-C.drain.firstRemaining', drainPolls[0].remaining);
        record('W-C.drain.elapsedMs', last.atMs);
        assert(last.drained,
            'the drain report never reported the previous generation drained');
        assert.strictEqual(last.remaining, 0);
        return done();
    });

    it('should deliver the union of both generations with no gap', done => {
        const all = deliveredByGeneration();
        const seen = new Set(all
            .map(r => `${r.topic}|${r.key}|${roundTimes[r.round]}`));
        const missing = [...produced].filter(triple => !seen.has(triple));
        const byGeneration = { 0: 0, 1: 0 };
        all.forEach(r => { byGeneration[r.generation] += 1; });
        record('W-C.union.produced', totalProduced);
        record('W-C.union.deliveredRecords', all.length);
        record('W-C.union.distinctDelivered', seen.size);
        record('W-C.union.duplicates', all.length - seen.size);
        record('W-C.union.byGeneration', byGeneration);
        assert.deepStrictEqual(missing, [],
            'a record the produced set holds was delivered by neither ' +
            'generation, which is the gap this design exists to prevent');
        assert.strictEqual(seen.size, totalProduced);
        assert(byGeneration[0] > 0,
            'the single pool delivered nothing, so no seam was crossed');
        assert(byGeneration[1] > 0,
            'the new generation delivered nothing, so no seam was crossed');
        return done();
    });

    it('should keep every object key in order within each generation',
    done => {
        const bySeries = new Map();
        deliveredByGeneration().forEach(r => {
            const seriesKey =
                `${r.generation}|${r.topic}|${r.partition}|${r.key}`;
            if (!bySeries.has(seriesKey)) {
                bySeries.set(seriesKey, []);
            }
            bySeries.get(seriesKey).push(r);
        });
        let checked = 0;
        bySeries.forEach((events, seriesKey) => {
            const rounds = events.slice()
                .sort((a, b) => a.offset - b.offset)
                .map(r => r.round);
            for (let i = 1; i < rounds.length; i++) {
                // no inversion. Equal neighbours are a duplicate of one
                // event, which at least once allows and which the union
                // case counts, and are not an ordering failure
                assert(rounds[i] >= rounds[i - 1],
                    `${seriesKey} was delivered out of order: ` +
                    `${rounds.join(', ')}`);
            }
            checked += 1;
        });
        record('W-C.order.seriesChecked', checked);
        assert(checked > 0);
        return done();
    });

    it('should show each new workgroup exactly one barrier of its own ' +
    'generation per partition', done => async.eachSeries([ids.zero, ids.one],
        (workgroupId, next) => readCounter(BARRIER_METRIC,
            { workgroup: workgroupId, match: 'current' }, (err, value) => {
                assert.ifError(err);
                record(`W-C.barriers.${workgroupId}.current`, value);
                if (restartedGenerationOne.has(workgroupId)) {
                    // this workgroup was restarted around the pre-existing
                    // wedge, and a restart re-reads every barrier it had not
                    // already committed past, so the exact count is no
                    // longer the invariant. The restart is recorded in
                    // run.wedgeOccurrences.
                    assert(value >= deliveryPartitions,
                        `${workgroupId} was restarted and still saw only ` +
                        `${value} barriers of its own generation`);
                    return next();
                }
                assert.strictEqual(value, deliveryPartitions,
                    `${workgroupId} saw ${value} barriers of its own ` +
                    'generation, one per partition would be ' +
                    `${deliveryPartitions}`);
                return next();
            }), err => {
        assert.ifError(err);
        return readCounter(BARRIER_METRIC, { match: 'other' },
            (err2, value) => {
                assert.ifError(err2);
                // the single pool consumed the same barriers while it was
                // still running, and it belongs to no generation
                record('W-C.barriers.other', value);
                return done();
            });
    }));

    it('should refuse to start on a consumer group that was never seeded',
    done => {
        assert(seededError,
            'the startup assertion accepted a group with no committed offset');
        const message = seededError.description || seededError.message;
        record('W-C.seededAssertion.message', message);
        assert(message.includes('has no committed offset on partitions'),
            `unexpected message: ${message}`);
        assert(message.includes('notificationWorkgroupCutover preseed'),
            `the message has to say what to run, got: ${message}`);
        return done();
    });
});

describe('GATE W-D :: what an operator can read off the metrics',
function gateObservability() {
    this.timeout(120000);

    it('should carry the workgroup label, correctly valued, on every ' +
    'workgroup that ran', done => {
        assert(RAN_WORKGROUPS.length > 0,
            'no workgroup ran, so there is nothing to attribute');
        record('W-D.workgroupsThatRan', RAN_WORKGROUPS.map(ran => ran.id));
        return async.eachSeries(RAN_WORKGROUPS, (ran, next) => async.series([
            step => readSeries(DELIVERED_METRIC, { workgroup: ran.id },
                (err, values) => {
                    assert.ifError(err);
                    // a workgroup that was deliberately blocked may have
                    // delivered nothing, but anything it did deliver has to
                    // belong to it
                    values.forEach(v => assert.strictEqual(
                        workgroupIdForDestination(ran.doc, v.labels.target),
                        ran.id,
                        `delivered_total{workgroup="${ran.id}"} names ` +
                        `${v.labels.target}, which that workgroup does not ` +
                        'own'));
                    return step();
                }),
            step => readCounter(SKIPPED_METRIC, { workgroup: ran.id },
                (err, value) => {
                    assert.ifError(err);
                    assert(value > 0,
                        `skipped_total carries no sample for ${ran.id}, so ` +
                        'its slice cannot be told apart from any other');
                    return step();
                }),
        ], next), done);
    });

    it('should read the lag of each workgroup independently of the others',
    done => {
        const blocked = OBSERVED['W-B.lag.blockedAtChosenMoment'];
        const drained = OBSERVED['W-B.lag.drainedAtChosenMoment'];
        assert.strictEqual(typeof blocked, 'number',
            'the isolation gate did not record a blocked lag');
        assert.strictEqual(typeof drained, 'number',
            'the isolation gate did not record a drained lag');
        assert(blocked > 0,
            'the blocked workgroup had no lag at the chosen moment');
        assert.strictEqual(drained, 0,
            'the drained workgroup still had lag at the chosen moment');
        assert(blocked !== drained,
            'the two groups read the same, so per-workgroup lag is not ' +
            'independently readable');
        return done();
    });

    it('should leave the flag-off series of the single pool with no ' +
    'workgroup label at all', done => {
        const targets = OBSERVED['W-C.destinations'];
        assert(Array.isArray(targets) && targets.length > 0,
            'the cutover gate did not record its destinations');
        let flagOff = 0;
        let flagOn = 0;
        return async.eachSeries(targets, (target, next) =>
            readSeries(DELIVERED_METRIC, { target }, (err, values) => {
                assert.ifError(err);
                values.forEach(v => {
                    if (v.labels.workgroup === undefined) {
                        flagOff += v.value;
                        return;
                    }
                    flagOn += v.value;
                });
                return next();
            }), err => {
            assert.ifError(err);
            record('W-D.flagOff.delivered', flagOff);
            record('W-D.flagOn.delivered', flagOn);
            // the same destinations were served by the single pool and then
            // by generation 1, so both kinds of series exist side by side on
            // one target, which is exactly the discontinuity a cutover has
            assert(flagOff > 0,
                'the single pool delivered nothing under a series with no ' +
                'workgroup label');
            assert(flagOn > 0,
                'generation 1 delivered nothing under a labelled series');
            return readSeries(SKIPPED_METRIC, { reason: 'barrier' },
                (err2, values) => {
                    assert.ifError(err2);
                    const unlabelled = values
                        .filter(v => v.labels.workgroup === undefined)
                        .reduce((total, v) => total + v.value, 0);
                    record('W-D.flagOff.barriersSkipped', unlabelled);
                    assert(unlabelled > 0,
                        'the single pool skipped no barrier under an ' +
                        'unlabelled series, so the flag-off path was never ' +
                        'exercised against a barrier');
                    return done();
                });
        });
    });
});

// ---------------------------------------------------------------------------
// GATE W-E: resharding two auto workgroups into three
// ---------------------------------------------------------------------------

// the exit codes bin/notificationWorkgroupCutover.js resolves a drain report
// to. The gate drives WorkgroupCutover directly rather than the bin, because
// the bin takes its topic and group names from lib/Config while every name
// here is scoped to the run, so the code an operator would see is resolved
// from the same report field the bin keys off
const EXIT_DRAINED = 0;
const EXIT_NOT_DRAINED = 2;

// a deployment does not start every worker of a generation in the same
// millisecond, and simultaneous joins widen the metadata churn window the
// pre-existing consumer wedge lives in
const E_WORKER_SETTLE_MS = 1200;
const E_DRAIN_POLL_MS = 1000;
// A wait that has not been through a restart yet gives up quickly, so a
// wedged workgroup is found and replaced promptly.
//
// The wait after the restart is far longer, because the replacement cannot
// take over its group at once. BackbeatConsumer.close() waits for a revoke
// callback a wedged consumer never delivers, so stopWorker gives up on its
// own timeout while that client is still a live member of the group. The
// replacement joining the same group makes it two members, one of which
// never rejoins, and the group sits in PreparingRebalance consuming nothing
// until the broker evicts the wedged member on its poll interval. Measured
// at about five minutes, after which the group went Stable with one member
// and drained to zero lag. It does recover, so a retry that gives up sooner
// turns a wedge the suite handles into a failed gate, which is what two
// earlier runs of this gate did.
const E_WAIT_MS = 90000;
const E_RETRY_WAIT_MS = 480000;
// how long a single restarted worker is given to be seen consuming. It is
// not competing with a wedged member for its group, so it only has to
// survive its own rebalance churn on the way in, which has been measured
// taking well over a minute
const E_DEFECTOR_WAIT_MS = 180000;

/**
 * Identity of one produced notification: the customer topic it is addressed
 * to, the object it is about and the round it belongs to.
 *
 * Two generations delivering the same record produce two copies of one
 * identity, which is what makes a duplicate countable and a gap nameable.
 *
 * @param {String} topic - customer topic of the destination
 * @param {String} objectKey - S3 object key
 * @param {String} dateTime - event time, one per round
 * @return {String} identity
 */
function identityOf(topic, objectKey, dateTime) {
    return `${topic}|${objectKey}|${dateTime}`;
}

function objectKeysFor(prefix, count) {
    const keys = [];
    for (let i = 0; i < count; i++) {
        keys.push(`${prefix}-obj-${`${i}`.padStart(3, '0')}`);
    }
    return keys;
}

/**
 * Builds and validates a workgroups document whose hashmod workgroups cover
 * one modulo exactly, one remainder each, in the order they are given
 *
 * @param {Object} params - topic, generation, modulo and ids
 * @return {Object} validated document
 */
function buildHashmodDocument(params) {
    const { error, value } = validateWorkgroupsDoc({
        configVersion: CONFIG_VERSION,
        generation: params.generation,
        topic: params.topic,
        updatedAt: new Date().toISOString(),
        workgroups: params.ids.map((id, remainder) => ({
            id,
            rule: {
                type: 'hashmod',
                modulo: params.modulo,
                remainders: [remainder],
            },
        })),
    });
    assert.ifError(error);
    return value;
}

/**
 * Picks destination names whose owner under the old document and under the
 * new one are exactly the pair a scenario asked for.
 *
 * A reshard has to be shown on named movers and named stayers rather than on
 * whatever one run's names happen to hash to, and the pair is proposed by the
 * document's own ownership function, so nothing here is a second
 * implementation of the routing rules.
 *
 * @param {Object} params - oldDoc, newDoc, prefix and wanted pairs
 * @return {String[]} one resource name per wanted pair, in order
 */
function selectReshardDestinations(params) {
    const { oldDoc, newDoc, prefix, wanted } = params;
    const remaining = wanted.map(pair => ({ ...pair, resource: null }));
    for (let i = 0; i < 2000 && remaining.some(e => !e.resource); i++) {
        const resource = `${prefix}-${i}-${RUN_ID}`;
        const from = workgroupIdForDestination(oldDoc, resource);
        const to = workgroupIdForDestination(newDoc, resource);
        const slot = remaining.find(e =>
            !e.resource && e.from === from && e.to === to);
        if (slot) {
            slot.resource = resource;
        }
    }
    const unfilled = remaining.filter(e => !e.resource)
        .map(e => `${e.from} to ${e.to}`);
    assert.deepStrictEqual(unfilled, [],
        `could not find ${prefix} names for every wanted move`);
    return remaining.map(e => e.resource);
}

/**
 * Commits offsets into a consumer group the way the cutover tool pre-seeds
 * one: assign, then a synchronous commit, on a client that never subscribes
 *
 * @param {String} groupId - consumer group id
 * @param {Object[]} toppars - toppars carrying an offset
 * @param {Function} done - callback
 * @return {undefined}
 */
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
 * The committed offset of every partition of a topic, for one group. A
 * partition the group never committed reads as -1.
 *
 * @param {String} groupId - consumer group id
 * @param {String} topic - topic name
 * @param {Number} partitionCount - number of partitions
 * @param {Function} done - callback: done(err, offsetsByPartition)
 * @return {undefined}
 */
function readCommitted(groupId, topic, partitionCount, done) {
    const toppars = [];
    for (let i = 0; i < partitionCount; i++) {
        toppars.push({ topic, partition: i });
    }
    return withConsumer(groupId, (consumer, cb) => callOrFail(cb, () =>
        consumer.committed(toppars, METADATA_TIMEOUT, (err, committed) => {
            if (err) {
                return cb(err);
            }
            const offsets = {};
            for (let i = 0; i < partitionCount; i++) {
                offsets[i] = -1;
            }
            (committed || []).forEach(tp => {
                offsets[tp.partition] = tp.offset;
            });
            return cb(null, offsets);
        })), done);
}

/**
 * How many records a group has consumed, counted from where it started.
 *
 * A committed offset is the next offset to fetch, and these topics start
 * empty, so on a group that started at the beginning it is the number of
 * records of that partition the group is done with. A generation seeded at
 * its barriers started there instead.
 *
 * @param {Object} committed - committed offsets by partition
 * @param {Object} startOffsets - starting offsets by partition, or null
 * @param {Number} partitionCount - number of partitions
 * @return {Number} records consumed
 */
function consumedFrom(committed, startOffsets, partitionCount) {
    let total = 0;
    for (let p = 0; p < partitionCount; p++) {
        const start = startOffsets ? Number(startOffsets[p]) : 0;
        const seen = committed[p] >= 0 ? committed[p] : start;
        total += Math.max(0, seen - start);
    }
    return total;
}

/**
 * Indexes the delivery topic as the broker actually laid it out: which
 * produced record sits at which offset of which partition, and which
 * workgroup owns it under the old rules and under the new ones.
 *
 * Every union, gap and duplicate this gate quotes is computed against this
 * index, so each number is a statement about the topic that existed rather
 * than about the stream the gate meant to produce.
 *
 * @param {Object} params - written, topicOf, oldDoc and newDoc
 * @return {Object} { records, barriers }
 */
function indexDeliveryTopic(params) {
    const { written, topicOf, oldDoc, newDoc } = params;
    const oldIndex = buildOwnershipIndex(oldDoc);
    const newIndex = buildOwnershipIndex(newDoc);
    const records = [];
    const barriers = [];
    written.forEach(raw => {
        if (isBarrierKey(raw.key)) {
            barriers.push({ partition: raw.partition, offset: raw.offset });
            return;
        }
        const parsed = JSON.parse(raw.value);
        const token = destinationTokenFromKey(raw.key);
        const topic = topicOf(parsed.destinationId);
        records.push({
            partition: raw.partition,
            offset: raw.offset,
            destinationId: parsed.destinationId,
            // the whole record key, so a spread destination's lanes can be
            // told apart: the routing token is only the part before the sub
            // key separator, which is the same for all of them
            key: raw.key,
            token,
            identity: topic ?
                identityOf(topic, parsed.key, parsed.dateTime) : null,
            oldOwner: ownerOfToken(oldIndex, token),
            newOwner: ownerOfToken(newIndex, token),
        });
    });
    return { records, barriers };
}

/**
 * What the previous generation's own committed offsets say about a cutover,
 * read against the delivery topic index: which records it still owed when
 * those offsets froze, and which ones it consumed a second time past the
 * barriers.
 *
 * A record below its barrier is the previous generation's responsibility and
 * nobody else's, so it is lost when the workgroup that owns it under the old
 * rules had not committed past it. A record at or after its barrier is
 * delivered by the new generation whatever happens, so the old generation
 * having consumed it too makes it a duplicate.
 *
 * @param {Object} params - index, barriers, committedByGroup, groupIdOf
 * @return {Object} { lost, duplicated }, both sets of identities
 */
function predictFromCommittedOffsets(params) {
    const { index, barriers, committedByGroup, groupIdOf } = params;
    const lost = new Set();
    const duplicated = new Set();
    index.records.forEach(rec => {
        if (rec.identity === null) {
            return;
        }
        const barrier = Number(barriers[rec.partition]);
        const committed = committedByGroup[groupIdOf(rec.oldOwner)] || {};
        const raw = committed[rec.partition];
        const seen = typeof raw === 'number' && raw >= 0 ? raw : 0;
        if (rec.offset < barrier) {
            if (seen <= rec.offset) {
                lost.add(rec.identity);
            }
            return;
        }
        if (seen > rec.offset) {
            duplicated.add(rec.identity);
        }
    });
    return { lost, duplicated };
}

/**
 * The highest offset each customer topic partition held at a moment, so that
 * records delivered before it can be told from records delivered after it
 *
 * @param {Map} tailers - customer topic to tailer
 * @return {Map} topic to a map of partition to offset
 */
function snapshotTailerBoundary(tailers) {
    const snapshot = new Map();
    tailers.forEach((tailer, topic) => {
        const byPartition = new Map();
        tailer.records.forEach(raw => {
            const seen = byPartition.get(raw.partition);
            if (seen === undefined || raw.offset > seen) {
                byPartition.set(raw.partition, raw.offset);
            }
        });
        snapshot.set(topic, byPartition);
    });
    return snapshot;
}

/**
 * Every delivered record, carrying the identity of the produced record it
 * holds and, where a boundary was taken, the generation that delivered it.
 *
 * The boundary only separates generations that never ran at the same time.
 * A scenario that overlaps them passes no boundary and attributes by the
 * delivery topic index instead.
 *
 * @param {Map} tailers - customer topic to tailer
 * @param {Map} [boundary] - snapshot from snapshotTailerBoundary
 * @return {Object[]} delivered records
 */
function deliveredRecordsOf(tailers, boundary) {
    const all = [];
    tailers.forEach((tailer, topic) => {
        const marks = (boundary && boundary.get(topic)) || new Map();
        tailer.records.forEach(raw => {
            const event = deliveredEvent(raw);
            const mark = marks.get(raw.partition);
            all.push({
                topic,
                partition: raw.partition,
                offset: raw.offset,
                key: event.key,
                eventTime: event.eventTime,
                identity: identityOf(topic, event.key, event.eventTime),
                generation: boundary && mark !== undefined &&
                    raw.offset <= mark ? 'old' : 'new',
            });
        });
    });
    return all;
}

/**
 * How many copies of each identity were delivered
 *
 * @param {Object[]} delivered - records from deliveredRecordsOf
 * @return {Map} identity to copy count
 */
function copiesByIdentity(delivered) {
    const copies = new Map();
    delivered.forEach(rec =>
        copies.set(rec.identity, (copies.get(rec.identity) || 0) + 1));
    return copies;
}

/**
 * The smallest number of non-decreasing runs a sequence can be split into.
 *
 * Two generations delivering one object key, each in order, produce a
 * merged sequence that splits into two however they interleave. A sequence
 * needing three means one of the two delivered its own copies out of order,
 * which is the guarantee that has to hold inside a generation whatever the
 * overlap does across them.
 *
 * The greedy that extends the run with the highest tail that still accepts
 * the value is the patience sorting greedy, and it is optimal here.
 *
 * @param {Number[]} rounds - the rounds delivered, in delivery order
 * @return {Number} number of non-decreasing runs
 */
function orderedRunsNeeded(rounds) {
    const tails = [];
    rounds.forEach(round => {
        let best = -1;
        tails.forEach((tail, i) => {
            if (tail <= round && (best === -1 || tail > tails[best])) {
                best = i;
            }
        });
        if (best === -1) {
            tails.push(round);
            return;
        }
        tails[best] = round;
    });
    return tails.length;
}

/**
 * Groups delivered records into one series per object key of one customer
 * topic, in the order the customer topic holds them
 *
 * @param {Object[]} delivered - records from deliveredRecordsOf
 * @param {String[]} roundTimes - event time of each round, in order
 * @return {Map} series key to the rounds delivered, in delivery order
 */
function roundsByObjectKey(delivered, roundTimes) {
    const series = new Map();
    delivered.slice()
        .sort((a, b) => (a.topic === b.topic ?
            a.offset - b.offset : a.topic.localeCompare(b.topic)))
        .forEach(rec => {
            const seriesKey = `${rec.topic}|${rec.key}`;
            if (!series.has(seriesKey)) {
                series.set(seriesKey, []);
            }
            series.get(seriesKey).push({
                round: roundTimes.indexOf(rec.eventTime),
                identity: rec.identity,
            });
        });
    return series;
}

/**
 * Runs the drain report an operator would run, and resolves the exit code
 * bin/notificationWorkgroupCutover.js gives it
 *
 * @param {Object} verifier - WorkgroupCutover instance
 * @param {Function} done - callback: done(err, { report, exitCode, remaining })
 * @return {undefined}
 */
function runVerify(verifier, done) {
    return verifier.verify((err, report) => {
        if (err) {
            return done(err);
        }
        return done(null, {
            report,
            exitCode: report.drained ? EXIT_DRAINED : EXIT_NOT_DRAINED,
            remaining: report.rows
                .reduce((total, row) => total + row.remaining, 0),
        });
    });
}

/**
 * Builds a WorkgroupCutover against the real zookeeper and the real broker
 *
 * @param {Object} params - notifConfig, options and name
 * @return {Object} WorkgroupCutover
 */
function buildCutoverTool(params) {
    return new WorkgroupCutover({
        kafkaConfig,
        zkConfig: {
            connectionString: ZOOKEEPER_HOSTS,
            autoCreateNamespace: false,
        },
        notifConfig: params.notifConfig,
        options: params.options || { timeout: 10000 },
        logger: new werelogs.Logger(`WorkgroupCutover:${params.name}`),
    });
}

/**
 * Starts one workgroup per id, each through the real loader, confirming the
 * delivery topic's metadata is stable immediately before every join and
 * leaving a gap between them.
 *
 * The reconfirm is per join, not per generation. A topic created and
 * verified minutes earlier is not verified at join time: the broker
 * intermittently answers "unknown topic or partition" for it, which drops it
 * out of the effective subscription and starts the rebalance loop of
 * design/06-backbeatconsumer-wedge.md. That is the mitigation
 * design/09-workgroups-observations.md found to work, and it only works if
 * it is done immediately before each join.
 *
 * @param {Object} params - zkPath, workgroupIds, baseGroupId, notifConfig,
 *   topic and runtimes, the map runtimes are recorded in
 * @param {Function} done - callback
 * @return {undefined}
 */
function startWorkgroups(params, done) {
    return async.eachSeries(params.workgroupIds, (workgroupId, next) =>
        waitForTopics([params.topic], topicErr => {
            if (topicErr) {
                return next(topicErr);
            }
            return startWorkgroup({
                zkPath: params.zkPath,
                workgroupId,
                baseGroupId: params.groupBaseOf ?
                    params.groupBaseOf(workgroupId) : params.baseGroupId,
                notifConfig: params.notifConfig,
            }, (err, runtime) => {
                if (runtime) {
                    params.runtimes.set(workgroupId, runtime);
                }
                if (err) {
                    return next(err);
                }
                return setTimeout(next, E_WORKER_SETTLE_MS);
            });
        }), done);
}

function stopWorkgroups(runtimes, done) {
    const entries = [...runtimes.entries()];
    return async.eachSeries(entries, (entry, next) =>
        stopWorkgroup(entry[1], () => {
            runtimes.delete(entry[0]);
            return next();
        }), () => done());
}

/**
 * Restarts, on the consumer group it already has, every workgroup whose own
 * counter fell short of what a phase needs.
 *
 * The consumer wedge is a property of one consumer, so it is looked for one
 * workgroup at a time: anything measured over a generation hides a wedge of
 * one member behind the health of the others. A fresh worker on the same
 * group resumes from that group's committed offset, so a workgroup that had
 * already delivered does not deliver anything twice.
 *
 * The replacement is slow to take over, and that is not a bug here. A wedged
 * consumer never delivers the revoke callback close() waits for, so
 * stopWorker gives up on its own timeout while that client is still a live
 * member of the group. The replacement makes it two members, one of which
 * never rejoins, and the group sits in PreparingRebalance consuming nothing
 * until the broker evicts the wedged member on its poll interval. Measured
 * at about five minutes, after which the group went Stable with one member
 * and drained to zero lag. E_COMMIT_TIMEOUT_MS is sized to outlast it.
 *
 * See design/06-backbeatconsumer-wedge.md.
 *
 * @param {Object} params - label, workgroupIds, runtimes, read, need,
 *   restarted, zkPath, baseGroupId, notifConfig and topic
 * @param {Function} done - callback
 * @return {undefined}
 */
function restartShortWorkgroups(params, done) {
    return async.eachSeries(params.workgroupIds, (workgroupId, next) =>
        params.read(workgroupId, (err, value) => {
            if (err) {
                return next(err);
            }
            if (value >= params.need) {
                return next();
            }
            WEDGES.push({
                phase: `${params.label} ${workgroupId}`,
                attempt: 1,
                error: `reached ${value} of ${params.need}`,
            });
            params.restarted.add(workgroupId);
            suiteLog.warn('a workgroup fell short of what its phase needs, ' +
                'restarting it on its own group and recording the occurrence ' +
                'as the pre-existing consumer wedge', {
                phase: params.label,
                workgroup: workgroupId,
                reached: value,
                need: params.need,
            });
            return stopWorkgroup(params.runtimes.get(workgroupId), () =>
                startWorkgroups({
                    zkPath: params.zkPath,
                    workgroupIds: [workgroupId],
                    baseGroupId: params.baseGroupId,
                    notifConfig: params.notifConfig,
                    topic: params.topic,
                    runtimes: params.runtimes,
                }, next));
        }), done);
}

/**
 * Waits for a phase, and when the wait fails, restarts the workgroups that
 * fell short and waits once more
 *
 * @param {Object} params - the restartShortWorkgroups params plus wait
 * @param {Function} done - callback
 * @return {undefined}
 */
function waitOrRestart(params, done) {
    return params.wait(params.waitMs || E_WAIT_MS, err => {
        if (!err) {
            return done();
        }
        suiteLog.warn('a phase did not complete, looking for a wedged ' +
            'workgroup before failing it', {
            phase: params.label,
            error: err.message,
        });
        return restartShortWorkgroups(params, restartErr => {
            if (restartErr) {
                return done(restartErr);
            }
            // the replacement cannot take over its group until the broker
            // has evicted the wedged member, so this wait is far longer than
            // the one that just failed
            return params.wait(E_RETRY_WAIT_MS, done);
        });
    });
}

/**
 * Waits until a group has committed anything at all past a floor.
 *
 * waitForCommittedTotal wants an exact total, which is the right question
 * for a generation that has to finish a topic. This is the question to ask
 * of a worker that only has to be seen working: has this group moved past
 * where it was put.
 *
 * @param {String} groupId - consumer group id
 * @param {String} topic - topic name
 * @param {Number} partitionCount - number of partitions
 * @param {Number} floor - offset total the group has to get past
 * @param {Number} timeoutMs - how long to wait for
 * @param {Function} done - callback
 * @return {undefined}
 */
function waitForCommittedAbove(groupId, topic, partitionCount, floor,
    timeoutMs, done) {
    const deadline = Date.now() + timeoutMs;
    let lastSeen = null;
    const check = () => committedTotal(groupId, topic, partitionCount,
        (err, total) => {
            if (!err) {
                lastSeen = total;
                if (total > floor) {
                    return done();
                }
            }
            if (Date.now() >= deadline) {
                return done(new Error(`timed out waiting for group ${groupId}` +
                    ` to commit past ${floor}, last seen ${lastSeen}`));
            }
            return setTimeout(check, 1000);
        });
    return check();
}

/**
 * The number of records a group is done with, summed over every partition
 *
 * @param {String} groupId - consumer group id
 * @param {String} topic - topic name
 * @param {Number} partitionCount - number of partitions
 * @param {Function} done - callback: done(err, total)
 * @return {undefined}
 */
function committedTotal(groupId, topic, partitionCount, done) {
    return readCommitted(groupId, topic, partitionCount, (err, offsets) => {
        if (err) {
            return done(err);
        }
        return done(null, consumedFrom(offsets, null, partitionCount));
    });
}

describe('GATE W-E :: resharding two auto workgroups into three',
function gateReshard() {
    this.timeout(3600000);

    before(done => createGateTopics('e', done));

    describe('E1 :: the happy reshard', function reshardHappyPath() {
        this.timeout(1200000);

        const deliverySpec = TOPICS.e1Delivery;
        const deliveryTopic = deliverySpec.name;
        const deliveryPartitions = deliverySpec.partitions;
        const baseGroupId = `poc-wg-e1-group-${RUN_ID}`;
        const zkPath = `${ZK_BASE}/gate-e1`;
        const cachePath = cachePathFor('gate-e1-cutover');
        const objectsPerDestination = 5;
        const spreadKeysPerSubKey = 2;
        const BATCH_SIZE = 3;
        // the cutover takes several seconds to reach the step that produces
        // the barriers, and everything the stream sends in that time lands
        // below them. The gap is wide enough that a useful share of the
        // stream is still to come when the barriers land, which is what
        // gives generation 2 something of its own to deliver
        const BATCH_GAP_MS = 500;
        const CUTOVER_DELAY_MS = 1000;

        const oldIds = ['e1-g1a', 'e1-g1b'];
        const newIds = ['e1-g2a', 'e1-g2b', 'e1-g2c'];

        // neither of these is ever written: they only propose destination
        // names, so that the cutover the tool actually runs moves the
        // destinations this gate wants moved and leaves the others where
        // they are
        const oldPlan = buildHashmodDocument({
            topic: deliveryTopic, generation: 1, modulo: 2, ids: oldIds });
        const newPlan = buildHashmodDocument({
            topic: deliveryTopic, generation: 2, modulo: 3, ids: newIds });

        // one destination of every class the remap has: modulo 2 to modulo 3
        // leaves two of six where they are and moves the other four
        const moves = [
            { from: oldIds[0], to: newIds[0] },
            { from: oldIds[1], to: newIds[1] },
            { from: oldIds[0], to: newIds[1] },
            { from: oldIds[0], to: newIds[2] },
            { from: oldIds[1], to: newIds[0] },
            { from: oldIds[1], to: newIds[2] },
        ];
        const plainResources = selectReshardDestinations({
            oldDoc: oldPlan,
            newDoc: newPlan,
            prefix: 'poc-wg-e1-dest',
            wanted: moves,
        });
        const [spreadResource] = selectReshardDestinations({
            oldDoc: oldPlan,
            newDoc: newPlan,
            prefix: 'poc-wg-e1-spread',
            // a spread destination that changes owner: every lane of it has
            // to move together, because the routing token is cut at the sub
            // key separator and is therefore the same for all of them
            wanted: [{ from: oldIds[1], to: newIds[2] }],
        });

        const plainDestinations = plainResources.map((resource, index) =>
            destinationConfig({
                resource,
                topic: TOPICS[`e1Customer${index}`].name,
            }));
        const spread = destinationConfig({
            resource: spreadResource,
            topic: TOPICS.e1Customer6.name,
            spreadFactor: 3,
        });
        const destinations = plainDestinations.concat([spread]);

        const notifConfig = {
            destinations,
            deliveryPool: {
                ...deliveryPoolConfig({
                    topic: deliveryTopic,
                    groupId: baseGroupId,
                    concurrency: 10,
                }),
                workgroups: { zookeeperPath: zkPath, cachePath },
            },
        };

        const eventTypes = ['s3:ObjectCreated:Put', 's3:ObjectCreated:Put',
            's3:ObjectRemoved:Delete'];
        const roundTimes = eventTypes.map((_, index) => eventTime(index));
        const keysOf = new Map();
        plainDestinations.forEach((destination, index) => keysOf.set(
            destination.topic, objectKeysFor(`e1-${index}`,
                objectsPerDestination)));
        keysOf.set(spread.topic,
            keysCoveringEverySubKey(spread, 'e1-spread', spreadKeysPerSubKey));

        const produced = new Set();
        const primingRecords = [];
        const streamBatches = [];
        const maxKeys = Math.max(...destinations
            .map(destination => keysOf.get(destination.topic).length));
        eventTypes.forEach((eventType, round) => {
            const roundRecords = [];
            // destinations are interleaved, so no worker ever sees one
            // destination's records as one uninterrupted run
            for (let k = 0; k < maxKeys; k++) {
                destinations.forEach(destination => {
                    const keys = keysOf.get(destination.topic);
                    if (k >= keys.length) {
                        return;
                    }
                    produced.add(identityOf(destination.topic, keys[k],
                        roundTimes[round]));
                    roundRecords.push(addressedRecord({
                        destination,
                        key: keys[k],
                        eventType,
                        dateTime: roundTimes[round],
                    }));
                });
            }
            if (round === 0) {
                // the topic is not empty when generation 1 joins, which is
                // what a running deployment looks like
                primingRecords.push(...roundRecords);
                return;
            }
            for (let i = 0; i < roundRecords.length; i += BATCH_SIZE) {
                streamBatches.push(roundRecords.slice(i, i + BATCH_SIZE));
            }
        });
        const totalProduced = produced.size;
        const totalOnDeliveryTopic = totalProduced + deliveryPartitions;

        const tailers = new Map();
        const oldRuntimes = new Map();
        const newRuntimes = new Map();
        const restartedOld = new Set();
        const restartedNew = new Set();
        const drainPolls = [];
        const oldCommitted = {};
        const newCommitted = {};
        const seededErrors = [];
        let stream = null;
        let verifier = null;
        let cutoverResult = null;
        let drainedReport = null;
        let frozenReport = null;
        let boundary = null;
        let deliveryIndex = null;
        let oldDelivered = 0;

        const oldGroupId = id => buildGroupId(baseGroupId, id, 1);
        const newGroupId = id => buildGroupId(baseGroupId, id, 2);
        const topicOf = destinationId => {
            const found = destinations
                .find(destination => destination.resource === destinationId);
            return found ? found.topic : null;
        };

        function deliveredCount() {
            return [...tailers.values()]
                .reduce((total, tailer) => total + tailer.records.length, 0);
        }

        function waitForDrain(startedAt, timeoutMs, done) {
            const deadline = Date.now() + timeoutMs;
            const attempt = () => runVerify(verifier, (err, result) => {
                if (err) {
                    return done(err);
                }
                drainPolls.push({
                    atMs: Date.now() - startedAt,
                    remaining: result.remaining,
                    exitCode: result.exitCode,
                });
                if (result.report.drained) {
                    return done(null, result.report);
                }
                if (Date.now() >= deadline) {
                    return done(new Error('generation 1 never committed past ' +
                        'every barrier, so the reshard could not proceed'));
                }
                return setTimeout(attempt, E_DRAIN_POLL_MS);
            });
            return attempt();
        }

        before(done => {
            const finish = settleOnce(done);
            record('W-E1.destinations.plain', plainResources);
            record('W-E1.destinations.spread', spreadResource);
            record('W-E1.destinations.moves', moves.map((move, i) =>
                `${plainResources[i]}: ${move.from} to ${move.to}`));
            record('W-E1.records.produced', totalProduced);
            record('W-E1.stream.batches', streamBatches.length);
            return async.series([
                next => async.eachSeries(destinations,
                    (destination, tailDone) => {
                        const tailer = new TopicTailer(destination.topic);
                        tailers.set(destination.topic, tailer);
                        return tailer.start(tailDone);
                    }, next),
                next => produceRecords(deliveryTopic, primingRecords, next),
                // the delivery topic was created and confirmed stable in the
                // gate's before hook, minutes of wall clock before these
                // workers join. The broker intermittently answers "unknown
                // topic or partition" for it anyway, which drops it out of
                // the effective subscription and starts the rebalance loop of
                // design/06-backbeatconsumer-wedge.md
                next => waitForTopics([deliverySpec], err => next(err)),
                next => writeWorkgroupsDocument(zkPath, buildHashmodDocument({
                    topic: deliveryTopic,
                    generation: 1,
                    modulo: 2,
                    ids: oldIds,
                }), next),
                next => startWorkgroups({
                    zkPath,
                    workgroupIds: oldIds,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    runtimes: oldRuntimes,
                }, next),
                next => waitOrRestart({
                    label: 'W-E1 generation 1 first delivery',
                    workgroupIds: oldIds,
                    runtimes: oldRuntimes,
                    restarted: restartedOld,
                    zkPath,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    need: 1,
                    read: (workgroupId, cb) => readCounter(DELIVERED_METRIC,
                        { workgroup: workgroupId }, cb),
                    wait: (waitMs, cb) => async.eachSeries(oldIds, (workgroupId, step) =>
                        waitForCounter(DELIVERED_METRIC,
                            { workgroup: workgroupId }, 1, waitMs, 500, step),
                        cb),
                }, next),
                next => {
                    stream = new RecordStream({
                        topic: deliveryTopic,
                        batches: streamBatches,
                        gapMs: BATCH_GAP_MS,
                    });
                    return stream.start(next);
                },
                next => setTimeout(next, CUTOVER_DELAY_MS),
                next => {
                    const startedAt = Date.now();
                    const cutover = buildCutoverTool({
                        name: 'e1',
                        notifConfig,
                        options: {
                            modulo: 3,
                            workgroup: newIds.map((id, r) => `${id}:${r}`),
                            timeout: 10000,
                        },
                    });
                    return cutover.cutover((err, result) => {
                        cutoverResult = result;
                        record('W-E1.cutover.ms', Date.now() - startedAt);
                        record('W-E1.cutover.recordsStreamedSoFar',
                            stream.sent);
                        return cutover.close(() => next(err));
                    });
                },
                next => waitFor(() => 'the record stream to finish ' +
                    `(${stream.sent} of ${streamBatches.length * BATCH_SIZE})`,
                    () => stream.finished, 120000, next),
                next => stream.close(next),
                next => {
                    assert.ifError(stream.error);
                    verifier = buildCutoverTool({
                        name: 'e1-verify',
                        notifConfig,
                        options: { timeout: 10000 },
                    });
                    return next();
                },
                next => waitOrRestart({
                    label: 'W-E1 generation 1 drain',
                    workgroupIds: oldIds,
                    runtimes: oldRuntimes,
                    restarted: restartedOld,
                    zkPath,
                    baseGroupId,
                    notifConfig,
                    // draining is committing past every barrier, so a
                    // workgroup that fell short of the barrier total is the
                    // one holding the drain up
                    topic: deliverySpec,
                    need: WorkgroupCutover
                        .partitionsOf(cutoverResult.doc.barriers)
                        .reduce((total, partition) =>
                            total + cutoverResult.doc.barriers[partition], 0),
                    read: (workgroupId, cb) => committedTotal(
                        oldGroupId(workgroupId), deliveryTopic,
                        deliveryPartitions, cb),
                    wait: (waitMs, cb) => waitForDrain(Date.now(), waitMs, (err, report) => {
                        drainedReport = report;
                        return cb(err);
                    }),
                }, next),
                // generation 1 is left running until it is done with the
                // whole topic, so its overshoot past the barriers, and the
                // duplicates that follow from it, are an exact number rather
                // than whatever it happened to reach when it was stopped
                next => waitOrRestart({
                    label: 'W-E1 generation 1 commits the whole topic',
                    workgroupIds: oldIds,
                    runtimes: oldRuntimes,
                    restarted: restartedOld,
                    zkPath,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    need: totalOnDeliveryTopic,
                    read: (workgroupId, cb) => committedTotal(
                        oldGroupId(workgroupId), deliveryTopic,
                        deliveryPartitions, cb),
                    wait: (waitMs, cb) => async.eachSeries(oldIds, (workgroupId, step) =>
                        waitForCommittedTotal(oldGroupId(workgroupId),
                            deliveryTopic, deliveryPartitions,
                            totalOnDeliveryTopic, waitMs, step),
                        cb),
                }, next),
                // only now may the previous generation be stopped
                next => stopWorkgroups(oldRuntimes, next),
                next => async.eachSeries([...tailers.values()],
                    (tailer, quietDone) => waitUntilQuiet(tailer, 500,
                        quietDone), next),
                next => {
                    boundary = snapshotTailerBoundary(tailers);
                    oldDelivered = deliveredCount();
                    record('W-E1.generation1.delivered', oldDelivered);
                    return next();
                },
                // the offsets generation 1 froze at, which is what the drain
                // report is reading and what every prediction is made from
                next => async.eachSeries(oldIds, (workgroupId, step) =>
                    readCommitted(oldGroupId(workgroupId), deliveryTopic,
                        deliveryPartitions, (err, offsets) => {
                            if (err) {
                                return step(err);
                            }
                            oldCommitted[oldGroupId(workgroupId)] = offsets;
                            return step();
                        }), next),
                next => runVerify(verifier, (err, result) => {
                    if (err) {
                        return next(err);
                    }
                    frozenReport = result.report;
                    record('W-E1.verify.afterStop.exitCode', result.exitCode);
                    record('W-E1.verify.afterStop.remaining',
                        result.remaining);
                    record('W-E1.verify.afterStop.overshoot',
                        result.report.overshoot);
                    record('W-E1.verify.afterStop.overshootByGroup',
                        result.report.overshootByGroup);
                    return next();
                }),
                // the delivery topic as the broker laid it out, read once
                // everything that will ever be produced has been
                next => readTopic(deliveryTopic, totalOnDeliveryTopic, 120000,
                    (err, written) => {
                        if (err) {
                            return next(err);
                        }
                        deliveryIndex = indexDeliveryTopic({
                            written,
                            topicOf,
                            oldDoc: oldPlan,
                            newDoc: cutoverResult.doc,
                        });
                        record('W-E1.deliveryTopic.records',
                            deliveryIndex.records.length);
                        record('W-E1.deliveryTopic.barriers',
                            deliveryIndex.barriers.length);
                        return next();
                    }),
                // the guard rail, on the groups the cutover really seeded
                next => async.eachSeries(cutoverResult.groupIds,
                    (groupId, cb) => assertSeededOffsets({
                        kafkaConfig,
                        topic: deliveryTopic,
                        groupId,
                        barriers: cutoverResult.doc.barriers,
                        logger: new werelogs.Logger('seededOffsets:ft'),
                    }, err => {
                        if (err) {
                            seededErrors.push(
                                err.description || err.message);
                        }
                        return cb(err);
                    }), next),
                next => waitForTopics([deliverySpec], err => next(err)),
                next => startWorkgroups({
                    zkPath,
                    workgroupIds: newIds,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    runtimes: newRuntimes,
                }, next),
                // generation 1 had already delivered every produced record
                // before it was stopped, so covering the produced set says
                // nothing about generation 2. Its own barriers, one per
                // partition, and the records that follow them are what say
                // it ran
                next => waitOrRestart({
                    label: 'W-E1 generation 2 barriers',
                    workgroupIds: newIds,
                    runtimes: newRuntimes,
                    restarted: restartedNew,
                    zkPath,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    need: deliveryPartitions,
                    read: (workgroupId, cb) => readCounter(BARRIER_METRIC,
                        { workgroup: workgroupId, match: 'current' }, cb),
                    wait: (waitMs, cb) => async.series([
                        step => async.eachSeries(newIds, (workgroupId, each) =>
                            waitForCounter(BARRIER_METRIC, {
                                workgroup: workgroupId, match: 'current',
                            }, deliveryPartitions, waitMs, 500,
                            each), step),
                        step => waitFor(() => 'generation 2 to redeliver ' +
                            'what follows its barriers ' +
                            `(${deliveredCount() - oldDelivered} so far)`,
                            () => deliveredCount() > oldDelivered, waitMs,
                            step),
                    ], err => cb(err)),
                }, next),
                next => waitOrRestart({
                    label: 'W-E1 generation 2 commits the whole topic',
                    workgroupIds: newIds,
                    runtimes: newRuntimes,
                    restarted: restartedNew,
                    zkPath,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    need: totalOnDeliveryTopic,
                    read: (workgroupId, cb) => committedTotal(
                        newGroupId(workgroupId), deliveryTopic,
                        deliveryPartitions, cb),
                    wait: (waitMs, cb) => async.eachSeries(newIds, (workgroupId, step) =>
                        waitForCommittedTotal(newGroupId(workgroupId),
                            deliveryTopic, deliveryPartitions,
                            totalOnDeliveryTopic, waitMs, step),
                        cb),
                }, next),
                next => async.eachSeries([...tailers.values()],
                    (tailer, quietDone) => waitUntilQuiet(tailer, 500,
                        quietDone), next),
                next => async.eachSeries(newIds, (workgroupId, step) =>
                    readCommitted(newGroupId(workgroupId), deliveryTopic,
                        deliveryPartitions, (err, offsets) => {
                            if (err) {
                                return step(err);
                            }
                            newCommitted[newGroupId(workgroupId)] = offsets;
                            return step();
                        }), next),
                next => {
                    [...oldRuntimes.values(), ...newRuntimes.values()]
                        .forEach(registerWorkgroup);
                    return next();
                },
            ], finish);
        });

        after(done => async.series([
            next => (stream ? stream.close(next) : next()),
            next => stopWorkgroups(oldRuntimes, next),
            next => stopWorkgroups(newRuntimes, next),
            next => (verifier ? verifier.close(next) : next()),
            next => async.eachSeries([...tailers.values()],
                (tailer, tailDone) => tailer.stop(tailDone), next),
        ], done));

        it('should write generation 2 with a barrier on every partition and ' +
        'exactly the two generation 1 groups it replaces', done => {
            const doc = cutoverResult.doc;
            record('W-E1.doc.generation', doc.generation);
            record('W-E1.doc.previousGroups', doc.previousGroups);
            record('W-E1.doc.barriers', doc.barriers);
            record('W-E1.doc.groupIds', cutoverResult.groupIds);
            assert.strictEqual(doc.generation, 2);
            assert.strictEqual(doc.topic, deliveryTopic);
            assert.deepStrictEqual(doc.previousGroups.slice().sort(),
                oldIds.map(oldGroupId).sort(),
                'the document has to record the two generation 1 groups as ' +
                'the ones it replaces, so a later verify drains those and ' +
                'not a set derived from the workgroups it lists');
            assert.strictEqual(Object.keys(doc.barriers).length,
                deliveryPartitions,
                'every partition needs a barrier or the new generation has ' +
                'a partition nobody seeded');
            assert.deepStrictEqual(cutoverResult.groupIds.slice().sort(),
                newIds.map(newGroupId).sort());
            assert.deepStrictEqual(seededErrors, [],
                'the startup assertion refused a group the cutover seeded');
            return done();
        });

        it('should hold the reshard until generation 1 has committed past ' +
        'every barrier', done => {
            const last = drainPolls[drainPolls.length - 1];
            assert(drainedReport, 'the drain report never came back');
            record('W-E1.drain.polls', drainPolls);
            record('W-E1.drain.pollCount', drainPolls.length);
            record('W-E1.drain.firstRemaining', drainPolls[0].remaining);
            record('W-E1.drain.elapsedMs', last.atMs);
            assert(drainedReport.drained,
                'the drain report never reported generation 1 drained');
            assert.strictEqual(last.remaining, 0);
            assert.strictEqual(last.exitCode, EXIT_DRAINED);
            assert.deepStrictEqual(
                Object.keys(drainedReport.overshootByGroup).sort(),
                oldIds.map(oldGroupId).sort(),
                'the drain report has to read exactly the two generation 1 ' +
                'groups the document records');
            return done();
        });

        it('should deliver the union of both generations with no gap',
        done => {
            const delivered = deliveredRecordsOf(tailers, boundary);
            const seen = new Set(delivered.map(rec => rec.identity));
            const missing = [...produced].filter(id => !seen.has(id));
            const byGeneration = { old: 0, new: 0 };
            delivered.forEach(rec => { byGeneration[rec.generation] += 1; });
            record('W-E1.union.produced', totalProduced);
            record('W-E1.union.deliveredRecords', delivered.length);
            record('W-E1.union.distinctDelivered', seen.size);
            record('W-E1.union.duplicates', delivered.length - seen.size);
            record('W-E1.union.byGeneration', byGeneration);
            assert.deepStrictEqual(missing, [],
                'a produced record was delivered by neither generation, ' +
                'which is the gap this design exists to prevent');
            assert.strictEqual(seen.size, totalProduced);
            assert(byGeneration.old > 0,
                'generation 1 delivered nothing, so no seam was crossed');
            assert(byGeneration.new > 0,
                'generation 2 delivered nothing, so no seam was crossed');
            return done();
        });

        it('should have each generation deliver exactly the records it ' +
        'consumed, one workgroup per destination', done => {
            const barriers = cutoverResult.doc.barriers;
            const delivered = deliveredRecordsOf(tailers, boundary);
            const generations = [
                { ids: oldIds, groupIdOf: oldGroupId, doc: oldPlan,
                    committed: oldCommitted, start: null, side: 'old',
                    label: 'generation 1' },
                { ids: newIds, groupIdOf: newGroupId, doc: cutoverResult.doc,
                    committed: newCommitted, start: barriers, side: 'new',
                    label: 'generation 2' },
            ];
            // stated as sets of identities rather than as counters: a
            // consumer that re-read records it had already handled, which is
            // what a rebalance under the pre-existing commit path throw
            // does, inflates a counter but cannot add an identity
            generations.forEach(generation => {
                const start = partition => (generation.start ?
                    Number(generation.start[partition]) : 0);
                const expected = new Set(deliveryIndex.records
                    .filter(rec => rec.offset >= start(rec.partition))
                    .map(rec => rec.identity));
                const seen = new Set(delivered
                    .filter(rec => rec.generation === generation.side)
                    .map(rec => rec.identity));
                const missing = [...expected].filter(id => !seen.has(id));
                const extra = [...seen].filter(id => !expected.has(id));
                record(`W-E1.coverage.${generation.side}`, {
                    consumed: expected.size,
                    delivered: seen.size,
                });
                assert.deepStrictEqual(missing, [],
                    `${generation.label} consumed a record and delivered it ` +
                    'to nobody');
                assert.deepStrictEqual(extra, [],
                    `${generation.label} delivered a record it never ` +
                    'consumed');
            });
            return async.mapSeries(generations, (generation, next) =>
                async.mapSeries(generation.ids, (workgroupId, step) =>
                    async.parallel({
                        delivered: cb => readCounter(DELIVERED_METRIC,
                            { workgroup: workgroupId }, cb),
                        notInSlice: cb => readCounter(SKIPPED_METRIC,
                            { workgroup: workgroupId, reason: 'not_in_slice' },
                            cb),
                        barrier: cb => readCounter(SKIPPED_METRIC,
                            { workgroup: workgroupId, reason: 'barrier' }, cb),
                        dropped: cb => readCounter(DROPPED_METRIC,
                            { workgroup: workgroupId }, cb),
                        targets: cb => readSeries(DELIVERED_METRIC,
                            { workgroup: workgroupId }, cb),
                    }, (err, counters) => step(err, { workgroupId, counters,
                        generation })),
                    next),
                (err, byGeneration) => {
                    assert.ifError(err);
                    [].concat(...byGeneration).forEach(row => {
                        const { workgroupId, counters, generation } = row;
                        const consumed = consumedFrom(
                            generation.committed[
                                generation.groupIdOf(workgroupId)],
                            generation.start, deliveryPartitions);
                        const handled = counters.delivered +
                            counters.notInSlice + counters.barrier +
                            counters.dropped;
                        record(`W-E1.totality.${workgroupId}`, {
                            consumed,
                            delivered: counters.delivered,
                            notInSlice: counters.notInSlice,
                            barrier: counters.barrier,
                            dropped: counters.dropped,
                            // records handled a second time because their
                            // offsets were never stored, which is the
                            // pre-existing commit path throw counted in
                            // run.commitPathThrows, not a reshard property
                            reHandled: handled - consumed,
                        });
                        assert.strictEqual(counters.dropped, 0,
                            `${workgroupId} dropped a record it owned`);
                        assert(handled >= consumed,
                            `${workgroupId} consumed ${consumed} records and ` +
                            `accounted for only ${handled} of them`);
                        assert(counters.barrier >= deliveryPartitions,
                            `${workgroupId} saw ${counters.barrier} ` +
                            'barriers, one per partition would be ' +
                            `${deliveryPartitions}`);
                        counters.targets.forEach(sample => assert.strictEqual(
                            workgroupIdForDestination(generation.doc,
                                sample.labels.target), workgroupId,
                            `${workgroupId} delivered to ` +
                            `${sample.labels.target}, which it does not own ` +
                            `in ${generation.label}`));
                    });
                    return done();
                });
        });

        it('should hand every moved destination from its generation 1 owner ' +
        'below the barrier to its generation 2 owner from the barrier on',
        done => {
            const delivered = deliveredRecordsOf(tailers, boundary);
            const byGeneration = { old: new Set(), new: new Set() };
            delivered.forEach(rec =>
                byGeneration[rec.generation].add(rec.identity));
            const barriers = cutoverResult.doc.barriers;
            const below = [];
            const from = [];
            deliveryIndex.records.forEach(rec => {
                if (rec.offset < Number(barriers[rec.partition])) {
                    below.push(rec);
                    return;
                }
                from.push(rec);
            });
            const belowMissed = below
                .filter(rec => !byGeneration.old.has(rec.identity))
                .map(rec => rec.identity);
            const fromMissed = from
                .filter(rec => !byGeneration.new.has(rec.identity))
                .map(rec => rec.identity);
            record('W-E1.barrier.recordsBelow', below.length);
            record('W-E1.barrier.recordsFrom', from.length);
            assert.deepStrictEqual(belowMissed, [],
                'a record below its barrier was not delivered by ' +
                'generation 1, which owns everything below the barrier');
            assert.deepStrictEqual(fromMissed, [],
                'a record at or after its barrier was not delivered by ' +
                'generation 2, which is seeded at the barrier');
            return async.eachSeries(
                moves.map((move, i) => ({ ...move,
                    resource: plainResources[i] })),
                (move, next) => async.parallel({
                    oldOwner: cb => readCounter(DELIVERED_METRIC,
                        { workgroup: move.from, target: move.resource }, cb),
                    newOwner: cb => readCounter(DELIVERED_METRIC,
                        { workgroup: move.to, target: move.resource }, cb),
                    others: cb => readSeries(DELIVERED_METRIC,
                        { target: move.resource }, cb),
                }, (err, counters) => {
                    if (err) {
                        return next(err);
                    }
                    record(`W-E1.handover.${move.resource}`, {
                        from: move.from,
                        to: move.to,
                        deliveredByOldOwner: counters.oldOwner,
                        deliveredByNewOwner: counters.newOwner,
                    });
                    assert(counters.oldOwner > 0,
                        `${move.resource} was not delivered by its ` +
                        `generation 1 owner ${move.from}`);
                    assert(counters.newOwner > 0,
                        `${move.resource} was not delivered by its ` +
                        `generation 2 owner ${move.to}`);
                    const strangers = counters.others
                        .map(sample => sample.labels.workgroup)
                        .filter(workgroup => workgroup !== move.from &&
                            workgroup !== move.to);
                    assert.deepStrictEqual(strangers, [],
                        `${move.resource} was delivered by a workgroup that ` +
                        'owns it in neither generation');
                    return next();
                }), done);
        });

        it('should keep every lane of the spread destination in one ' +
        'generation 2 workgroup', done => {
            const spreadRecords = deliveryIndex.records
                .filter(rec => rec.destinationId === spreadResource);
            const lanes = new Set(spreadRecords.map(rec => rec.key));
            const partitions = new Set(
                spreadRecords.map(rec => rec.partition));
            const oldOwner = workgroupIdForDestination(oldPlan,
                spreadResource);
            const newOwner = workgroupIdForDestination(cutoverResult.doc,
                spreadResource);
            record('W-E1.spread.lanes', lanes.size);
            record('W-E1.spread.deliveryPartitions',
                [...partitions].sort((a, b) => a - b));
            record('W-E1.spread.oldOwner', oldOwner);
            record('W-E1.spread.newOwner', newOwner);
            assert.strictEqual(lanes.size, spread.spreadFactor,
                'the spread destination did not use every lane it has');
            // a workgroup id says nothing about the remainder it serves, so
            // moving is a change of position in the ownership rules
            assert.notStrictEqual(oldIds.indexOf(oldOwner),
                newIds.indexOf(newOwner),
                'the spread destination has to change owner, otherwise it ' +
                'says nothing about a reshard');
            return readSeries(DELIVERED_METRIC, { target: spreadResource },
                (err, values) => {
                    assert.ifError(err);
                    const newOwners = new Set(values
                        .map(sample => sample.labels.workgroup)
                        .filter(workgroup => newIds.includes(workgroup)));
                    record('W-E1.spread.generation2Workgroups',
                        [...newOwners]);
                    assert.strictEqual(newOwners.size, 1,
                        'the lanes of one spread destination were split ' +
                        `across ${newOwners.size} generation 2 workgroups, ` +
                        'so a sub key is owned by nobody or by two owners');
                    assert.strictEqual([...newOwners][0], newOwner);
                    return done();
                });
        });

        it('should have delivered exactly the duplicates generation 1 ran up ' +
        'past its barriers', done => {
            const delivered = deliveredRecordsOf(tailers, boundary);
            const copies = copiesByIdentity(delivered);
            const observed = new Set([...copies.entries()]
                .filter(entry => entry[1] > 1).map(entry => entry[0]));
            const predicted = predictFromCommittedOffsets({
                index: deliveryIndex,
                barriers: cutoverResult.doc.barriers,
                committedByGroup: oldCommitted,
                groupIdOf: oldGroupId,
            });
            const extraCopies = [...copies.values()]
                .filter(count => count > 2).length;
            record('W-E1.duplicates.observed', observed.size);
            record('W-E1.duplicates.predicted', predicted.duplicated.size);
            record('W-E1.duplicates.predictedLost', predicted.lost.size);
            record('W-E1.duplicates.identitiesOverTwoCopies', extraCopies);
            record('W-E1.duplicates.commitPathThrows',
                OFFSET_STORE_THROWS.length);
            record('W-E1.duplicates.overshootByGroup',
                frozenReport.overshootByGroup);
            record('W-E1.duplicates.overshootTotal', frozenReport.overshoot);
            assert.strictEqual(predicted.lost.size, 0,
                'the drain report exited 0, so it cannot be predicting a ' +
                'record generation 1 still owed');
            // an empty set matching an empty set would pass the comparison
            // below while saying nothing at all about a cutover
            assert(observed.size > 0,
                'no record was delivered twice, so generation 1 never ran ' +
                'past its barriers and there is no overlap to measure');
            assert.deepStrictEqual([...observed].sort(),
                [...predicted.duplicated].sort(),
                'the records delivered twice are not the ones generation ' +
                "1's own committed offsets say it consumed past the barriers");
            // a third copy is one generation delivering the same record
            // twice, which a reshard cannot cause: it is a record whose
            // offset was never stored because the commit path threw, counted
            // in run.commitPathThrows and re-read after the rebalance.
            //
            // Not one throw per record. A single throw abandons the
            // committable offset of that moment, so every entry that had
            // completed behind it is re-read: three extra copies against two
            // throws is the shape actually measured
            assert(extraCopies === 0 || OFFSET_STORE_THROWS.length > 0,
                `${extraCopies} records were delivered more than twice with ` +
                'no commit path throw to account for any of them');
            // the overshoot column counts offsets a group consumed past its
            // barrier, per group. Every old group consumes every partition,
            // whether or not it owns the record, so the column counts each
            // record once per old group and counts the barrier record itself
            oldIds.map(oldGroupId).forEach(groupId => assert.strictEqual(
                frozenReport.overshootByGroup[groupId],
                observed.size + deliveryPartitions,
                `the overshoot of ${groupId} is not the duplicate count ` +
                'plus its own barriers'));
            assert.strictEqual(frozenReport.overshoot,
                oldIds.length * (observed.size + deliveryPartitions));
            return done();
        });

        it('should show each generation 2 workgroup one barrier of its own ' +
        'generation per partition', done => async.eachSeries(newIds,
            (workgroupId, next) => readCounter(BARRIER_METRIC,
                { workgroup: workgroupId, match: 'current' }, (err, value) => {
                    assert.ifError(err);
                    record(`W-E1.barriers.${workgroupId}.current`, value);
                    if (restartedNew.has(workgroupId)) {
                        // a restart re-reads every barrier the workgroup had
                        // not already committed past, so the exact count is
                        // no longer the invariant. The restart is recorded in
                        // run.wedgeOccurrences
                        assert(value >= deliveryPartitions,
                            `${workgroupId} was restarted and still saw ` +
                            `only ${value} barriers of its own generation`);
                        return next();
                    }
                    assert.strictEqual(value, deliveryPartitions,
                        `${workgroupId} saw ${value} barriers of its own ` +
                        'generation, one per partition would be ' +
                        `${deliveryPartitions}`);
                    return next();
                }), err => {
            assert.ifError(err);
            return async.eachSeries(oldIds, (workgroupId, next) =>
                readCounter(BARRIER_METRIC,
                    { workgroup: workgroupId, match: 'other' },
                    (err2, value) => {
                        assert.ifError(err2);
                        // generation 1 consumed the same barriers while it
                        // was still running, and they belong to the
                        // generation that replaces it
                        record(`W-E1.barriers.${workgroupId}.other`, value);
                        assert.strictEqual(value, deliveryPartitions);
                        return next();
                    }), done);
        }));
    });

    describe('E2 :: stopping the old generation before it drained',
    function earlyStopGap() {
        this.timeout(1200000);

        const deliverySpec = TOPICS.e2Delivery;
        const deliveryTopic = deliverySpec.name;
        const deliveryPartitions = deliverySpec.partitions;
        const baseGroupId = `poc-wg-e2-group-${RUN_ID}`;
        const zkPath = `${ZK_BASE}/gate-e2`;
        const cachePath = cachePathFor('gate-e2-cutover');
        const objectsPerDestination = 50;
        const primingRounds = 2;
        const BATCH_SIZE = 5;
        const BATCH_GAP_MS = 250;
        const CUTOVER_DELAY_MS = 500;

        const oldIds = ['e2-g1a', 'e2-g1b'];
        const newIds = ['e2-g2a', 'e2-g2b', 'e2-g2c'];

        const oldPlan = buildHashmodDocument({
            topic: deliveryTopic, generation: 1, modulo: 2, ids: oldIds });
        const newPlan = buildHashmodDocument({
            topic: deliveryTopic, generation: 2, modulo: 3, ids: newIds });

        const moves = [
            { from: oldIds[0], to: newIds[0] },
            { from: oldIds[0], to: newIds[2] },
            { from: oldIds[1], to: newIds[1] },
            { from: oldIds[1], to: newIds[0] },
        ];
        const resources = selectReshardDestinations({
            oldDoc: oldPlan,
            newDoc: newPlan,
            prefix: 'poc-wg-e2-dest',
            wanted: moves,
        });
        const destinations = resources.map((resource, index) =>
            destinationConfig({
                resource,
                topic: TOPICS[`e2Customer${index}`].name,
                // a delivery report arrives on the producer's poll interval,
                // so a wide interval and a narrow pool hold generation 1 to
                // a few records a second. It has to still be well below the
                // barriers when the operator stops it, which is the state
                // this scenario is about, and slowing it is the only way to
                // arrange that without making a destination unreachable and
                // leaving an entry stuck in the queue at close
                pollIntervalMs: 500,
            }));

        const notifConfig = {
            destinations,
            deliveryPool: {
                ...deliveryPoolConfig({
                    topic: deliveryTopic,
                    groupId: baseGroupId,
                    concurrency: 2,
                }),
                workgroups: { zookeeperPath: zkPath, cachePath },
            },
        };
        // the new generation is not the one being held back
        const newNotifConfig = {
            destinations,
            deliveryPool: {
                ...notifConfig.deliveryPool,
                concurrency: 10,
            },
        };

        const eventTypes = ['s3:ObjectCreated:Put', 's3:ObjectCreated:Put',
            's3:ObjectRemoved:Delete'];
        const roundTimes = eventTypes.map((_, index) => eventTime(index));
        const keysOf = new Map();
        destinations.forEach((destination, index) => keysOf.set(
            destination.topic,
            objectKeysFor(`e2-${index}`, objectsPerDestination)));

        const produced = new Set();
        const primingRecords = [];
        const streamBatches = [];
        eventTypes.forEach((eventType, round) => {
            const roundRecords = [];
            for (let k = 0; k < objectsPerDestination; k++) {
                destinations.forEach(destination => {
                    const key = keysOf.get(destination.topic)[k];
                    produced.add(identityOf(destination.topic, key,
                        roundTimes[round]));
                    roundRecords.push(addressedRecord({
                        destination,
                        key,
                        eventType,
                        dateTime: roundTimes[round],
                    }));
                });
            }
            // a backlog is on the topic before generation 1 ever joins, so
            // that it is still working through records the barriers will
            // land far above
            if (round < primingRounds) {
                primingRecords.push(...roundRecords);
                return;
            }
            for (let i = 0; i < roundRecords.length; i += BATCH_SIZE) {
                streamBatches.push(roundRecords.slice(i, i + BATCH_SIZE));
            }
        });
        const totalProduced = produced.size;
        const totalOnDeliveryTopic = totalProduced + deliveryPartitions;

        const tailers = new Map();
        const oldRuntimes = new Map();
        const newRuntimes = new Map();
        const restartedOld = new Set();
        const restartedNew = new Set();
        const oldCommitted = {};
        let stream = null;
        let verifier = null;
        let cutoverResult = null;
        let runningReport = null;
        let frozenReport = null;
        let boundary = null;
        let deliveryIndex = null;

        const oldGroupId = id => buildGroupId(baseGroupId, id, 1);
        const newGroupId = id => buildGroupId(baseGroupId, id, 2);
        const topicOf = destinationId => {
            const found = destinations
                .find(destination => destination.resource === destinationId);
            return found ? found.topic : null;
        };

        function deliveredCount() {
            return [...tailers.values()]
                .reduce((total, tailer) => total + tailer.records.length, 0);
        }

        before(done => {
            const finish = settleOnce(done);
            record('W-E2.destinations', resources);
            record('W-E2.records.produced', totalProduced);
            record('W-E2.records.priming', primingRecords.length);
            record('W-E2.stream.batches', streamBatches.length);
            return async.series([
                next => async.eachSeries(destinations,
                    (destination, tailDone) => {
                        const tailer = new TopicTailer(destination.topic);
                        tailers.set(destination.topic, tailer);
                        return tailer.start(tailDone);
                    }, next),
                next => produceRecords(deliveryTopic, primingRecords, next),
                next => waitForTopics([deliverySpec], err => next(err)),
                next => writeWorkgroupsDocument(zkPath, buildHashmodDocument({
                    topic: deliveryTopic,
                    generation: 1,
                    modulo: 2,
                    ids: oldIds,
                }), next),
                next => startWorkgroups({
                    zkPath,
                    workgroupIds: oldIds,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    runtimes: oldRuntimes,
                }, next),
                next => waitOrRestart({
                    label: 'W-E2 generation 1 first delivery',
                    workgroupIds: oldIds,
                    runtimes: oldRuntimes,
                    restarted: restartedOld,
                    zkPath,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    need: 1,
                    read: (workgroupId, cb) => readCounter(DELIVERED_METRIC,
                        { workgroup: workgroupId }, cb),
                    wait: (waitMs, cb) => async.eachSeries(oldIds, (workgroupId, step) =>
                        waitForCounter(DELIVERED_METRIC,
                            { workgroup: workgroupId }, 1, waitMs, 500, step),
                        cb),
                }, next),
                next => {
                    stream = new RecordStream({
                        topic: deliveryTopic,
                        batches: streamBatches,
                        gapMs: BATCH_GAP_MS,
                    });
                    return stream.start(next);
                },
                next => setTimeout(next, CUTOVER_DELAY_MS),
                next => {
                    const startedAt = Date.now();
                    const cutover = buildCutoverTool({
                        name: 'e2',
                        notifConfig,
                        options: {
                            modulo: 3,
                            workgroup: newIds.map((id, r) => `${id}:${r}`),
                            timeout: 10000,
                        },
                    });
                    return cutover.cutover((err, result) => {
                        cutoverResult = result;
                        record('W-E2.cutover.ms', Date.now() - startedAt);
                        record('W-E2.cutover.recordsStreamedSoFar',
                            stream.sent);
                        return cutover.close(() => next(err));
                    });
                },
                next => {
                    verifier = buildCutoverTool({
                        name: 'e2-verify',
                        notifConfig,
                        options: { timeout: 10000 },
                    });
                    return next();
                },
                // the guard, consulted while generation 1 is still running
                // and still behind, which is the moment the operator ignores
                next => runVerify(verifier, (err, result) => {
                    if (err) {
                        return next(err);
                    }
                    runningReport = result;
                    record('W-E2.verify.running.exitCode', result.exitCode);
                    record('W-E2.verify.running.remaining', result.remaining);
                    return next();
                }),
                // and stopped anyway, which is what this scenario is for
                next => stopWorkgroups(oldRuntimes, next),
                next => async.eachSeries([...tailers.values()],
                    (tailer, quietDone) => waitUntilQuiet(tailer, 500,
                        quietDone), next),
                next => {
                    boundary = snapshotTailerBoundary(tailers);
                    record('W-E2.generation1.delivered', deliveredCount());
                    return next();
                },
                next => runVerify(verifier, (err, result) => {
                    if (err) {
                        return next(err);
                    }
                    frozenReport = result;
                    record('W-E2.verify.stopped.exitCode', result.exitCode);
                    record('W-E2.verify.stopped.remaining', result.remaining);
                    return next();
                }),
                next => async.eachSeries(oldIds, (workgroupId, step) =>
                    readCommitted(oldGroupId(workgroupId), deliveryTopic,
                        deliveryPartitions, (err, offsets) => {
                            if (err) {
                                return step(err);
                            }
                            oldCommitted[oldGroupId(workgroupId)] = offsets;
                            return step();
                        }), next),
                next => async.eachSeries(cutoverResult.groupIds,
                    (groupId, cb) => assertSeededOffsets({
                        kafkaConfig,
                        topic: deliveryTopic,
                        groupId,
                        barriers: cutoverResult.doc.barriers,
                        logger: new werelogs.Logger('seededOffsets:ft'),
                    }, cb), next),
                next => waitForTopics([deliverySpec], err => next(err)),
                next => startWorkgroups({
                    zkPath,
                    workgroupIds: newIds,
                    baseGroupId,
                    notifConfig: newNotifConfig,
                    topic: deliverySpec,
                    runtimes: newRuntimes,
                }, next),
                next => waitFor(() => 'the record stream to finish ' +
                    `(${stream.sent} of ${streamBatches.length * BATCH_SIZE})`,
                    () => stream.finished, 180000, next),
                next => stream.close(next),
                next => {
                    assert.ifError(stream.error);
                    return next();
                },
                next => waitOrRestart({
                    label: 'W-E2 generation 2 commits the whole topic',
                    workgroupIds: newIds,
                    runtimes: newRuntimes,
                    restarted: restartedNew,
                    zkPath,
                    baseGroupId,
                    notifConfig: newNotifConfig,
                    topic: deliverySpec,
                    need: totalOnDeliveryTopic,
                    read: (workgroupId, cb) => committedTotal(
                        newGroupId(workgroupId), deliveryTopic,
                        deliveryPartitions, cb),
                    wait: (waitMs, cb) => async.eachSeries(newIds, (workgroupId, step) =>
                        waitForCommittedTotal(newGroupId(workgroupId),
                            deliveryTopic, deliveryPartitions,
                            totalOnDeliveryTopic, waitMs, step),
                        cb),
                }, next),
                next => async.eachSeries([...tailers.values()],
                    (tailer, quietDone) => waitUntilQuiet(tailer, 500,
                        quietDone), next),
                next => readTopic(deliveryTopic, totalOnDeliveryTopic, 180000,
                    (err, written) => {
                        if (err) {
                            return next(err);
                        }
                        deliveryIndex = indexDeliveryTopic({
                            written,
                            topicOf,
                            oldDoc: oldPlan,
                            newDoc: cutoverResult.doc,
                        });
                        record('W-E2.deliveryTopic.records',
                            deliveryIndex.records.length);
                        return next();
                    }),
                next => {
                    [...newRuntimes.values()].forEach(registerWorkgroup);
                    return next();
                },
            ], finish);
        });

        after(done => async.series([
            next => (stream ? stream.close(next) : next()),
            next => stopWorkgroups(oldRuntimes, next),
            next => stopWorkgroups(newRuntimes, next),
            next => (verifier ? verifier.close(next) : next()),
            next => async.eachSeries([...tailers.values()],
                (tailer, tailDone) => tailer.stop(tailDone), next),
        ], done));

        it('should have refused to call the old generation drained, both ' +
        'while it ran and once it had stopped', () => {
            assert.strictEqual(runningReport.exitCode, EXIT_NOT_DRAINED,
                'the drain report cleared a generation that was still ' +
                'behind its barriers, so there was nothing to ignore');
            assert(runningReport.remaining > 0);
            assert.strictEqual(frozenReport.exitCode, EXIT_NOT_DRAINED);
            assert(frozenReport.remaining > 0,
                'the offsets froze past every barrier, so stopping the old ' +
                'generation cost nothing and this scenario shows nothing');
            record('W-E2.verify.rows', frozenReport.report.rows);
        });

        it('should lose exactly the records the drain report said the old ' +
        'generation still owed', () => {
            const delivered = deliveredRecordsOf(tailers, boundary);
            const seen = new Set(delivered.map(rec => rec.identity));
            const missing = new Set(
                [...produced].filter(id => !seen.has(id)));
            const predicted = predictFromCommittedOffsets({
                index: deliveryIndex,
                barriers: cutoverResult.doc.barriers,
                committedByGroup: oldCommitted,
                groupIdOf: oldGroupId,
            });
            const lostByDestination = {};
            const lostByPartition = {};
            deliveryIndex.records
                .filter(rec => missing.has(rec.identity))
                .forEach(rec => {
                    lostByDestination[rec.destinationId] =
                        (lostByDestination[rec.destinationId] || 0) + 1;
                    lostByPartition[rec.partition] =
                        (lostByPartition[rec.partition] || 0) + 1;
                });
            record('W-E2.gap.produced', totalProduced);
            record('W-E2.gap.delivered', seen.size);
            record('W-E2.gap.lost', missing.size);
            record('W-E2.gap.predictedLost', predicted.lost.size);
            record('W-E2.gap.lostByDestination', lostByDestination);
            record('W-E2.gap.lostByPartition', lostByPartition);
            const unwarned = [...missing].filter(id => !predicted.lost.has(id));
            const warnedButDelivered = [...predicted.lost]
                .filter(id => !missing.has(id));
            record('W-E2.gap.remainingAtStop', frozenReport.remaining);
            record('W-E2.gap.unwarned', unwarned.length);
            record('W-E2.gap.warnedButDelivered', warnedButDelivered.length);
            assert(missing.size > 0,
                'nothing was lost, so stopping a generation that the guard ' +
                'refused to clear cost nothing');
            // the load-bearing property: every record that was lost is one
            // the drain report had already counted as still owed. A record
            // lost that the report did not warn about would be a gap the
            // guard cannot see, which is the failure this design exists to
            // rule out
            assert.deepStrictEqual(unwarned, [],
                'a record was lost that the drain report did not count as ' +
                'still owed, so the guard cannot see the whole gap');
            // the other direction is allowed and is not symmetric. The
            // report reads committed offsets, and a record can be delivered
            // without its offset being stored, so the report over-warns.
            // That only happens when the commit path threw, which is the
            // pre-existing defect counted in run.commitPathThrows
            assert(warnedButDelivered.length === 0 ||
                OFFSET_STORE_THROWS.length > 0,
                'the drain report over-warned about ' +
                `${warnedButDelivered.length} records with no commit path ` +
                'throw to account for their offsets never being stored');
            // remaining counts offsets below the barrier for every old
            // group, whether or not that group owns the record sitting
            // there, so it is an upper bound on the loss and never an
            // under-warning
            assert(missing.size <= frozenReport.remaining,
                `${missing.size} records were lost while the drain report ` +
                `only warned about ${frozenReport.remaining}`);
        });

        it('should have lost nothing at or after a barrier, because the new ' +
        'generation was seeded there', () => {
            const delivered = deliveredRecordsOf(tailers, boundary);
            const seen = new Set(delivered.map(rec => rec.identity));
            const barriers = cutoverResult.doc.barriers;
            const lostAbove = deliveryIndex.records
                .filter(rec => rec.identity !== null &&
                    rec.offset >= Number(barriers[rec.partition]) &&
                    !seen.has(rec.identity))
                .map(rec => rec.identity);
            const above = deliveryIndex.records
                .filter(rec => rec.offset >= Number(barriers[rec.partition]));
            record('W-E2.barrier.recordsFrom', above.length);
            assert.deepStrictEqual(lostAbove, [],
                'a record at or after its barrier was not delivered, which ' +
                'the new generation being seeded at the barrier rules out');
        });
    });

    describe('E3 :: both generations delivering at once',
    function overlappingGenerations() {
        // two commit waits, either of which may have to sit out a wedged
        // consumer being evicted from its group
        this.timeout(2400000);

        const deliverySpec = TOPICS.e3Delivery;
        const deliveryTopic = deliverySpec.name;
        const deliveryPartitions = deliverySpec.partitions;
        const baseGroupId = `poc-wg-e3-group-${RUN_ID}`;
        const zkPath = `${ZK_BASE}/gate-e3`;
        const cachePath = cachePathFor('gate-e3-cutover');
        const objectsPerDestination = 8;
        const BATCH_SIZE = 3;
        const BATCH_GAP_MS = 500;
        const CUTOVER_DELAY_MS = 1000;

        const oldIds = ['e3-g1a', 'e3-g1b'];
        const newIds = ['e3-g2a', 'e3-g2b', 'e3-g2c'];

        const oldPlan = buildHashmodDocument({
            topic: deliveryTopic, generation: 1, modulo: 2, ids: oldIds });
        const newPlan = buildHashmodDocument({
            topic: deliveryTopic, generation: 2, modulo: 3, ids: newIds });

        const moves = [
            { from: oldIds[0], to: newIds[0] },
            { from: oldIds[0], to: newIds[2] },
            { from: oldIds[1], to: newIds[1] },
            { from: oldIds[1], to: newIds[0] },
        ];
        const resources = selectReshardDestinations({
            oldDoc: oldPlan,
            newDoc: newPlan,
            prefix: 'poc-wg-e3-dest',
            wanted: moves,
        });
        const destinations = resources.map((resource, index) =>
            destinationConfig({
                resource,
                topic: TOPICS[`e3Customer${index}`].name,
            }));

        const notifConfig = {
            destinations,
            deliveryPool: {
                ...deliveryPoolConfig({
                    topic: deliveryTopic,
                    groupId: baseGroupId,
                    concurrency: 10,
                }),
                workgroups: { zookeeperPath: zkPath, cachePath },
            },
        };

        const eventTypes = ['s3:ObjectCreated:Put', 's3:ObjectCreated:Put',
            's3:ObjectRemoved:Delete'];
        const roundTimes = eventTypes.map((_, index) => eventTime(index));
        const keysOf = new Map();
        destinations.forEach((destination, index) => keysOf.set(
            destination.topic,
            objectKeysFor(`e3-${index}`, objectsPerDestination)));

        const produced = new Set();
        const primingRecords = [];
        const cutoverBatches = [];
        const overlapBatches = [];
        eventTypes.forEach((eventType, round) => {
            const roundRecords = [];
            for (let k = 0; k < objectsPerDestination; k++) {
                destinations.forEach(destination => {
                    const key = keysOf.get(destination.topic)[k];
                    produced.add(identityOf(destination.topic, key,
                        roundTimes[round]));
                    roundRecords.push(addressedRecord({
                        destination,
                        key,
                        eventType,
                        dateTime: roundTimes[round],
                    }));
                });
            }
            if (round === 0) {
                primingRecords.push(...roundRecords);
                return;
            }
            // the last round is held back and produced only once both
            // generations are running, so they are delivering the same
            // records at the same time rather than taking turns
            const batches = round === 1 ? cutoverBatches : overlapBatches;
            for (let i = 0; i < roundRecords.length; i += BATCH_SIZE) {
                batches.push(roundRecords.slice(i, i + BATCH_SIZE));
            }
        });
        const totalProduced = produced.size;
        const totalOnDeliveryTopic = totalProduced + deliveryPartitions;

        const tailers = new Map();
        const oldRuntimes = new Map();
        const newRuntimes = new Map();
        const bothRuntimes = new Map();
        const restartedOld = new Set();
        const restartedNew = new Set();
        const oldCommitted = {};
        const drainPolls = [];
        let cutoverStream = null;
        let overlapStream = null;
        let verifier = null;
        let cutoverResult = null;
        let drainedReport = null;
        let deliveryIndex = null;
        let overlapWindowMs = 0;

        const oldGroupId = id => buildGroupId(baseGroupId, id, 1);
        const newGroupId = id => buildGroupId(baseGroupId, id, 2);
        const topicOf = destinationId => {
            const found = destinations
                .find(destination => destination.resource === destinationId);
            return found ? found.topic : null;
        };

        function deliveredCount() {
            return [...tailers.values()]
                .reduce((total, tailer) => total + tailer.records.length, 0);
        }

        function waitForDrain(startedAt, timeoutMs, done) {
            const deadline = Date.now() + timeoutMs;
            const attempt = () => runVerify(verifier, (err, result) => {
                if (err) {
                    return done(err);
                }
                drainPolls.push({
                    atMs: Date.now() - startedAt,
                    remaining: result.remaining,
                    exitCode: result.exitCode,
                });
                if (result.report.drained) {
                    return done(null, result.report);
                }
                if (Date.now() >= deadline) {
                    return done(new Error('generation 1 never committed past ' +
                        'every barrier'));
                }
                return setTimeout(attempt, E_DRAIN_POLL_MS);
            });
            return attempt();
        }

        before(done => {
            const finish = settleOnce(done);
            record('W-E3.destinations', resources);
            record('W-E3.records.produced', totalProduced);
            return async.series([
                next => async.eachSeries(destinations,
                    (destination, tailDone) => {
                        const tailer = new TopicTailer(destination.topic);
                        tailers.set(destination.topic, tailer);
                        return tailer.start(tailDone);
                    }, next),
                next => produceRecords(deliveryTopic, primingRecords, next),
                next => waitForTopics([deliverySpec], err => next(err)),
                next => writeWorkgroupsDocument(zkPath, buildHashmodDocument({
                    topic: deliveryTopic,
                    generation: 1,
                    modulo: 2,
                    ids: oldIds,
                }), next),
                next => startWorkgroups({
                    zkPath,
                    workgroupIds: oldIds,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    runtimes: oldRuntimes,
                }, next),
                next => waitOrRestart({
                    label: 'W-E3 generation 1 first delivery',
                    workgroupIds: oldIds,
                    runtimes: oldRuntimes,
                    restarted: restartedOld,
                    zkPath,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    need: 1,
                    read: (workgroupId, cb) => readCounter(DELIVERED_METRIC,
                        { workgroup: workgroupId }, cb),
                    wait: (waitMs, cb) => async.eachSeries(oldIds, (workgroupId, step) =>
                        waitForCounter(DELIVERED_METRIC,
                            { workgroup: workgroupId }, 1, waitMs, 500, step),
                        cb),
                }, next),
                next => {
                    cutoverStream = new RecordStream({
                        topic: deliveryTopic,
                        batches: cutoverBatches,
                        gapMs: BATCH_GAP_MS,
                    });
                    return cutoverStream.start(next);
                },
                next => setTimeout(next, CUTOVER_DELAY_MS),
                next => {
                    const startedAt = Date.now();
                    const cutover = buildCutoverTool({
                        name: 'e3',
                        notifConfig,
                        options: {
                            modulo: 3,
                            workgroup: newIds.map((id, r) => `${id}:${r}`),
                            timeout: 10000,
                        },
                    });
                    return cutover.cutover((err, result) => {
                        cutoverResult = result;
                        record('W-E3.cutover.ms', Date.now() - startedAt);
                        return cutover.close(() => next(err));
                    });
                },
                next => waitFor(() => 'the cutover stream to finish ' +
                    `(${cutoverStream.sent})`,
                    () => cutoverStream.finished, 120000, next),
                next => cutoverStream.close(next),
                next => {
                    verifier = buildCutoverTool({
                        name: 'e3-verify',
                        notifConfig,
                        options: { timeout: 10000 },
                    });
                    return next();
                },
                // the procedure is followed to the letter: the new
                // generation is only started once verify exits 0. What this
                // scenario does not do is stop the old one afterwards
                next => waitOrRestart({
                    label: 'W-E3 generation 1 drain',
                    workgroupIds: oldIds,
                    runtimes: oldRuntimes,
                    restarted: restartedOld,
                    zkPath,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    need: WorkgroupCutover
                        .partitionsOf(cutoverResult.doc.barriers)
                        .reduce((total, partition) =>
                            total + cutoverResult.doc.barriers[partition], 0),
                    read: (workgroupId, cb) => committedTotal(
                        oldGroupId(workgroupId), deliveryTopic,
                        deliveryPartitions, cb),
                    wait: (waitMs, cb) => waitForDrain(Date.now(), waitMs, (err, report) => {
                        drainedReport = report;
                        return cb(err);
                    }),
                }, next),
                next => async.eachSeries(cutoverResult.groupIds,
                    (groupId, cb) => assertSeededOffsets({
                        kafkaConfig,
                        topic: deliveryTopic,
                        groupId,
                        barriers: cutoverResult.doc.barriers,
                        logger: new werelogs.Logger('seededOffsets:ft'),
                    }, cb), next),
                next => waitForTopics([deliverySpec], err => next(err)),
                next => startWorkgroups({
                    zkPath,
                    workgroupIds: newIds,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    runtimes: newRuntimes,
                }, next),
                // both generations are now consuming, and this is the round
                // they consume together
                next => {
                    overlapWindowMs = Date.now();
                    overlapStream = new RecordStream({
                        topic: deliveryTopic,
                        batches: overlapBatches,
                        gapMs: BATCH_GAP_MS,
                    });
                    return overlapStream.start(next);
                },
                next => waitFor(() => 'the overlap stream to finish ' +
                    `(${overlapStream.sent})`,
                    () => overlapStream.finished, 120000, next),
                next => overlapStream.close(next),
                next => {
                    assert.ifError(cutoverStream.error);
                    assert.ifError(overlapStream.error);
                    return next();
                },
                next => {
                    // one map covering both generations, so a workgroup
                    // restarted around a wedge in the wait below is still a
                    // workgroup the after hook knows how to stop
                    oldRuntimes.forEach((runtime, id) =>
                        bothRuntimes.set(id, runtime));
                    newRuntimes.forEach((runtime, id) =>
                        bothRuntimes.set(id, runtime));
                    return next();
                },
                next => waitOrRestart({
                    label: 'W-E3 both generations commit the whole topic',
                    workgroupIds: oldIds.concat(newIds),
                    runtimes: bothRuntimes,
                    restarted: restartedNew,
                    zkPath,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    need: totalOnDeliveryTopic,
                    read: (workgroupId, cb) => committedTotal(
                        oldIds.includes(workgroupId) ?
                            oldGroupId(workgroupId) : newGroupId(workgroupId),
                        deliveryTopic, deliveryPartitions, cb),
                    wait: (waitMs, cb) => async.eachSeries(oldIds.concat(newIds),
                        (workgroupId, step) => waitForCommittedTotal(
                            oldIds.includes(workgroupId) ?
                                oldGroupId(workgroupId) :
                                newGroupId(workgroupId),
                            deliveryTopic, deliveryPartitions,
                            totalOnDeliveryTopic, waitMs, step),
                        cb),
                }, next),
                next => {
                    overlapWindowMs = Date.now() - overlapWindowMs;
                    record('W-E3.overlap.windowMs', overlapWindowMs);
                    return next();
                },
                // by id rather than by map, so the old generation is stopped
                // even if one of its workgroups was restarted above
                next => async.eachSeries(oldIds, (workgroupId, step) =>
                    stopWorkgroup(bothRuntimes.get(workgroupId), () => {
                        bothRuntimes.delete(workgroupId);
                        oldRuntimes.delete(workgroupId);
                        return step();
                    }), next),
                next => async.eachSeries([...tailers.values()],
                    (tailer, quietDone) => waitUntilQuiet(tailer, 500,
                        quietDone), next),
                next => async.eachSeries(oldIds, (workgroupId, step) =>
                    readCommitted(oldGroupId(workgroupId), deliveryTopic,
                        deliveryPartitions, (err, offsets) => {
                            if (err) {
                                return step(err);
                            }
                            oldCommitted[oldGroupId(workgroupId)] = offsets;
                            return step();
                        }), next),
                next => readTopic(deliveryTopic, totalOnDeliveryTopic, 120000,
                    (err, written) => {
                        if (err) {
                            return next(err);
                        }
                        deliveryIndex = indexDeliveryTopic({
                            written,
                            topicOf,
                            oldDoc: oldPlan,
                            newDoc: cutoverResult.doc,
                        });
                        record('W-E3.deliveryTopic.records',
                            deliveryIndex.records.length);
                        record('W-E3.delivered.total', deliveredCount());
                        return next();
                    }),
                next => {
                    [...newRuntimes.values()].forEach(registerWorkgroup);
                    return next();
                },
            ], finish);
        });

        after(done => async.series([
            next => (cutoverStream ? cutoverStream.close(next) : next()),
            next => (overlapStream ? overlapStream.close(next) : next()),
            next => stopWorkgroups(bothRuntimes, next),
            next => stopWorkgroups(oldRuntimes, next),
            next => stopWorkgroups(newRuntimes, next),
            next => (verifier ? verifier.close(next) : next()),
            next => async.eachSeries([...tailers.values()],
                (tailer, tailDone) => tailer.stop(tailDone), next),
        ], done));

        it('should deliver every produced record with no gap while both ' +
        'generations ran', () => {
            const delivered = deliveredRecordsOf(tailers, null);
            const seen = new Set(delivered.map(rec => rec.identity));
            const missing = [...produced].filter(id => !seen.has(id));
            record('W-E3.union.produced', totalProduced);
            record('W-E3.union.deliveredRecords', delivered.length);
            record('W-E3.union.distinctDelivered', seen.size);
            record('W-E3.union.duplicates', delivered.length - seen.size);
            assert(drainedReport.drained,
                'the drain report never cleared generation 1');
            assert.deepStrictEqual(missing, [],
                'a produced record was delivered by neither generation');
        });

        it('should have delivered twice exactly what the old generation ' +
        'consumed past its barriers', () => {
            const delivered = deliveredRecordsOf(tailers, null);
            const copies = copiesByIdentity(delivered);
            const observed = new Set([...copies.entries()]
                .filter(entry => entry[1] > 1).map(entry => entry[0]));
            const predicted = predictFromCommittedOffsets({
                index: deliveryIndex,
                barriers: cutoverResult.doc.barriers,
                committedByGroup: oldCommitted,
                groupIdOf: oldGroupId,
            });
            const extraCopies = [...copies.values()]
                .filter(count => count > 2).length;
            record('W-E3.duplicates.observed', observed.size);
            record('W-E3.duplicates.predicted', predicted.duplicated.size);
            record('W-E3.duplicates.predictedLost', predicted.lost.size);
            record('W-E3.duplicates.identitiesOverTwoCopies', extraCopies);
            record('W-E3.duplicates.commitPathThrows',
                OFFSET_STORE_THROWS.length);
            assert.strictEqual(predicted.lost.size, 0,
                'the old generation was left running, so it cannot still ' +
                'owe a record');
            assert.deepStrictEqual([...observed].sort(),
                [...predicted.duplicated].sort(),
                'the records delivered twice are not the ones the old ' +
                'generation consumed past the barriers');
            // see E1: one throw abandons the committable offset of that
            // moment, so it can strand more than one record
            assert(extraCopies === 0 || OFFSET_STORE_THROWS.length > 0,
                `${extraCopies} records were delivered more than twice with ` +
                'no commit path throw to account for any of them');
        });

        it('should keep every object key in order within each generation, ' +
        'and record how often the merged stream is not', () => {
            const delivered = deliveredRecordsOf(tailers, null);
            const copies = copiesByIdentity(delivered);
            const series = roundsByObjectKey(delivered, roundTimes);
            let inversions = 0;
            let seriesWithInversions = 0;
            let checked = 0;
            series.forEach((events, seriesKey) => {
                const rounds = events.map(event => event.round);
                assert(orderedRunsNeeded(rounds) <= 2,
                    `${seriesKey} needs more than two ordered runs, so one ` +
                    `generation delivered it out of order: ${rounds.join(', ')}`);
                // the copies only one generation delivered are unambiguous,
                // and their order in the customer topic is that generation's
                // delivery order
                const single = events
                    .filter(event => copies.get(event.identity) === 1)
                    .map(event => event.round);
                for (let i = 1; i < single.length; i++) {
                    assert(single[i] >= single[i - 1],
                        `${seriesKey} was delivered out of order by the one ` +
                        `generation that delivered it: ${single.join(', ')}`);
                }
                let seriesInversions = 0;
                for (let i = 1; i < rounds.length; i++) {
                    if (rounds[i] < rounds[i - 1]) {
                        seriesInversions += 1;
                    }
                }
                inversions += seriesInversions;
                if (seriesInversions > 0) {
                    seriesWithInversions += 1;
                }
                checked += 1;
            });
            // measured, not asserted: an old generation copy of an earlier
            // record landing after a new generation copy of a later one is
            // what the overlap window costs a consumer reading the customer
            // topic, and v1 does not claim to prevent it
            record('W-E3.order.seriesChecked', checked);
            record('W-E3.order.crossGenerationInversions', inversions);
            record('W-E3.order.seriesWithInversions', seriesWithInversions);
            assert(checked > 0);
        });
    });

    describe('E4 :: restarting an old worker after the document moved on',
    function recoveryHole() {
        this.timeout(1200000);

        const deliverySpec = TOPICS.e4Delivery;
        const deliveryTopic = deliverySpec.name;
        const deliveryPartitions = deliverySpec.partitions;
        const baseGroupId = `poc-wg-e4-group-${RUN_ID}`;
        const zkPath = `${ZK_BASE}/gate-e4`;
        const cachePath = cachePathFor('gate-e4-cutover');
        const objectsPerDestination = 60;
        const rounds = 3;

        // the operator keeps the workgroup names and only changes the
        // modulo, so the workgroup a crashed worker was deployed as still
        // exists in the new document. That is what makes the restart look
        // legitimate and is what this scenario is about
        const oldIds = ['e4-a', 'e4-b'];
        const newIds = ['e4-a', 'e4-b', 'e4-c'];
        const probed = oldIds[0];

        const oldPlan = buildHashmodDocument({
            topic: deliveryTopic, generation: 1, modulo: 2, ids: oldIds });
        const newPlan = buildHashmodDocument({
            topic: deliveryTopic, generation: 2, modulo: 3, ids: newIds });

        // one destination per generation 1 workgroup. Both of them belonging
        // to the probed workgroup would leave the other one owning nothing,
        // delivering nothing, and never satisfying the liveness wait below
        const resources = selectReshardDestinations({
            oldDoc: oldPlan,
            newDoc: newPlan,
            prefix: 'poc-wg-e4-dest',
            wanted: [
                { from: oldIds[0], to: newIds[2] },
                { from: oldIds[1], to: newIds[1] },
            ],
        });
        const destinations = resources.map((resource, index) =>
            destinationConfig({
                resource,
                topic: TOPICS[`e4Customer${index}`].name,
                // the old generation has to still be short of the barriers
                // when it crashes, which is the state a recovery starts from
                pollIntervalMs: 500,
            }));

        const notifConfig = {
            destinations,
            deliveryPool: {
                ...deliveryPoolConfig({
                    topic: deliveryTopic,
                    groupId: baseGroupId,
                    concurrency: 1,
                }),
                workgroups: { zookeeperPath: zkPath, cachePath },
            },
        };

        const eventTypes = ['s3:ObjectCreated:Put', 's3:ObjectCreated:Put',
            's3:ObjectRemoved:Delete'];
        const records = [];
        for (let round = 0; round < rounds; round++) {
            for (let k = 0; k < objectsPerDestination; k++) {
                destinations.forEach((destination, index) =>
                    records.push(addressedRecord({
                        destination,
                        key: `e4-${index}-obj-${`${k}`.padStart(3, '0')}`,
                        eventType: eventTypes[round],
                        dateTime: eventTime(round),
                    })));
            }
        }

        const oldRuntimes = new Map();
        const restartedOld = new Set();
        let verifier = null;
        let cutoverResult = null;
        let afterCrash = null;
        let pinnedError = null;
        let cachedGeneration = null;
        const defectorRuntimes = new Map();
        const restartedDefector = new Set();
        let defectorGroupId = null;
        let defectorGeneration = null;
        let defectorStartedAt = 0;
        let defectorCommitted = null;
        let defectorBarriers = 0;
        let barriersBeforeDefection = 0;
        let barrierTotal = null;
        let committedBefore = null;
        let committedAfter = null;
        let afterDefection = null;

        const oldGroupId = id => buildGroupId(baseGroupId, id, 1);
        const newGroupId = id => buildGroupId(baseGroupId, id, 2);

        before(done => {
            const finish = settleOnce(done);
            record('W-E4.destinations', resources);
            record('W-E4.records.produced', records.length);
            return async.series([
                next => produceRecords(deliveryTopic, records, next),
                next => waitForTopics([deliverySpec], err => next(err)),
                next => writeWorkgroupsDocument(zkPath, buildHashmodDocument({
                    topic: deliveryTopic,
                    generation: 1,
                    modulo: 2,
                    ids: oldIds,
                }), next),
                next => startWorkgroups({
                    zkPath,
                    workgroupIds: oldIds,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    runtimes: oldRuntimes,
                }, next),
                next => waitOrRestart({
                    label: 'W-E4 generation 1 first delivery',
                    workgroupIds: oldIds,
                    runtimes: oldRuntimes,
                    restarted: restartedOld,
                    zkPath,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    need: 1,
                    read: (workgroupId, cb) => readCounter(DELIVERED_METRIC,
                        { workgroup: workgroupId }, cb),
                    wait: (waitMs, cb) => async.eachSeries(oldIds, (workgroupId, step) =>
                        waitForCounter(DELIVERED_METRIC,
                            { workgroup: workgroupId }, 1, waitMs, 500, step),
                        cb),
                }, next),
                next => {
                    const cutover = buildCutoverTool({
                        name: 'e4',
                        notifConfig,
                        options: {
                            modulo: 3,
                            workgroup: newIds.map((id, r) => `${id}:${r}`),
                            timeout: 10000,
                        },
                    });
                    return cutover.cutover((err, result) => {
                        cutoverResult = result;
                        return cutover.close(() => next(err));
                    });
                },
                // the crash: the old generation stops with the new document
                // already in zookeeper and its own drain unfinished
                next => stopWorkgroups(oldRuntimes, next),
                next => {
                    // where the cutover seeded the new generation, which is
                    // the offset the defector has to be seen moving past
                    barrierTotal = WorkgroupCutover
                        .partitionsOf(cutoverResult.doc.barriers)
                        .reduce((total, partition) =>
                            total + cutoverResult.doc.barriers[partition], 0);
                    verifier = buildCutoverTool({
                        name: 'e4-verify',
                        notifConfig,
                        options: { timeout: 10000 },
                    });
                    return next();
                },
                next => runVerify(verifier, (err, result) => {
                    if (err) {
                        return next(err);
                    }
                    afterCrash = result;
                    record('W-E4.verify.afterCrash.exitCode', result.exitCode);
                    record('W-E4.verify.afterCrash.remaining',
                        result.remaining);
                    return next();
                }),
                next => readCommitted(oldGroupId(probed), deliveryTopic,
                    deliveryPartitions, (err, offsets) => {
                        committedBefore = offsets;
                        return next(err);
                    }),
                // (a) the operator restarts the crashed worker exactly as it
                // was deployed, pinned to the generation it was running
                next => {
                    const loader = new WorkgroupConfigLoader({
                        zkConfig: {
                            connectionString: ZOOKEEPER_HOSTS,
                            autoCreateNamespace: false,
                        },
                        workgroupsConfig: {
                            zookeeperPath: zkPath,
                            cachePath: cachePathFor(probed),
                            generation: 1,
                        },
                        topic: deliveryTopic,
                        workgroupId: probed,
                        logger: new werelogs.Logger(
                            'WorkgroupConfigLoader:e4-pinned'),
                    });
                    return loader.load(err => {
                        pinnedError = err;
                        return loader.stop(() => next());
                    });
                },
                // the cache the crashed worker left behind still holds the
                // rules it was running, which is what makes the refusal
                // worth a design amendment rather than only a warning
                next => fs.readFile(cachePathFor(probed), (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    cachedGeneration = JSON.parse(data).generation;
                    return next();
                }),
                // (b) the same restart with no generation pinned
                next => readCounter(BARRIER_METRIC,
                    { workgroup: probed, match: 'current' }, (err, value) => {
                        barriersBeforeDefection = value;
                        return next(err);
                    }),
                next => startWorkgroups({
                    zkPath,
                    workgroupIds: [probed],
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    runtimes: defectorRuntimes,
                }, err => {
                    defectorStartedAt = Date.now();
                    const runtime = defectorRuntimes.get(probed);
                    if (runtime) {
                        defectorGroupId = runtime.workgroup.groupId;
                        defectorGeneration = runtime.doc.generation;
                    }
                    return next(err);
                }),
                // the old group cannot be called frozen until the defector
                // is demonstrably working somewhere else. Reading the old
                // offsets while the replacement has not even been assigned
                // yet proves nothing: they would read exactly the same if it
                // had rejoined the old group and simply not started
                // consuming.
                //
                // Everything this scenario produced sits below the barriers,
                // so the barriers are the last records on the topic and the
                // new generation's group was seeded exactly at them. A
                // barrier consumed and counted against the generation this
                // worker runs is therefore the defector reading from where
                // the cutover put it and nowhere else.
                //
                // One is enough, and asking for one per partition is asking
                // for something else. A worker that spends its first minutes
                // in an assign and revoke churn holds a different subset of
                // the partitions each time round, so the whole set is a
                // question about the rebalance loop rather than about which
                // group this worker joined. That is what failed here: 47
                // assign and revoke cycles, no barrier inside the first
                // ninety seconds, two of them shortly after.
                //
                // and it gets the wedge handling every other join in this
                // gate has. It is one consumer joining an empty group, and
                // it churns on the way in like any other: 22 assign and
                // revoke cycles with nothing consumed was measured here
                next => waitOrRestart({
                    label: 'W-E4 the restarted worker',
                    workgroupIds: [probed],
                    runtimes: defectorRuntimes,
                    restarted: restartedDefector,
                    zkPath,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    waitMs: E_DEFECTOR_WAIT_MS,
                    need: barriersBeforeDefection + 1,
                    read: (workgroupId, cb) => readCounter(BARRIER_METRIC,
                        { workgroup: workgroupId, match: 'current' }, cb),
                    wait: (waitMs, cb) => waitForCounter(BARRIER_METRIC,
                        { workgroup: probed, match: 'current' },
                        barriersBeforeDefection + 1, waitMs, 500, cb),
                }, next),
                // and waited for, not read once: the counter above rises as
                // the record is handled, while the offset behind it is only
                // committed on the consumer's auto-commit interval, so a
                // single read here would usually still see the seed
                next => waitForCommittedAbove(newGroupId(probed),
                    deliveryTopic, deliveryPartitions, barrierTotal,
                    E_DEFECTOR_WAIT_MS, next),
                next => async.parallel({
                    committed: cb => committedTotal(newGroupId(probed),
                        deliveryTopic, deliveryPartitions, cb),
                    barriers: cb => readCounter(BARRIER_METRIC,
                        { workgroup: probed, match: 'current' }, cb),
                }, (err, seen) => {
                    if (err) {
                        return next(err);
                    }
                    defectorCommitted = seen.committed;
                    defectorBarriers = seen.barriers - barriersBeforeDefection;
                    record('W-E4.defector.workingAfterMs',
                        Date.now() - defectorStartedAt);
                    record('W-E4.defector.committedTotal', seen.committed);
                    record('W-E4.defector.seededAt', barrierTotal);
                    record('W-E4.defector.barriersOfItsOwnGeneration',
                        defectorBarriers);
                    return next();
                }),
                // only now is reading the old group's offsets a statement
                // about anything
                next => readCommitted(oldGroupId(probed), deliveryTopic,
                    deliveryPartitions, (err, offsets) => {
                        committedAfter = offsets;
                        return next(err);
                    }),
                next => runVerify(verifier, (err, result) => {
                    if (err) {
                        return next(err);
                    }
                    afterDefection = result;
                    record('W-E4.verify.afterDefection.exitCode',
                        result.exitCode);
                    record('W-E4.verify.afterDefection.remaining',
                        result.remaining);
                    return next();
                }),
                next => stopWorkgroups(defectorRuntimes, next),
            ], finish);
        });

        after(done => async.series([
            next => stopWorkgroups(oldRuntimes, next),
            next => stopWorkgroups(defectorRuntimes, next),
            next => (verifier ? verifier.close(next) : next()),
        ], done));

        it('should leave the old generation permanently short of its ' +
        'barriers once it has crashed', () => {
            record('W-E4.doc.generation', cutoverResult.doc.generation);
            record('W-E4.doc.previousGroups',
                cutoverResult.doc.previousGroups);
            assert.strictEqual(afterCrash.exitCode, EXIT_NOT_DRAINED,
                'the old generation drained before it crashed, so there is ' +
                'no recovery to attempt');
            assert(afterCrash.remaining > 0);
        });

        it('should refuse to start a worker pinned to the generation it was ' +
        'deployed as, even though its own cache still holds those rules',
        () => {
            assert(pinnedError,
                'a worker pinned to generation 1 loaded a generation 2 ' +
                'document');
            const message = pinnedError.description || pinnedError.message;
            record('W-E4.pinned.message', message);
            record('W-E4.pinned.cachedGeneration', cachedGeneration);
            assert(message.includes('at generation 2'),
                `the refusal has to name the document generation: ${message}`);
            assert(message.includes('pinned to generation 1'),
                `the refusal has to name the pin: ${message}`);
            assert.strictEqual(cachedGeneration, 1,
                'the on-disk cache no longer holds generation 1, so the ' +
                'refusal is not the only thing standing between this worker ' +
                'and its own rules');
        });

        it('should make an unpinned restart join the new generation instead ' +
        'and leave the old drain stalled', () => {
            record('W-E4.defector.derivedGroupId', defectorGroupId);
            record('W-E4.defector.generation', defectorGeneration);
            record('W-E4.defector.abandonedGroupId', oldGroupId(probed));
            assert.strictEqual(defectorGeneration, 2,
                'the restarted worker loaded a document at generation ' +
                `${defectorGeneration}`);
            assert.strictEqual(defectorGroupId, newGroupId(probed),
                'the restarted worker did not derive the new generation ' +
                'group id');
            assert.notStrictEqual(defectorGroupId, oldGroupId(probed),
                'the restarted worker rejoined the group it crashed out of');
            // the defector was working when the old group was read, so the
            // two readings below are a statement about a stalled drain and
            // not about a worker that had not started yet
            record('W-E4.defector.barriersSeen', defectorBarriers);
            assert(defectorBarriers > 0,
                'the restarted worker consumed no barrier of its own ' +
                'generation, so nothing says it was reading from where the ' +
                'cutover seeded the new generation');
            assert(defectorCommitted > barrierTotal,
                'the restarted worker had committed nothing past the ' +
                `offsets the cutover seeded (${defectorCommitted} against ` +
                `${barrierTotal}), so it was not yet consuming when the old ` +
                'group was read and the readings below say nothing');
            assert.strictEqual(afterDefection.exitCode, EXIT_NOT_DRAINED,
                'the drain finished, so the restarted worker did not defect');
            assert.strictEqual(afterDefection.remaining, afterCrash.remaining,
                'the old generation made progress while the defector was ' +
                'working, so it did not abandon its own group');
            assert.deepStrictEqual(committedAfter, committedBefore,
                'the old group committed something while the defector was ' +
                'working, so it is not frozen');
        });
    });

    describe('E5 :: a generation seeded on only some of its partitions',
    function partialPreseed() {
        this.timeout(300000);

        const deliveryTopic = TOPICS.e5Delivery.name;
        const deliveryPartitions = TOPICS.e5Delivery.partitions;
        const groupId = `poc-wg-e5-partial-${RUN_ID}`;
        // one partition is left out of the pre-seed, which is the failure a
        // pre-seed interrupted part way through leaves behind. Nothing in
        // this scenario builds a worker: the point is that the guard rail
        // refuses before one is ever started
        const missingPartition = deliveryPartitions - 1;
        const seeded = [];
        for (let partition = 0; partition < deliveryPartitions; partition++) {
            if (partition !== missingPartition) {
                seeded.push(partition);
            }
        }
        let refusal = null;
        let elapsedMs = 0;

        before(done => async.series([
            next => commitOffsets(groupId, seeded.map(partition => ({
                topic: deliveryTopic,
                partition,
                offset: 0,
            })), next),
            next => {
                const startedAt = Date.now();
                return assertSeededOffsets({
                    kafkaConfig,
                    topic: deliveryTopic,
                    groupId,
                    logger: new werelogs.Logger('seededOffsets:ft'),
                }, err => {
                    refusal = err;
                    elapsedMs = Date.now() - startedAt;
                    return next();
                });
            },
        ], done));

        it('should refuse a partially pre-seeded group, naming the partition ' +
        'nobody seeded and what to run', () => {
            assert(refusal,
                'the startup assertion accepted a group that is seeded on ' +
                `${seeded.length} of ${deliveryPartitions} partitions`);
            const message = refusal.description || refusal.message;
            record('W-E5.seededPartitions', seeded);
            record('W-E5.missingPartition', missingPartition);
            record('W-E5.refusal.message', message);
            record('W-E5.refusal.ms', elapsedMs);
            assert(message.includes(
                `has no committed offset on partitions ${missingPartition}`),
                `the refusal has to name the partition, got: ${message}`);
            assert(message.includes(groupId),
                `the refusal has to name the group, got: ${message}`);
            assert(message.includes(deliveryTopic),
                `the refusal has to name the topic, got: ${message}`);
            assert(message.includes('notificationWorkgroupCutover preseed'),
                `the refusal has to say what to run, got: ${message}`);
            // a refusal that named every partition would be the W-C negative
            // case again, and would not tell an operator where to look
            assert(!message.includes(`partitions ${seeded.join(', ')}`),
                'the refusal named the partitions that are seeded as well, ' +
                `so it does not point at the gap: ${message}`);
            assert(elapsedMs < 30000,
                `the refusal took ${elapsedMs} ms, which is not fast`);
        });
    });

    describe('E6 :: the records nobody configured, under two moduli at once',
    function totalityUnderMixedModuli() {
        this.timeout(2400000);

        const deliverySpec = TOPICS.e6Delivery;
        const deliveryTopic = deliverySpec.name;
        const deliveryPartitions = deliverySpec.partitions;
        const baseGroupId = `poc-wg-e6-group-${RUN_ID}`;
        const zkPath = `${ZK_BASE}/gate-e6`;
        const cachePath = cachePathFor('gate-e6-cutover');
        const objectsPerDestination = 6;
        const BATCH_SIZE = 3;
        const BATCH_GAP_MS = 500;
        const CUTOVER_DELAY_MS = 1000;

        const oldIds = ['e6-g1a', 'e6-g1b'];
        const newIds = ['e6-g2a', 'e6-g2b', 'e6-g2c'];

        const oldPlan = buildHashmodDocument({
            topic: deliveryTopic, generation: 1, modulo: 2, ids: oldIds });
        const newPlan = buildHashmodDocument({
            topic: deliveryTopic, generation: 2, modulo: 3, ids: newIds });

        const resources = selectReshardDestinations({
            oldDoc: oldPlan,
            newDoc: newPlan,
            prefix: 'poc-wg-e6-dest',
            wanted: [
                { from: oldIds[0], to: newIds[0] },
                { from: oldIds[1], to: newIds[1] },
                { from: oldIds[1], to: newIds[2] },
            ],
        });
        // a destination the record names and the configuration does not
        // know, chosen so it changes owner between the two moduli: an
        // unknown id is still routed, and under three workgroups it is
        // routed somewhere else than under two
        const [unknownResource] = selectReshardDestinations({
            oldDoc: oldPlan,
            newDoc: newPlan,
            prefix: 'poc-wg-e6-unknown',
            wanted: [{ from: oldIds[0], to: newIds[2] }],
        });
        const sinkResource = `poc-wg-e6-sink-${RUN_ID}`;

        const plain = resources.map((resource, index) =>
            destinationConfig({
                resource,
                topic: TOPICS[`e6Customer${index}`].name,
            }));
        // nothing is ever addressed to the sink by key: it exists so the one
        // record with an empty key has somewhere to be delivered, which
        // makes its customer topic and its delivered_total series carry that
        // record and nothing else
        const sink = destinationConfig({
            resource: sinkResource,
            topic: TOPICS.e6Customer3.name,
        });
        const destinations = plain.concat([sink]);

        const notifConfig = {
            destinations,
            deliveryPool: {
                ...deliveryPoolConfig({
                    topic: deliveryTopic,
                    groupId: baseGroupId,
                    concurrency: 10,
                }),
                workgroups: { zookeeperPath: zkPath, cachePath },
            },
        };

        const eventTypes = ['s3:ObjectCreated:Put', 's3:ObjectCreated:Put'];
        const roundTimes = eventTypes.map((_, index) => eventTime(index));
        const emptyKeyObject = 'e6-empty-key-obj';
        const unknownObject = 'e6-unknown-dest-obj';
        const keysOf = new Map();
        plain.forEach((destination, index) => keysOf.set(destination.topic,
            objectKeysFor(`e6-${index}`, objectsPerDestination)));

        const produced = new Set();
        const primingRecords = [];
        const streamBatches = [];
        eventTypes.forEach((eventType, round) => {
            const roundRecords = [];
            for (let k = 0; k < objectsPerDestination; k++) {
                plain.forEach(destination => {
                    const key = keysOf.get(destination.topic)[k];
                    produced.add(identityOf(destination.topic, key,
                        roundTimes[round]));
                    roundRecords.push(addressedRecord({
                        destination,
                        key,
                        eventType,
                        dateTime: roundTimes[round],
                    }));
                });
            }
            if (round === 0) {
                primingRecords.push(...roundRecords);
                return;
            }
            for (let i = 0; i < roundRecords.length; i += BATCH_SIZE) {
                streamBatches.push(roundRecords.slice(i, i + BATCH_SIZE));
            }
        });

        // the record for a destination the configuration does not carry,
        // addressed over the same wire contract as every other record
        const unknownRecord = addressedRecord({
            destination: { resource: unknownResource },
            key: unknownObject,
            eventType: 's3:ObjectCreated:Put',
            dateTime: eventTime(0),
        });
        // and the record with no routing key at all, which the ownership
        // rules have to give exactly one owner all the same
        const emptyKeyRecord = {
            ...addressedRecord({
                destination: sink,
                key: emptyKeyObject,
                eventType: 's3:ObjectCreated:Put',
                dateTime: eventTime(0),
            }),
            key: '',
        };
        const specials = [unknownRecord, emptyKeyRecord];

        const totalProduced = produced.size;
        const totalOnDeliveryTopic =
            totalProduced + specials.length + deliveryPartitions;

        const tailers = new Map();
        const oldRuntimes = new Map();
        const newRuntimes = new Map();
        const bothRuntimes = new Map();
        const restartedOld = new Set();
        const restartedNew = new Set();
        const drainPolls = [];
        let stream = null;
        let verifier = null;
        let cutoverResult = null;
        let deliveryIndex = null;

        const oldGroupId = id => buildGroupId(baseGroupId, id, 1);
        const newGroupId = id => buildGroupId(baseGroupId, id, 2);
        const topicOf = destinationId => {
            const found = destinations
                .find(destination => destination.resource === destinationId);
            return found ? found.topic : null;
        };

        function waitForDrain(timeoutMs, done) {
            const deadline = Date.now() + timeoutMs;
            const attempt = () => runVerify(verifier, (err, result) => {
                if (err) {
                    return done(err);
                }
                drainPolls.push(result.remaining);
                if (result.report.drained) {
                    return done();
                }
                if (Date.now() >= deadline) {
                    return done(new Error('generation 1 never committed past ' +
                        'every barrier'));
                }
                return setTimeout(attempt, E_DRAIN_POLL_MS);
            });
            return attempt();
        }

        before(done => {
            const finish = settleOnce(done);
            record('W-E6.destinations', resources);
            record('W-E6.destinations.unknown', unknownResource);
            record('W-E6.destinations.sink', sinkResource);
            record('W-E6.records.produced', totalProduced);
            return async.series([
                next => async.eachSeries(destinations,
                    (destination, tailDone) => {
                        const tailer = new TopicTailer(destination.topic);
                        tailers.set(destination.topic, tailer);
                        return tailer.start(tailDone);
                    }, next),
                next => produceRecords(deliveryTopic, primingRecords, next),
                next => waitForTopics([deliverySpec], err => next(err)),
                next => writeWorkgroupsDocument(zkPath, buildHashmodDocument({
                    topic: deliveryTopic,
                    generation: 1,
                    modulo: 2,
                    ids: oldIds,
                }), next),
                next => startWorkgroups({
                    zkPath,
                    workgroupIds: oldIds,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    runtimes: oldRuntimes,
                }, next),
                next => waitOrRestart({
                    label: 'W-E6 generation 1 first delivery',
                    workgroupIds: oldIds,
                    runtimes: oldRuntimes,
                    restarted: restartedOld,
                    zkPath,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    need: 1,
                    read: (workgroupId, cb) => readCounter(DELIVERED_METRIC,
                        { workgroup: workgroupId }, cb),
                    wait: (waitMs, cb) => async.eachSeries(oldIds, (workgroupId, step) =>
                        waitForCounter(DELIVERED_METRIC,
                            { workgroup: workgroupId }, 1, waitMs, 500, step),
                        cb),
                }, next),
                next => {
                    stream = new RecordStream({
                        topic: deliveryTopic,
                        batches: streamBatches,
                        gapMs: BATCH_GAP_MS,
                    });
                    return stream.start(next);
                },
                next => setTimeout(next, CUTOVER_DELAY_MS),
                next => {
                    const cutover = buildCutoverTool({
                        name: 'e6',
                        notifConfig,
                        options: {
                            modulo: 3,
                            workgroup: newIds.map((id, r) => `${id}:${r}`),
                            timeout: 10000,
                        },
                    });
                    return cutover.cutover((err, result) => {
                        cutoverResult = result;
                        return cutover.close(() => next(err));
                    });
                },
                next => waitFor(() => 'the record stream to finish ' +
                    `(${stream.sent})`, () => stream.finished, 120000, next),
                next => stream.close(next),
                next => {
                    assert.ifError(stream.error);
                    verifier = buildCutoverTool({
                        name: 'e6-verify',
                        notifConfig,
                        options: { timeout: 10000 },
                    });
                    return next();
                },
                next => waitOrRestart({
                    label: 'W-E6 generation 1 drain',
                    workgroupIds: oldIds,
                    runtimes: oldRuntimes,
                    restarted: restartedOld,
                    zkPath,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    need: WorkgroupCutover
                        .partitionsOf(cutoverResult.doc.barriers)
                        .reduce((total, partition) =>
                            total + cutoverResult.doc.barriers[partition], 0),
                    read: (workgroupId, cb) => committedTotal(
                        oldGroupId(workgroupId), deliveryTopic,
                        deliveryPartitions, cb),
                    wait: (waitMs, cb) => waitForDrain(waitMs, cb),
                }, next),
                next => async.eachSeries(cutoverResult.groupIds,
                    (groupId, cb) => assertSeededOffsets({
                        kafkaConfig,
                        topic: deliveryTopic,
                        groupId,
                        barriers: cutoverResult.doc.barriers,
                        logger: new werelogs.Logger('seededOffsets:ft'),
                    }, cb), next),
                next => waitForTopics([deliverySpec], err => next(err)),
                next => startWorkgroups({
                    zkPath,
                    workgroupIds: newIds,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    runtimes: newRuntimes,
                }, next),
                // injected while both generations are running, and above
                // every barrier, so each generation has to give each of them
                // exactly one owner
                next => produceRecords(deliveryTopic, specials, next),
                next => {
                    // one map covering both generations, so a workgroup
                    // restarted around a wedge in the wait below is still a
                    // workgroup the after hook knows how to stop
                    oldRuntimes.forEach((runtime, id) =>
                        bothRuntimes.set(id, runtime));
                    newRuntimes.forEach((runtime, id) =>
                        bothRuntimes.set(id, runtime));
                    return next();
                },
                next => waitOrRestart({
                    label: 'W-E6 both generations commit the whole topic',
                    workgroupIds: oldIds.concat(newIds),
                    runtimes: bothRuntimes,
                    restarted: restartedNew,
                    zkPath,
                    baseGroupId,
                    notifConfig,
                    topic: deliverySpec,
                    need: totalOnDeliveryTopic,
                    read: (workgroupId, cb) => committedTotal(
                        oldIds.includes(workgroupId) ?
                            oldGroupId(workgroupId) : newGroupId(workgroupId),
                        deliveryTopic, deliveryPartitions, cb),
                    wait: (waitMs, cb) => async.eachSeries(oldIds.concat(newIds),
                        (workgroupId, step) => waitForCommittedTotal(
                            oldIds.includes(workgroupId) ?
                                oldGroupId(workgroupId) :
                                newGroupId(workgroupId),
                            deliveryTopic, deliveryPartitions,
                            totalOnDeliveryTopic, waitMs, step),
                        cb),
                }, next),
                next => async.eachSeries(oldIds, (workgroupId, step) =>
                    stopWorkgroup(bothRuntimes.get(workgroupId), () => {
                        bothRuntimes.delete(workgroupId);
                        oldRuntimes.delete(workgroupId);
                        return step();
                    }), next),
                next => async.eachSeries([...tailers.values()],
                    (tailer, quietDone) => waitUntilQuiet(tailer, 500,
                        quietDone), next),
                next => readTopic(deliveryTopic, totalOnDeliveryTopic, 120000,
                    (err, written) => {
                        if (err) {
                            return next(err);
                        }
                        deliveryIndex = indexDeliveryTopic({
                            written,
                            topicOf,
                            oldDoc: oldPlan,
                            newDoc: cutoverResult.doc,
                        });
                        return next();
                    }),
                next => {
                    [...newRuntimes.values()].forEach(registerWorkgroup);
                    return next();
                },
            ], finish);
        });

        after(done => async.series([
            next => (stream ? stream.close(next) : next()),
            next => stopWorkgroups(bothRuntimes, next),
            next => stopWorkgroups(oldRuntimes, next),
            next => stopWorkgroups(newRuntimes, next),
            next => (verifier ? verifier.close(next) : next()),
            next => async.eachSeries([...tailers.values()],
                (tailer, tailDone) => tailer.stop(tailDone), next),
        ], done));

        it('should have put both odd records on the topic with the keys ' +
        'they were meant to have', () => {
            const unknown = deliveryIndex.records
                .filter(rec => rec.destinationId === unknownResource);
            const empty = deliveryIndex.records
                .filter(rec => rec.token === '');
            record('W-E6.unknown.onTopic', unknown.length);
            record('W-E6.emptyKey.onTopic', empty.length);
            record('W-E6.emptyKey.owners', {
                generation1: empty.length ? empty[0].oldOwner : null,
                generation2: empty.length ? empty[0].newOwner : null,
            });
            record('W-E6.unknown.owners', {
                generation1: unknown.length ? unknown[0].oldOwner : null,
                generation2: unknown.length ? unknown[0].newOwner : null,
            });
            assert.strictEqual(unknown.length, 1);
            assert.strictEqual(empty.length, 1,
                'the record produced with an empty key did not arrive with ' +
                'an empty routing token');
        });

        it('should drop the unknown destination in exactly one workgroup of ' +
        'each generation, and deliver it nowhere', done => {
            const generations = [
                { ids: oldIds, doc: oldPlan, label: 'generation 1' },
                { ids: newIds, doc: cutoverResult.doc, label: 'generation 2' },
            ];
            return async.mapSeries(generations, (generation, next) =>
                async.mapSeries(generation.ids, (workgroupId, step) =>
                    readCounter(DROPPED_METRIC, {
                        workgroup: workgroupId,
                        target: unknownResource,
                        reason: 'unknown_destination',
                    }, (err, value) => step(err, { workgroupId, value })),
                    (err, rows) => next(err, { generation, rows })),
                (err, results) => {
                    assert.ifError(err);
                    results.forEach(({ generation, rows }) => {
                        const owner = workgroupIdForDestination(
                            generation.doc, unknownResource);
                        const total = rows
                            .reduce((sum, row) => sum + row.value, 0);
                        record(`W-E6.unknown.drops.${generation.label}`,
                            Object.fromEntries(rows
                                .map(row => [row.workgroupId, row.value])));
                        assert.strictEqual(total, 1,
                            `${generation.label} dropped the unknown ` +
                            `destination ${total} times, exactly one ` +
                            'workgroup owns it');
                        const dropper = rows.find(row => row.value > 0);
                        assert.strictEqual(dropper.workgroupId, owner,
                            `${generation.label} dropped it in ` +
                            `${dropper.workgroupId}, which does not own it`);
                    });
                    return readCounter(DELIVERED_METRIC,
                        { target: unknownResource }, (err2, delivered) => {
                            assert.ifError(err2);
                            record('W-E6.unknown.delivered', delivered);
                            assert.strictEqual(delivered, 0,
                                'a destination the configuration does not ' +
                                'carry cannot have been delivered to');
                            return done();
                        });
                });
        });

        it('should give the record with no key exactly one owner in each ' +
        'generation, and have that owner deliver it once', done => {
            const sinkTailer = tailers.get(sink.topic);
            const copies = sinkTailer.records
                .map(deliveredEvent)
                .filter(event => event.key === emptyKeyObject);
            record('W-E6.emptyKey.copiesDelivered', copies.length);
            assert.strictEqual(sinkTailer.records.length, copies.length,
                'the sink received a record that is not the keyless one');
            const generations = [
                { ids: oldIds, doc: oldPlan, label: 'generation 1' },
                { ids: newIds, doc: cutoverResult.doc, label: 'generation 2' },
            ];
            return async.mapSeries(generations, (generation, next) =>
                async.mapSeries(generation.ids, (workgroupId, step) =>
                    readCounter(DELIVERED_METRIC,
                        { workgroup: workgroupId, target: sinkResource },
                        (err, value) => step(err, { workgroupId, value })),
                    (err, rows) => next(err, { generation, rows })),
                (err, results) => {
                    assert.ifError(err);
                    results.forEach(({ generation, rows }) => {
                        const owner = ownerOfToken(
                            buildOwnershipIndex(generation.doc), '');
                        const total = rows
                            .reduce((sum, row) => sum + row.value, 0);
                        record(`W-E6.emptyKey.delivered.${generation.label}`,
                            Object.fromEntries(rows
                                .map(row => [row.workgroupId, row.value])));
                        assert.strictEqual(total, 1,
                            `${generation.label} delivered the keyless ` +
                            `record ${total} times, one owner processing it ` +
                            'once would be 1');
                        const deliverer = rows.find(row => row.value > 0);
                        assert.strictEqual(deliverer.workgroupId, owner,
                            `${generation.label} delivered it from ` +
                            `${deliverer.workgroupId}, and the empty token ` +
                            `is owned by ${owner}`);
                    });
                    assert.strictEqual(copies.length, generations.length,
                        'the keyless record was not delivered exactly once ' +
                        'per generation');
                    return done();
                });
        });
    });
});
