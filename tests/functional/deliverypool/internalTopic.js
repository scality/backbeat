/**
 * The delivery worker on today's internal topic, against a real broker and
 * a real zookeeper.
 *
 * The migration this proves: the per-destination queue processors have
 * delivered up to some offset each, the containers are replaced, and the new
 * worker group is seeded from the processors' committed offsets with each
 * destination's own offset kept as a watermark. Nothing is delivered twice,
 * nothing is lost, and the destination that was behind receives its backlog.
 * A control worker seeded the same way but without the watermarks shows the
 * duplicates the watermarks remove.
 *
 * Hosts come from KAFKA_HOSTS and ZOOKEEPER_HOSTS, defaulting to the ports
 * of the plain CI stack.
 */
const assert = require('assert');
const async = require('async');
const { AdminClient, KafkaConsumer } = require('node-rdkafka');

const werelogs = require('werelogs');

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const ZookeeperManager = require('../../../lib/clients/ZookeeperManager');
const DeliveryWorker =
    require('../../../extensions/notification/deliveryWorker/DeliveryWorker');
const DeliverySeeder =
    require('../../../extensions/notification/deliveryWorker/DeliverySeeder');
const WorkgroupConfigLoader =
    require('../../../extensions/notification/deliveryWorker/WorkgroupConfigLoader');
const messageUtil = require('../../../extensions/notification/utils/message');
const {
    buildGroupId,
    createSliceFilter,
} = require('../../../extensions/notification/utils/workgroups');

const KAFKA_HOSTS = process.env.KAFKA_HOSTS || 'localhost:9092';
const ZOOKEEPER_HOSTS = process.env.ZOOKEEPER_HOSTS || 'localhost:2181';
const CONNECT_TIMEOUT = 20000;
const STOP_TIMEOUT = 20000;
const METADATA_TIMEOUT = 10000;
const TOPIC_ALREADY_EXISTS = 36;
const RUN_ID = `${Date.now()}`;
const BUCKET = 'ftint-bucket';
const RECORDS = 40;
const PARTITIONS = 2;

// the processors were at different points: dest-a had delivered most of
// each partition, dest-b only a little
const DELIVERED_SHARE = { 'dest-a': 0.75, 'dest-b': 0.25 };

const PROCESSOR_GROUP = `ftint-qp-${RUN_ID}`;
const POOL_GROUP = `ftint-pool-${RUN_ID}`;
const CONTROL_GROUP = `ftint-control-${RUN_ID}`;
const ZK_PATH = `/ftint-${RUN_ID}/delivery-workgroups`;
const WORKGROUP = 'wg-all';
const GENERATION = 1;

const TOPICS = {
    internal: { name: `ftint-internal-${RUN_ID}`, partitions: PARTITIONS },
    custA: { name: `ftint-cust-a-${RUN_ID}`, partitions: 1 },
    custB: { name: `ftint-cust-b-${RUN_ID}`, partitions: 1 },
    controlA: { name: `ftint-control-a-${RUN_ID}`, partitions: 1 },
    controlB: { name: `ftint-control-b-${RUN_ID}`, partitions: 1 },
    // a bucket whose configuration shows up after the worker started
    lateInternal: { name: `ftint-late-internal-${RUN_ID}`, partitions: 1 },
    lateCust: { name: `ftint-late-cust-${RUN_ID}`, partitions: 1 },
};
const LATE_BUCKET = 'ftint-late-bucket';
const LATE_GROUP = `ftint-late-${RUN_ID}`;
const LATE_RECORDS = 10;

const log = new werelogs.Logger('Backbeat:Test:InternalTopic');
werelogs.configure({ level: 'warn', dump: 'error' });

const kafkaConfig = { hosts: KAFKA_HOSTS };
const zkConfig = { connectionString: ZOOKEEPER_HOSTS, autoCreateNamespace: true };

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

function waitForTopics(topics, done) {
    return withConsumer(`ftint-meta-${RUN_ID}`, (consumer, cb) => {
        const deadline = Date.now() + 60000;
        let stable = 0;
        const check = () => consumer.getMetadata({ timeout: METADATA_TIMEOUT },
            (err, metadata) => {
                const missing = err ? topics : topics.filter(topic => {
                    const found = metadata.topics.find(t => t.name === topic.name);
                    return !found || found.partitions.length !== topic.partitions ||
                        !found.partitions.every(p => p.leader >= 0);
                });
                stable = missing.length === 0 ? stable + 1 : 0;
                if (stable >= 3) {
                    return cb();
                }
                if (Date.now() >= deadline) {
                    return cb(new Error(`timed out waiting for topics: ${ 
                        missing.map(t => t.name).join(', ')}`));
                }
                return setTimeout(check, 1000);
            });
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

function committedOffsets(groupId, topic, partitions, done) {
    return withConsumer(groupId, (consumer, cb) => consumer.committed(
        partitions.map(partition => ({ topic, partition })),
        METADATA_TIMEOUT, (err, toppars) => {
            if (err) {
                return cb(err);
            }
            const offsets = {};
            (toppars || []).forEach(tp => { offsets[tp.partition] = tp.offset; });
            return cb(null, offsets);
        }), done);
}

function highWatermarks(topic, partitions, done) {
    return withConsumer(`ftint-wm-${RUN_ID}`, (consumer, cb) => {
        const highs = {};
        return async.eachSeries(partitions, (partition, next) =>
            consumer.queryWatermarkOffsets(topic, partition, METADATA_TIMEOUT,
                (err, offsets) => {
                    if (err) {
                        return next(err);
                    }
                    highs[partition] = offsets.highOffset;
                    return next();
                }), err => cb(err, highs));
    }, done);
}

/**
 * Reads a whole topic from its first offset, until nothing new shows up
 * for half a second after at least minCount records
 */
function readTopic(topic, minCount, timeoutMs, done) {
    const consumer = new KafkaConsumer({
        'metadata.broker.list': KAFKA_HOSTS,
        'group.id': `ftint-reader-${topic}`,
        'enable.auto.commit': false,
        'enable.auto.offset.store': false,
    }, {});
    consumer.on('error', () => {});
    consumer.on('event.error', () => {});
    const records = [];
    const deadline = Date.now() + timeoutMs;
    let quietSince = null;
    let finished = false;
    const finish = err => {
        if (finished) {
            return;
        }
        finished = true;
        consumer.disconnect(() => done(err, records));
    };
    const poll = () => consumer.consume(100, (err, batch) => {
        if (!err && batch && batch.length > 0) {
            batch.forEach(record => records.push({
                partition: record.partition,
                offset: record.offset,
                key: record.key === null || record.key === undefined ?
                    null : record.key.toString(),
                value: record.value.toString(),
            }));
            quietSince = null;
        } else if (records.length >= minCount) {
            quietSince = quietSince || Date.now();
            if (Date.now() - quietSince >= 500) {
                return finish();
            }
        }
        if (Date.now() >= deadline) {
            return finish(new Error(`timed out reading ${topic}: ` +
                `${records.length} records, wanted at least ${minCount}`));
        }
        return setTimeout(poll, 50);
    });
    return consumer.connect({ timeout: CONNECT_TIMEOUT }, connectErr => {
        if (connectErr) {
            return finish(connectErr);
        }
        return consumer.getMetadata({ topic, timeout: METADATA_TIMEOUT },
            (mdErr, metadata) => {
                if (mdErr) {
                    return finish(mdErr);
                }
                const found = metadata.topics.find(t => t.name === topic);
                consumer.assign(found.partitions.map(p =>
                    ({ topic, partition: p.id, offset: 0 })));
                return poll();
            });
    });
}

function waitFor(what, predicate, timeoutMs, done) {
    const deadline = Date.now() + timeoutMs;
    const check = () => predicate((err, ok) => {
        if (err) {
            return done(err);
        }
        if (ok) {
            return done();
        }
        if (Date.now() >= deadline) {
            return done(new Error(`timed out waiting for ${what}`));
        }
        return setTimeout(check, 500);
    });
    return check();
}

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

function legacyRecord(index, bucket) {
    const key = `object-${String(index).padStart(4, '0')}`;
    return {
        key: `${bucket || BUCKET}/${key}`,
        message: JSON.stringify({
            bucket: bucket || BUCKET,
            key,
            eventType: 's3:ObjectCreated:Put',
            dateTime: new Date(Date.UTC(2026, 0, 1, 0, 0, index)).toISOString(),
            versionId: null,
            size: '1024',
            region: 'us-east-1',
            schemaVersion: '5',
        }),
    };
}

/**
 * What a queue processor would have delivered for an internal record
 * @param {Object} record - internal topic record, as read back
 * @param {String} configurationId - matching configuration id
 * @return {Object} kafka message for the customer topic
 */
function processorDelivery(record, configurationId) {
    const entry = JSON.parse(record.value);
    entry.configurationId = configurationId;
    return {
        key: `${entry.bucket}/${entry.key}`,
        message: JSON.stringify(messageUtil.transformToSpec(entry)),
    };
}

function deliveredKeys(records) {
    return records.map(record =>
        JSON.parse(record.value).Records[0].s3.object.key);
}

function destination(resource, topic) {
    return {
        resource,
        type: 'kafka',
        host: KAFKA_HOSTS.split(':')[0],
        port: Number(KAFKA_HOSTS.split(':')[1]),
        topic,
    };
}

function notifConfigFor(destinations, groupId) {
    return {
        topic: TOPICS.internal.name,
        queueProcessor: { groupId: PROCESSOR_GROUP, concurrency: 10 },
        destinations,
        deliveryPool: {
            enabled: true,
            source: 'internal',
            topic: `ftint-unused-delivery-${RUN_ID}`,
            groupId,
            deliveryTimeoutMs: 30000,
            producerIdleMs: 300000,
            maxProducers: 50,
            concurrency: 10,
            maxQueued: 100,
            workgroups: {
                id: WORKGROUP,
                zookeeperPath: ZK_PATH,
                cachePath: `/tmp/ftint-workgroups-${RUN_ID}-${groupId}.json`,
            },
        },
    };
}

// both destinations take every event of the bucket
const bucketConfig = {
    bucket: BUCKET,
    notificationConfiguration: {
        queueConfig: [
            { id: 'all-to-a', queueArn: 'arn:scality:bucketnotif:::dest-a',
                events: ['s3:ObjectCreated:*'] },
            { id: 'all-to-b', queueArn: 'arn:scality:bucketnotif:::dest-b',
                events: ['s3:ObjectCreated:*'] },
        ],
    },
};

// the configurations the worker can see, mutable so a test can make one
// appear while the worker is running
const knownConfigs = { [BUCKET]: bucketConfig };
const configManager = {
    getConfig: (bucket, cb) => process.nextTick(() =>
        cb(null, knownConfigs[bucket])),
    setup: cb => process.nextTick(cb),
};

const workgroupsDoc = {
    configVersion: 1,
    generation: GENERATION,
    topic: TOPICS.internal.name,
    workgroups: [
        { id: WORKGROUP, rule: { type: 'hashmod', modulo: 1, remainders: [0] } },
    ],
};

function workgroupFor(groupId) {
    return {
        id: WORKGROUP,
        generation: GENERATION,
        groupId,
        filter: createSliceFilter({ doc: workgroupsDoc, workgroupId: WORKGROUP }),
    };
}

/**
 * Waits until a group has committed up to the head of every partition
 */
function waitForDrain(groupId, done) {
    const partitions = [...Array(PARTITIONS).keys()];
    return waitFor(`${groupId} to reach the head`, cb =>
        highWatermarks(TOPICS.internal.name, partitions, (err, highs) => {
            if (err) {
                return cb(err);
            }
            return committedOffsets(groupId, TOPICS.internal.name, partitions,
                (cErr, committed) => {
                    if (cErr) {
                        return cb(cErr);
                    }
                    return cb(null, partitions.every(p =>
                        committed[p] === highs[p]));
                });
        }), 90000, done);
}

describe('delivery worker on the internal topic', function internalTopic() {
    this.timeout(180000);

    let zkClient = null;
    let internalRecords = [];
    // per destination, per partition: the offset the processor had committed
    const processorOffsets = { 'dest-a': {}, 'dest-b': {} };
    let seedResult = null;

    before(done => async.series([
        next => createTopics(Object.values(TOPICS), next),
        next => waitForTopics(Object.values(TOPICS), next),
        // map passes the index as a second argument, which legacyRecord
        // would take for the bucket name
        next => produceRecords(TOPICS.internal.name,
            [...Array(RECORDS).keys()].map(i => legacyRecord(i)), next),
        next => readTopic(TOPICS.internal.name, RECORDS, 60000, (err, records) => {
            if (err) {
                return next(err);
            }
            internalRecords = records;
            return next();
        }),
        // what the processors delivered before the swap, and where they
        // committed: a prefix of every partition, per destination
        next => async.eachSeries(Object.keys(DELIVERED_SHARE), (dest, cb) => {
            const share = DELIVERED_SHARE[dest];
            const delivered = [];
            [...Array(PARTITIONS).keys()].forEach(partition => {
                const inPartition = internalRecords
                    .filter(r => r.partition === partition)
                    .sort((a, b) => a.offset - b.offset);
                const count = Math.floor(inPartition.length * share);
                processorOffsets[dest][partition] = count;
                delivered.push(...inPartition.slice(0, count));
            });
            const configurationId = dest === 'dest-a' ? 'all-to-a' : 'all-to-b';
            const messages = delivered.map(r => processorDelivery(r, configurationId));
            const topics = dest === 'dest-a' ?
                [TOPICS.custA.name, TOPICS.controlA.name] :
                [TOPICS.custB.name, TOPICS.controlB.name];
            return async.series([
                n => produceRecords(topics[0], messages, n),
                n => produceRecords(topics[1], messages, n),
                n => commitOffsets(`${PROCESSOR_GROUP}-${dest}`,
                    Object.keys(processorOffsets[dest]).map(partition => ({
                        topic: TOPICS.internal.name,
                        partition: Number(partition),
                        offset: processorOffsets[dest][partition],
                    })), n),
            ], cb);
        }, next),
        next => {
            zkClient = new ZookeeperManager(ZOOKEEPER_HOSTS, {
                autoCreateNamespace: true,
            }, log);
            zkClient.once('error', next);
            zkClient.once('ready', () => {
                zkClient.removeAllListeners('error');
                zkClient.setOrCreate(ZK_PATH,
                    Buffer.from(JSON.stringify(workgroupsDoc)), next);
            });
        },
    ], done));

    after(done => {
        if (zkClient) {
            zkClient.close();
        }
        done();
    });

    it('seeds the worker group from the processors and writes the watermarks', done => {
        const seeder = new DeliverySeeder({
            kafkaConfig,
            zkConfig,
            notifConfig: notifConfigFor([
                destination('dest-a', TOPICS.custA.name),
                destination('dest-b', TOPICS.custB.name),
            ], POOL_GROUP),
            options: { generation: GENERATION },
            logger: log,
        });
        seeder.seedFromProcessors((err, result) => seeder.close(() => {
            assert.ifError(err);
            seedResult = result;
            const [group] = result.groups;
            assert.strictEqual(group.groupId,
                buildGroupId(POOL_GROUP, WORKGROUP, GENERATION));
            [...Array(PARTITIONS).keys()].forEach(partition => {
                const expected = Math.min(processorOffsets['dest-a'][partition],
                    processorOffsets['dest-b'][partition]);
                assert.strictEqual(group.offsets[partition].offset, expected,
                    `partition ${partition} seeded at the lowest processor offset`);
                assert.strictEqual(result.watermarks['dest-a'][partition],
                    processorOffsets['dest-a'][partition]);
                assert.strictEqual(result.watermarks['dest-b'][partition],
                    processorOffsets['dest-b'][partition]);
            });
            assert.strictEqual(result.watermarksPath,
                `${ZK_PATH}/watermarks/gen${GENERATION}`);
            return committedOffsets(group.groupId, TOPICS.internal.name,
                [...Array(PARTITIONS).keys()], (cErr, committed) => {
                    assert.ifError(cErr);
                    [...Array(PARTITIONS).keys()].forEach(partition => {
                        assert.strictEqual(committed[partition],
                            group.offsets[partition].offset);
                    });
                    done();
                });
        }));
    });

    it('delivers everything once with the watermarks: no gap, no duplicate', done => {
        const notifConfig = notifConfigFor([
            destination('dest-a', TOPICS.custA.name),
            destination('dest-b', TOPICS.custB.name),
        ], POOL_GROUP);
        const groupId = buildGroupId(POOL_GROUP, WORKGROUP, GENERATION);
        // the worker loads its watermarks the way the task does
        const loader = new WorkgroupConfigLoader({
            zkConfig,
            workgroupsConfig: notifConfig.deliveryPool.workgroups,
            topic: TOPICS.internal.name,
            workgroupId: WORKGROUP,
            logger: log,
        });
        let worker = null;
        return async.waterfall([
            next => loader.load(err => next(err)),
            next => loader.loadWatermarks(next),
            (watermarks, next) => {
                assert.deepStrictEqual(watermarks, seedResult.watermarks);
                worker = new DeliveryWorker(kafkaConfig, notifConfig,
                    workgroupFor(groupId), { configManager, watermarks });
                worker.start(null, next);
            },
            next => waitForDrain(groupId, next),
            next => readTopic(TOPICS.custA.name, RECORDS, 60000, next),
            (recordsA, next) => readTopic(TOPICS.custB.name, RECORDS, 60000,
                (err, recordsB) => next(err, recordsA, recordsB)),
            (recordsA, recordsB, next) => {
                const keysA = deliveredKeys(recordsA);
                const keysB = deliveredKeys(recordsB);
                assert.strictEqual(new Set(keysA).size, RECORDS,
                    'dest-a: every object reached the destination');
                assert.strictEqual(keysA.length, RECORDS,
                    'dest-a: nothing was delivered twice');
                assert.strictEqual(new Set(keysB).size, RECORDS,
                    'dest-b: every object reached the destination');
                assert.strictEqual(keysB.length, RECORDS,
                    'dest-b: nothing was delivered twice');
                next();
            },
        ], err => {
            loader.stop();
            stopWorker(worker, () => done(err));
        });
    });

    it('delivers the seeded window twice without the watermarks, and still loses nothing', done => {
        const notifConfig = notifConfigFor([
            destination('dest-a', TOPICS.controlA.name),
            destination('dest-b', TOPICS.controlB.name),
        ], CONTROL_GROUP);
        const groupId = buildGroupId(CONTROL_GROUP, WORKGROUP, GENERATION);
        const partitions = [...Array(PARTITIONS).keys()];
        // seeded exactly as the pool group was, but no watermarks
        const seeded = partitions.map(partition => ({
            topic: TOPICS.internal.name,
            partition,
            offset: Math.min(processorOffsets['dest-a'][partition],
                processorOffsets['dest-b'][partition]),
        }));
        const expectedDuplicatesA = partitions.reduce((sum, partition) =>
            sum + processorOffsets['dest-a'][partition] -
                Math.min(processorOffsets['dest-a'][partition],
                    processorOffsets['dest-b'][partition]), 0);
        const expectedDuplicatesB = partitions.reduce((sum, partition) =>
            sum + processorOffsets['dest-b'][partition] -
                Math.min(processorOffsets['dest-a'][partition],
                    processorOffsets['dest-b'][partition]), 0);
        assert(expectedDuplicatesA > 0, 'the fixture must make dest-a the one ahead');
        let worker = null;
        return async.waterfall([
            next => commitOffsets(groupId, seeded, err => next(err)),
            next => {
                worker = new DeliveryWorker(kafkaConfig, notifConfig,
                    workgroupFor(groupId), { configManager, watermarks: null });
                worker.start(null, next);
            },
            next => waitForDrain(groupId, next),
            next => readTopic(TOPICS.controlA.name, RECORDS, 60000, next),
            (recordsA, next) => readTopic(TOPICS.controlB.name, RECORDS, 60000,
                (err, recordsB) => next(err, recordsA, recordsB)),
            (recordsA, recordsB, next) => {
                const keysA = deliveredKeys(recordsA);
                const keysB = deliveredKeys(recordsB);
                assert.strictEqual(new Set(keysA).size, RECORDS, 'dest-a: no gap');
                assert.strictEqual(new Set(keysB).size, RECORDS, 'dest-b: no gap');
                assert.strictEqual(keysA.length - RECORDS, expectedDuplicatesA,
                    'dest-a: the records between the seed and its own offset ' +
                    'are delivered again');
                assert.strictEqual(keysB.length - RECORDS, expectedDuplicatesB,
                    'dest-b: seeded at its own offset, nothing twice');
                next();
            },
        ], err => stopWorker(worker, () => done(err)));
    });

    it('delivers a bucket whose configuration appears after the worker started', done => {
        // an empty group on a topic that already holds the records, and a
        // bucket the configuration store does not know yet: the worker must
        // read the configuration again rather than commit the records away
        const notifConfig = notifConfigFor([
            destination('dest-a', TOPICS.lateCust.name),
        ], LATE_GROUP);
        notifConfig.topic = TOPICS.lateInternal.name;
        delete notifConfig.deliveryPool.workgroups;
        let worker = null;
        return async.series([
            next => produceRecords(TOPICS.lateInternal.name,
                [...Array(LATE_RECORDS).keys()].map(i => legacyRecord(i, LATE_BUCKET)),
                next),
            next => {
                worker = new DeliveryWorker(kafkaConfig, notifConfig, null,
                    { configManager, watermarks: null });
                worker.start(null, next);
            },
            // the worker is consuming: the records are being looked up and
            // finding nothing, inside the retry window
            next => setTimeout(next, 3000),
            next => {
                knownConfigs[LATE_BUCKET] = {
                    bucket: LATE_BUCKET,
                    notificationConfiguration: {
                        queueConfig: [{
                            id: 'late-to-a',
                            queueArn: 'arn:scality:bucketnotif:::dest-a',
                            events: ['s3:ObjectCreated:*'],
                        }],
                    },
                };
                return next();
            },
            next => readTopic(TOPICS.lateCust.name, LATE_RECORDS, 60000,
                (err, records) => {
                    if (err) {
                        return next(err);
                    }
                    const keys = deliveredKeys(records);
                    assert.strictEqual(new Set(keys).size, LATE_RECORDS,
                        'every record of the late bucket reached the destination');
                    assert.strictEqual(keys.length, LATE_RECORDS,
                        'and none of them twice');
                    return next();
                }),
        ], err => stopWorker(worker, () => done(err)));
    });
});
