const assert = require('assert');
const { CODES } = require('node-rdkafka');

const FakeLogger = require('../../utils/fakeLogger');
const DeliverySeeder = require(
    '../../../extensions/notification/deliveryWorker/DeliverySeeder');
const { buildHistoryPath } = DeliverySeeder;

const TOPIC = 'backbeat-bucket-notification';
const ZK_PATH = '/notification/delivery-workgroups';
const BASE_GROUP = 'delivery-group';
const PROCESSOR_GROUP = 'notification-group';

const kafkaConfig = { hosts: 'kafka:9092' };
const zkConfig = { connectionString: 'zookeeper:2181' };

function destination(resource, extra) {
    return Object.assign({
        resource, type: 'kafka', host: 'h', port: 9092, topic: `${resource}-t`,
    }, extra || {});
}

function makeNotifConfig(overrides) {
    return Object.assign({
        topic: TOPIC,
        queueProcessor: { groupId: PROCESSOR_GROUP },
        destinations: [
            destination('dest-a'),
            destination('dest-b'),
            destination('dest-c'),
            destination('dest-own', { internalTopic: 'own-topic' }),
        ],
        deliveryPool: {
            enabled: true,
            source: 'internal',
            topic: 'delivery-topic',
            groupId: BASE_GROUP,
            workgroups: { zookeeperPath: ZK_PATH },
        },
    }, overrides || {});
}

// wg-a owns dest-a by name, wg-b owns everything else through modulo 1
function makeDoc(generation, workgroups) {
    return {
        configVersion: 1,
        generation,
        topic: TOPIC,
        workgroups: workgroups || [
            { id: 'wg-a', rule: { type: 'static', destinationIds: ['dest-a'] } },
            { id: 'wg-b', rule: { type: 'hashmod', modulo: 1, remainders: [0] } },
        ],
    };
}

/**
 * A zookeeper client over an in-memory map of node paths to JSON values
 * @param {Object} nodes - path to value
 * @return {Object} client stub, exposing the map as .nodes
 */
function fakeZk(nodes) {
    const client = {
        nodes: Object.assign({}, nodes),
        writes: [],
        closed: 0,
        getData(path, watcher, cb) {
            return process.nextTick(() => {
                if (!(path in client.nodes)) {
                    return cb({ name: 'NO_NODE' });
                }
                return cb(null, Buffer.from(JSON.stringify(client.nodes[path])));
            });
        },
        setOrCreate(path, data, cb) {
            client.writes.push(path);
            client.nodes[path] = JSON.parse(data.toString());
            return process.nextTick(cb);
        },
        close() { client.closed += 1; },
    };
    return client;
}

/**
 * The metadata and watermark consumer
 * @param {Number[]} partitions - partition ids
 * @param {Object} lows - partition to low watermark
 * @param {Object} highs - partition to high watermark
 * @return {Object} consumer stub
 */
function fakeConsumer(partitions, lows, highs) {
    return {
        getMetadata(opts, cb) {
            return process.nextTick(() => cb(null, {
                topics: [{ name: opts.topic, partitions: partitions.map(id => ({ id })) }],
            }));
        },
        queryWatermarkOffsets(topic, partition, timeout, cb) {
            return process.nextTick(() => cb(null, {
                lowOffset: lows[partition], highOffset: highs[partition],
            }));
        },
        disconnect(cb) { return process.nextTick(cb); },
    };
}

/**
 * Consumers bound to groups, over a table of committed offsets that
 * commitSync updates, so a verify read sees what was seeded
 * @param {Object} table - groupId to { partition: offset }
 * @param {Object} [options] - { throwOnCommit: groupId to error code }
 * @return {Object} { factory, table, commits }
 */
function fakeGroups(initial, options) {
    const table = Object.assign({}, initial);
    const commits = [];
    const factory = groupId => ({
        connect(opts, cb) { return process.nextTick(cb); },
        disconnect(cb) { return process.nextTick(cb); },
        committed(toppars, timeout, cb) {
            const offsets = table[groupId] || {};
            return process.nextTick(() => cb(null, toppars.map(tp => ({
                topic: tp.topic,
                partition: tp.partition,
                offset: tp.partition in offsets ? offsets[tp.partition] : -1001,
            }))));
        },
        assign() {},
        commitSync(toppars) {
            const code = options && options.throwOnCommit &&
                options.throwOnCommit[groupId];
            if (code !== undefined) {
                const err = new Error('commit refused');
                err.code = code;
                throw err;
            }
            table[groupId] = table[groupId] || {};
            toppars.forEach(tp => { table[groupId][tp.partition] = tp.offset; });
            commits.push({ groupId, toppars });
        },
    });
    return { factory, table, commits };
}

function makeSeeder(params) {
    return new DeliverySeeder(Object.assign({
        kafkaConfig,
        zkConfig,
        notifConfig: makeNotifConfig(),
        logger: FakeLogger,
    }, params));
}

describe('notification DeliverySeeder', () => {
    describe('seedFromProcessors', () => {
        it('should seed each group at the lowest offset of its destinations and write watermarks', done => {
            const zk = fakeZk({ [ZK_PATH]: makeDoc(1) });
            const groups = fakeGroups({
                [`${PROCESSOR_GROUP}-dest-a`]: { 0: 100, 1: 50 },
                [`${PROCESSOR_GROUP}-dest-b`]: { 0: 80, 1: 60 },
                // dest-c never committed: it gets everything from the start
            });
            const seeder = makeSeeder({
                options: { generation: '1' },
                zkClient: zk,
                consumer: fakeConsumer([0, 1], { 0: 10, 1: 0 }, { 0: 500, 1: 300 }),
                groupClientFactory: groups.factory,
            });
            seeder.seedFromProcessors((err, result) => {
                assert.ifError(err);
                assert.strictEqual(result.generation, 1);
                assert.deepStrictEqual(result.partitions, [0, 1]);
                const byId = {};
                result.groups.forEach(g => { byId[g.groupId] = g; });
                const a = byId[`${BASE_GROUP}-wg-a-gen1`];
                const b = byId[`${BASE_GROUP}-wg-b-gen1`];
                assert.deepStrictEqual(a.destinations, ['dest-a']);
                assert.strictEqual(a.offsets[0].offset, 100);
                assert.strictEqual(a.offsets[1].offset, 50);
                assert.deepStrictEqual(b.destinations, ['dest-b', 'dest-c']);
                // dest-c has no offset, so it pulls the group to the low
                // watermark on both partitions
                assert.strictEqual(b.offsets[0].offset, 10);
                assert.strictEqual(b.offsets[1].offset, 0);
                assert(b.offsets[0].source.startsWith('dest-c: low watermark'));
                // the destination reading its own topic is not seeded
                assert(!Object.keys(result.destinations).includes('dest-own'));
                // watermarks only where a processor really committed
                assert.deepStrictEqual(result.watermarks, {
                    'dest-a': { 0: 100, 1: 50 },
                    'dest-b': { 0: 80, 1: 60 },
                });
                assert.strictEqual(result.watermarksPath,
                    `${ZK_PATH}/watermarks/gen1`);
                assert.deepStrictEqual(zk.nodes[`${ZK_PATH}/watermarks/gen1`],
                    result.watermarks);
                // the document is archived for the next layout change
                assert.deepStrictEqual(zk.nodes[buildHistoryPath(ZK_PATH, 1)],
                    makeDoc(1));
                // both groups were committed, then read back
                assert.deepStrictEqual(groups.table[`${BASE_GROUP}-wg-a-gen1`],
                    { 0: 100, 1: 50 });
                assert.deepStrictEqual(groups.table[`${BASE_GROUP}-wg-b-gen1`],
                    { 0: 10, 1: 0 });
                const text = DeliverySeeder.formatResult(result);
                assert(text.includes(`${BASE_GROUP}-wg-a-gen1`));
                assert(text.includes('watermarks for 2 destination(s)'));
                done();
            });
        });

        it('should start a workgroup that owns nothing at the head', done => {
            const doc = makeDoc(1, [
                { id: 'wg-all', rule: { type: 'hashmod', modulo: 1, remainders: [0] } },
                { id: 'wg-empty', rule: { type: 'static', destinationIds: ['nobody'] } },
            ]);
            const zk = fakeZk({ [ZK_PATH]: doc });
            const groups = fakeGroups({
                [`${PROCESSOR_GROUP}-dest-a`]: { 0: 5 },
                [`${PROCESSOR_GROUP}-dest-b`]: { 0: 7 },
                [`${PROCESSOR_GROUP}-dest-c`]: { 0: 9 },
            });
            const seeder = makeSeeder({
                options: { generation: 1 },
                zkClient: zk,
                consumer: fakeConsumer([0], { 0: 0 }, { 0: 900 }),
                groupClientFactory: groups.factory,
            });
            seeder.seedFromProcessors((err, result) => {
                assert.ifError(err);
                const empty = result.groups.find(g => g.workgroupId === 'wg-empty');
                assert.deepStrictEqual(empty.destinations, []);
                assert.strictEqual(empty.offsets[0].offset, 900);
                const all = result.groups.find(g => g.workgroupId === 'wg-all');
                assert.strictEqual(all.offsets[0].offset, 5);
                done();
            });
        });

        it('should refuse a generation that is not the one in zookeeper unless forced', done => {
            const zk = fakeZk({ [ZK_PATH]: makeDoc(2) });
            const groups = fakeGroups({});
            const seeder = makeSeeder({
                options: { generation: 1 },
                zkClient: zk,
                consumer: fakeConsumer([0], { 0: 0 }, { 0: 1 }),
                groupClientFactory: groups.factory,
            });
            seeder.seedFromProcessors(err => {
                assert(err, 'expected an error');
                assert(/generation 2, not 1/.test(err.description));
                assert.strictEqual(groups.commits.length, 0);
                const forced = makeSeeder({
                    options: { generation: 1, force: true },
                    zkClient: zk,
                    consumer: fakeConsumer([0], { 0: 0 }, { 0: 1 }),
                    groupClientFactory: groups.factory,
                });
                forced.seedFromProcessors((forcedErr, result) => {
                    assert.ifError(forcedErr);
                    assert.strictEqual(result.generation, 1);
                    done();
                });
            });
        });

        it('should fail when there is no document to name the groups', done => {
            const seeder = makeSeeder({
                options: { generation: 1 },
                zkClient: fakeZk({}),
                consumer: fakeConsumer([0], { 0: 0 }, { 0: 1 }),
                groupClientFactory: fakeGroups({}).factory,
            });
            seeder.seedFromProcessors(err => {
                assert(err);
                assert(/no workgroups document/.test(err.description));
                done();
            });
        });

        it('should name the group that still has members', done => {
            const zk = fakeZk({ [ZK_PATH]: makeDoc(1) });
            const groups = fakeGroups({}, {
                throwOnCommit: {
                    [`${BASE_GROUP}-wg-a-gen1`]: CODES.ERRORS.ERR_ILLEGAL_GENERATION,
                },
            });
            const seeder = makeSeeder({
                options: { generation: 1 },
                zkClient: zk,
                consumer: fakeConsumer([0], { 0: 0 }, { 0: 1 }),
                groupClientFactory: groups.factory,
            });
            seeder.seedFromProcessors(err => {
                assert(err);
                assert(/already has members/.test(err.description));
                assert(err.description.includes(`${BASE_GROUP}-wg-a-gen1`));
                done();
            });
        });

        it('should require workgroups and a processor group id', done => {
            const noWorkgroups = makeNotifConfig();
            delete noWorkgroups.deliveryPool.workgroups;
            makeSeeder({ options: { generation: 1 }, notifConfig: noWorkgroups })
                .seedFromProcessors(err => {
                    assert(err);
                    assert(/workgroups is not configured/.test(err.description));
                    const noProcessor = makeNotifConfig();
                    delete noProcessor.queueProcessor;
                    makeSeeder({ options: { generation: 1 }, notifConfig: noProcessor })
                        .seedFromProcessors(err2 => {
                            assert(err2);
                            assert(/queueProcessor.groupId/.test(err2.description));
                            done();
                        });
                });
        });
    });

    describe('seedFromGeneration', () => {
        // generation 2 regroups: wg-x takes dest-a and dest-b by name,
        // wg-y takes the rest
        const gen2 = makeDoc(2, [
            { id: 'wg-x', rule: { type: 'static', destinationIds: ['dest-a', 'dest-b'] } },
            { id: 'wg-y', rule: { type: 'hashmod', modulo: 1, remainders: [0] } },
        ]);

        it('should seed from each destination\'s previous owner when the history is known', done => {
            const zk = fakeZk({
                [ZK_PATH]: gen2,
                [buildHistoryPath(ZK_PATH, 1)]: makeDoc(1),
            });
            const groups = fakeGroups({
                [`${BASE_GROUP}-wg-a-gen1`]: { 0: 300 },
                [`${BASE_GROUP}-wg-b-gen1`]: { 0: 200 },
            });
            const seeder = makeSeeder({
                options: { from: '1', to: '2' },
                zkClient: zk,
                consumer: fakeConsumer([0], { 0: 0 }, { 0: 1000 }),
                groupClientFactory: groups.factory,
            });
            seeder.seedFromGeneration((err, result) => {
                assert.ifError(err);
                const x = result.groups.find(g => g.workgroupId === 'wg-x');
                const y = result.groups.find(g => g.workgroupId === 'wg-y');
                // dest-a came from wg-a (300), dest-b from wg-b (200)
                assert.strictEqual(x.offsets[0].offset, 200);
                assert(x.offsets[0].source.startsWith('dest-b: generation 1 group'));
                assert.strictEqual(y.offsets[0].offset, 200);
                assert.deepStrictEqual(result.watermarks, {
                    'dest-a': { 0: 300 },
                    'dest-b': { 0: 200 },
                    'dest-c': { 0: 200 },
                });
                assert.strictEqual(result.notes.length, 0);
                assert.deepStrictEqual(zk.nodes[buildHistoryPath(ZK_PATH, 2)], gen2);
                done();
            });
        });

        it('should take the lowest offset over every previous group without the history', done => {
            // the cutover tool lists the groups a document replaced
            const recorded = Object.assign({}, gen2, {
                previousGroups: [`${BASE_GROUP}-wg-a-gen1`, `${BASE_GROUP}-wg-b-gen1`],
            });
            const zk = fakeZk({ [ZK_PATH]: recorded });
            const groups = fakeGroups({
                [`${BASE_GROUP}-wg-a-gen1`]: { 0: 300 },
                [`${BASE_GROUP}-wg-b-gen1`]: { 0: 200 },
            });
            const seeder = makeSeeder({
                options: { from: 1, to: 2 },
                zkClient: zk,
                consumer: fakeConsumer([0], { 0: 0 }, { 0: 1000 }),
                groupClientFactory: groups.factory,
            });
            seeder.seedFromGeneration((err, result) => {
                assert.ifError(err);
                result.groups.forEach(g => assert.strictEqual(g.offsets[0].offset, 200));
                assert.deepStrictEqual(result.watermarks['dest-a'], { 0: 200 });
                assert.strictEqual(result.notes.length, 1);
                assert(/no document for generation 1/.test(result.notes[0]));
                done();
            });
        });

        it('should read the previous document from --previous-spec', done => {
            const fs = require('fs');
            const os = require('os');
            const path = require('path');
            const specPath = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'seed-')),
                'gen1.json');
            fs.writeFileSync(specPath, JSON.stringify(makeDoc(1)));
            const zk = fakeZk({ [ZK_PATH]: gen2 });
            const groups = fakeGroups({
                [`${BASE_GROUP}-wg-a-gen1`]: { 0: 300 },
                [`${BASE_GROUP}-wg-b-gen1`]: { 0: 200 },
            });
            const seeder = makeSeeder({
                options: { from: 1, to: 2, previousSpec: specPath },
                zkClient: zk,
                consumer: fakeConsumer([0], { 0: 0 }, { 0: 1000 }),
                groupClientFactory: groups.factory,
            });
            seeder.seedFromGeneration((err, result) => {
                assert.ifError(err);
                assert.deepStrictEqual(result.watermarks['dest-a'], { 0: 300 });
                assert.strictEqual(result.notes.length, 0);
                done();
            });
        });

        it('should validate the generation pair', done => {
            makeSeeder({ options: { from: 2, to: 2 } }).seedFromGeneration(err => {
                assert(err);
                assert(/greater than/.test(err.description));
                done();
            });
        });
    });

    it('should close what it opened', done => {
        const zk = fakeZk({});
        let disconnected = 0;
        const seeder = makeSeeder({
            zkClient: zk,
            consumer: { disconnect(cb) { disconnected += 1; cb(); } },
        });
        seeder.close(() => {
            // injected clients are left to their owner
            assert.strictEqual(disconnected, 0);
            assert.strictEqual(zk.closed, 0);
            done();
        });
    });
});
