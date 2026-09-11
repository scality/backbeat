const assert = require('assert');

const FakeLogger = require('../../utils/fakeLogger');
const SelfSeeder = require(
    '../../../extensions/notification/deliveryWorker/SelfSeeder');
const { buildLockPath, previousGenerationOf } = SelfSeeder;

const TOPIC = 'backbeat-bucket-notification';
const ZK_PATH = '/notification/delivery-workgroups';
const BASE_GROUP = 'delivery-group';
const PROCESSOR_GROUP = 'notification-group';

const kafkaConfig = { hosts: 'kafka:9092' };
const zkConfig = { connectionString: 'zookeeper:2181' };

function makeNotifConfig(deliveryPoolOverrides) {
    return {
        topic: TOPIC,
        queueProcessor: { groupId: PROCESSOR_GROUP },
        destinations: [
            { resource: 'dest-a', type: 'kafka', host: 'h', port: 9092,
                topic: 'a' },
        ],
        deliveryPool: Object.assign({
            enabled: true,
            source: 'internal',
            topic: 'delivery-topic',
            groupId: BASE_GROUP,
            workgroups: { zookeeperPath: ZK_PATH },
        }, deliveryPoolOverrides || {}),
    };
}

function makeDoc(generation, previousGroups) {
    const doc = {
        configVersion: 1,
        generation,
        topic: TOPIC,
        workgroups: [
            { id: 'wg-a', rule: { type: 'hashmod', modulo: 1, remainders: [0] } },
        ],
    };
    if (previousGroups) {
        doc.previousGroups = previousGroups;
    }
    return doc;
}

/**
 * A zookeeper client whose ephemeral nodes are entries in a map shared by
 * every client built from the same store, which is how two workers race for
 * one lock in these tests.
 *
 * @param {Object} store - shared node map, path to value
 * @return {Object} client stub
 */
function fakeZk(store) {
    const nodes = store;
    const client = {
        nodes,
        mkdirp(path, cb) {
            nodes[path] = nodes[path] || null;
            return process.nextTick(cb);
        },
        create(path, data, acls, mode, cb) {
            if (path in nodes && nodes[path] !== null) {
                const err = new Error('node exists');
                err.name = 'NODE_EXISTS';
                return process.nextTick(() => cb(err));
            }
            nodes[path] = data;
            return process.nextTick(() => cb(null, path));
        },
        remove(path, version, cb) {
            if (!(path in nodes)) {
                const err = new Error('no node');
                err.name = 'NO_NODE';
                return process.nextTick(() => cb(err));
            }
            delete nodes[path];
            return process.nextTick(cb);
        },
    };
    return client;
}

/**
 * A committed offsets table shared between the state reader and the fake
 * seeder, so a seed a test runs is visible to the next read
 *
 * @param {Object} table - groupId to { partition: offset }
 * @param {Number[]} partitions - partition ids of the topic
 * @return {Function} readState(params, cb)
 */
function makeReadState(table, partitions) {
    const reads = [];
    const readState = (params, cb) => {
        reads.push(params.groupId);
        const offsets = Object.assign({}, table[params.groupId]);
        const unseeded = partitions.filter(p => typeof offsets[p] !== 'number');
        return process.nextTick(() => cb(null, {
            topic: params.topic,
            partitions,
            offsets,
            unseeded,
        }));
    };
    readState.reads = reads;
    return readState;
}

/**
 * A DeliverySeeder stand-in: records the options it was built with and
 * commits the offsets a test wants into the shared table
 *
 * @param {Object} params - { table, offsets, error, calls, onSeed }
 * @return {Function} seederFactory(options)
 */
function makeSeederFactory(params) {
    const table = params.table;
    const closes = [];
    const factory = options => ({
        seedFromProcessors(cb) {
            return this._run('seedFromProcessors', options, cb);
        },
        seedFromGeneration(cb) {
            return this._run('seedFromGeneration', options, cb);
        },
        _run(method, opts, cb) {
            params.calls.push({ method, options: opts });
            if (params.onSeed) {
                params.onSeed();
            }
            if (params.error) {
                return process.nextTick(() => cb(params.error));
            }
            Object.keys(params.offsets).forEach(groupId => {
                table[groupId] = Object.assign({}, params.offsets[groupId]);
            });
            return process.nextTick(() => cb(null, {
                generation: opts.generation || opts.to,
                groups: Object.keys(params.offsets).map(groupId => ({ groupId })),
                watermarksPath: `${ZK_PATH}/watermarks/gen${opts.generation ||
                    opts.to}`,
            }));
        },
        close(cb) {
            closes.push(options);
            return process.nextTick(cb);
        },
    });
    factory.closes = closes;
    return factory;
}

function makeSelfSeeder(overrides) {
    return new SelfSeeder(Object.assign({
        kafkaConfig,
        zkConfig,
        notifConfig: makeNotifConfig(),
        doc: makeDoc(1),
        groupId: `${BASE_GROUP}-wg-a-gen1`,
        workgroupId: 'wg-a',
        logger: FakeLogger,
        pollMs: 5,
    }, overrides));
}

describe('notification SelfSeeder', () => {
    describe('isEnabled', () => {
        it('should be on by default for the internal source with workgroups',
            () => {
                assert.strictEqual(
                    SelfSeeder.isEnabled(makeNotifConfig()), true);
            });

        it('should be off when seedOnStart is false', () => {
            assert.strictEqual(SelfSeeder.isEnabled(
                makeNotifConfig({ seedOnStart: false })), false);
        });

        it('should be off on the delivery source', () => {
            assert.strictEqual(SelfSeeder.isEnabled(
                makeNotifConfig({ source: 'delivery' })), false);
        });

        it('should be off without a workgroups document', () => {
            const notifConfig = makeNotifConfig();
            delete notifConfig.deliveryPool.workgroups;
            assert.strictEqual(SelfSeeder.isEnabled(notifConfig), false);
        });

        it('should be off when the pool is not enabled', () => {
            assert.strictEqual(SelfSeeder.isEnabled(
                makeNotifConfig({ enabled: false })), false);
        });
    });

    describe('previousGenerationOf', () => {
        it('should read the generation the document says it replaced', () => {
            assert.strictEqual(previousGenerationOf(makeDoc(5,
                [`${BASE_GROUP}-wg-a-gen3`, `${BASE_GROUP}-wg-b-gen3`])), 3);
        });

        it('should fall back to the generation before this one', () => {
            assert.strictEqual(previousGenerationOf(makeDoc(4)), 3);
        });
    });

    describe('seed', () => {
        it('should seed the generation itself when its group is empty', done => {
            const table = {};
            const seedParams = {
                table,
                calls: [],
                offsets: { [`${BASE_GROUP}-wg-a-gen1`]: { 0: 100, 1: 50 } },
            };
            const store = {};
            const seederFactory = makeSeederFactory(seedParams);
            const seeder = makeSelfSeeder({
                zkClient: fakeZk(store),
                readState: makeReadState(table, [0, 1]),
                seederFactory,
            });
            seeder.seed((err, outcome) => {
                assert.ifError(err);
                assert.strictEqual(outcome.seeded, true);
                assert.strictEqual(outcome.reason, 'seeded');
                assert.strictEqual(seedParams.calls.length, 1);
                assert.strictEqual(seedParams.calls[0].method,
                    'seedFromProcessors');
                assert.deepStrictEqual(seedParams.calls[0].options,
                    { generation: 1 });
                assert.strictEqual(seederFactory.closes.length, 1);
                // the lock is released, so the next worker never waits on it
                assert(!(buildLockPath(ZK_PATH, 1) in store));
                done();
            });
        });

        it('should seed a later generation from the previous one', done => {
            const table = {};
            const seedParams = {
                table,
                calls: [],
                offsets: { [`${BASE_GROUP}-wg-a-gen3`]: { 0: 900 } },
            };
            const seeder = makeSelfSeeder({
                doc: makeDoc(3, [`${BASE_GROUP}-wg-a-gen2`]),
                groupId: `${BASE_GROUP}-wg-a-gen3`,
                zkClient: fakeZk({}),
                readState: makeReadState(table, [0]),
                seederFactory: makeSeederFactory(seedParams),
            });
            seeder.seed((err, outcome) => {
                assert.ifError(err);
                assert.strictEqual(outcome.seeded, true);
                assert.strictEqual(seedParams.calls[0].method,
                    'seedFromGeneration');
                assert.deepStrictEqual(seedParams.calls[0].options,
                    { from: 2, to: 3 });
                done();
            });
        });

        it('should leave a group that already has offsets untouched', done => {
            const table = { [`${BASE_GROUP}-wg-a-gen1`]: { 0: 10, 1: 20 } };
            const seedParams = { table, calls: [], offsets: {} };
            const store = {};
            const seeder = makeSelfSeeder({
                zkClient: fakeZk(store),
                readState: makeReadState(table, [0, 1]),
                seederFactory: makeSeederFactory(seedParams),
            });
            seeder.seed((err, outcome) => {
                assert.ifError(err);
                assert.strictEqual(outcome.seeded, false);
                assert.strictEqual(outcome.reason, 'already-seeded');
                assert.strictEqual(seedParams.calls.length, 0);
                // no lock was ever taken
                assert.deepStrictEqual(store, {});
                done();
            });
        });

        it('should have the second worker wait and find the offsets', done => {
            // both workers see an empty group; the first takes the lock and
            // seeds, the second waits on the lock and then reads what the
            // first committed
            const table = {};
            const store = {};
            const partitions = [0, 1];
            const readState = makeReadState(table, partitions);
            const firstSeeded = {
                [`${BASE_GROUP}-wg-a-gen1`]: { 0: 100, 1: 50 },
                [`${BASE_GROUP}-wg-b-gen1`]: { 0: 80, 1: 40 },
            };
            const calls = [];
            let release = null;
            const first = makeSelfSeeder({
                zkClient: fakeZk(store),
                readState,
                seederFactory: makeSeederFactory({
                    table, calls, offsets: firstSeeded,
                    // hold the lock until the second worker has been told to
                    // wait at least once
                    onSeed: () => { release = true; },
                }),
            });
            const second = makeSelfSeeder({
                groupId: `${BASE_GROUP}-wg-b-gen1`,
                workgroupId: 'wg-b',
                zkClient: fakeZk(store),
                readState,
                seederFactory: makeSeederFactory({ table, calls, offsets: {} }),
            });
            let firstOutcome = null;
            let secondOutcome = null;
            const finish = () => {
                if (!firstOutcome || !secondOutcome) {
                    return undefined;
                }
                assert.strictEqual(firstOutcome.seeded, true);
                // only one worker ever ran the seeding
                assert.strictEqual(calls.length, 1);
                assert.strictEqual(secondOutcome.seeded, false);
                assert.strictEqual(secondOutcome.reason, 'already-seeded');
                assert.deepStrictEqual(table[`${BASE_GROUP}-wg-b-gen1`],
                    { 0: 80, 1: 40 });
                assert(release, 'the first worker never seeded');
                return done();
            };
            first.seed((err, outcome) => {
                assert.ifError(err);
                firstOutcome = outcome;
                return finish();
            });
            second.seed((err, outcome) => {
                assert.ifError(err);
                secondOutcome = outcome;
                return finish();
            });
        });

        it('should leave the group unseeded when the seeding fails', done => {
            const table = {};
            const store = {};
            const seedParams = {
                table,
                calls: [],
                offsets: {},
                error: new Error('the coordinator is not available'),
            };
            const readState = makeReadState(table, [0, 1]);
            const seeder = makeSelfSeeder({
                zkClient: fakeZk(store),
                readState,
                seederFactory: makeSeederFactory(seedParams),
            });
            seeder.seed((err, outcome) => {
                assert.ifError(err);
                assert.strictEqual(outcome.seeded, false);
                assert.strictEqual(outcome.reason, 'seed-failed');
                assert.strictEqual(outcome.error.message,
                    'the coordinator is not available');
                // the group is still empty, which is what makes
                // assertSeededOffsets refuse the start
                assert.deepStrictEqual(table, {});
                // and the lock is released, so a retry is not blocked by it
                assert(!(buildLockPath(ZK_PATH, 1) in store));
                done();
            });
        });

        it('should give up waiting when the holder never publishes', done => {
            const table = {};
            const store = {};
            // a lock left behind by a worker that is no longer seeding
            store[buildLockPath(ZK_PATH, 1)] = Buffer.from('{}');
            const seedParams = { table, calls: [], offsets: {} };
            const seeder = makeSelfSeeder({
                notifConfig: makeNotifConfig({ seedOnStartTimeoutMs: 20 }),
                zkClient: fakeZk(store),
                readState: makeReadState(table, [0]),
                seederFactory: makeSeederFactory(seedParams),
            });
            seeder.seed((err, outcome) => {
                assert.ifError(err);
                assert.strictEqual(outcome.seeded, false);
                assert.strictEqual(outcome.reason, 'wait-timeout');
                assert.strictEqual(seedParams.calls.length, 0);
                assert.deepStrictEqual(table, {});
                done();
            });
        });

        it('should not seed when the committed offsets cannot be read', done => {
            const seedParams = { table: {}, calls: [], offsets: {} };
            const store = {};
            const seeder = makeSelfSeeder({
                zkClient: fakeZk(store),
                readState: (params, cb) => process.nextTick(() =>
                    cb(new Error('broker transport failure'))),
                seederFactory: makeSeederFactory(seedParams),
            });
            seeder.seed((err, outcome) => {
                assert.ifError(err);
                assert.strictEqual(outcome.seeded, false);
                assert.strictEqual(outcome.reason, 'unreadable');
                assert.strictEqual(seedParams.calls.length, 0);
                assert.deepStrictEqual(store, {});
                done();
            });
        });
    });
});
