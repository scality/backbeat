const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const sinon = require('sinon');

const WorkgroupCutover = require(
    '../../../extensions/notification/deliveryWorker/WorkgroupCutover');

const TOPIC = 'bucket-notification-delivery';
const BASE_GROUP = 'backbeat-notification-delivery';
const ZK_PATH = '/notification/delivery-workgroups';

const BARRIER_KEY = '%00wg-barrier';
const BARRIER_RECORD_TYPE = 'delivery-workgroup-barrier';

// the broker code for an offset commit refused because the group has
// members, which is what a pre-seed against a running generation hits
const ERR_UNKNOWN_MEMBER_ID = 25;

/**
 * The pinned membership contract, stubbed. WorkgroupCutover is written
 * against these signatures and takes the module by injection, so this suite
 * exercises the tool without the module itself.
 */
const membership = {
    BARRIER_KEY,
    BARRIER_RECORD_TYPE,
    CONFIG_VERSION: 1,
    validateWorkgroupsDoc: doc => ({ error: null, value: doc }),
    buildGroupId: (base, workgroupId, generation) =>
        `${base}-${workgroupId}-gen${generation}`,
    buildBarrierRecord: params => JSON.stringify({
        type: BARRIER_RECORD_TYPE,
        generation: params.generation,
        partition: params.partition,
        createdAt: new Date().toISOString(),
    }),
    parseBarrierRecord: value => {
        try {
            const parsed = JSON.parse(value);
            return parsed.type === BARRIER_RECORD_TYPE ? parsed : null;
        } catch {
            return null;
        }
    },
    workgroupIdForDestination: (doc, destinationId) =>
        (destinationId === 'acme-prod-events' ? 'wg-whale' : 'wg-a'),
};

function makeLogger() {
    const logger = {
        infos: [],
        warns: [],
        errors: [],
        info: (msg, data) => logger.infos.push({ msg, data }),
        warn: (msg, data) => logger.warns.push({ msg, data }),
        error: (msg, data) => logger.errors.push({ msg, data }),
        debug: () => {},
        trace: () => {},
    };
    return logger;
}

function makeTool(params) {
    return new WorkgroupCutover(Object.assign({
        kafkaConfig: { hosts: 'localhost:9092' },
        zkConfig: { connectionString: '127.0.0.1:2181' },
        notifConfig: {
            deliveryPool: {
                enabled: true,
                topic: TOPIC,
                groupId: BASE_GROUP,
                workgroups: { zookeeperPath: ZK_PATH },
            },
            destinations: [
                { resource: 'acme-prod-events' },
                { resource: 'other-events' },
            ],
        },
        options: {},
        logger: makeLogger(),
        membership,
    }, params));
}

/**
 * Producer stub that acknowledges every record it is given at a predictable
 * offset, the way the broker's delivery report does
 *
 * @return {Object} producer stub
 */
function makeProducer() {
    const producer = {
        produced: [],
        handlers: {},
        on(event, handler) {
            producer.handlers[event] = handler;
        },
        setPollInterval: sinon.spy(),
        connect: (options, cb) => process.nextTick(() => cb(null, {})),
        disconnect: cb => process.nextTick(cb),
        produce(topic, partition, message, key, timestamp) {
            producer.produced.push({ topic, partition, message, key,
                timestamp });
            return process.nextTick(() =>
                producer.handlers['delivery-report'](null, {
                    topic,
                    partition,
                    offset: 1000 + partition,
                }));
        },
    };
    return producer;
}

/**
 * Consumer stub bound to one group id. Records what was assigned and
 * committed so a test can assert the tool seeds exactly the barriers.
 *
 * @param {Object} params - stub params
 * @param {Error} [params.commitSyncError] - error commitSync throws
 * @param {Object} [params.committed] - committed offset by partition
 * @return {Object} consumer stub
 */
function makeGroupConsumer(params) {
    const consumer = {
        assigned: null,
        committedOffsets: null,
        connect: (options, cb) => process.nextTick(() => cb(null)),
        disconnect: cb => process.nextTick(cb),
        assign(toppars) {
            consumer.assigned = toppars;
        },
        commitSync(toppars) {
            if (params.commitSyncError) {
                throw params.commitSyncError;
            }
            consumer.committedOffsets = toppars;
        },
        committed(toppars, timeout, cb) {
            return process.nextTick(() => cb(null, toppars.map(tp =>
                Object.assign({}, tp,
                    { offset: params.committed[tp.partition] }))));
        },
    };
    return consumer;
}

/**
 * Zookeeper client stub holding one document, so a test can assert what a
 * cutover committed
 *
 * @param {Object} [initialDoc] - document already in zookeeper
 * @return {Object} zookeeper client stub
 */
function makeZkClient(initialDoc) {
    const client = {
        writes: [],
        stored: initialDoc ? Buffer.from(JSON.stringify(initialDoc)) : null,
        getData(zkPath, watcher, cb) {
            return process.nextTick(() => (client.stored ?
                cb(null, client.stored) : cb({ name: 'NO_NODE' })));
        },
        setOrCreate(zkPath, data, cb) {
            client.writes.push(JSON.parse(data.toString()));
            client.stored = data;
            return process.nextTick(cb);
        },
        close: () => {},
    };
    return client;
}

function makeMetadataConsumer(partitions) {
    return {
        on: () => {},
        connect: (options, cb) => process.nextTick(() => cb(null)),
        disconnect: cb => process.nextTick(cb),
        getMetadata: (options, cb) => process.nextTick(() => cb(null, {
            topics: [{
                name: options.topic,
                partitions: partitions.map(id => ({ id })),
            }],
        })),
    };
}

/**
 * Group client factory backed by one shared offset store, so offsets a
 * pre-seed commits are what a later committed() call reads back
 *
 * @param {Object} [preset] - group id to committed offsets by partition, for
 *   groups this test never seeds
 * @return {Object} { factory, seeded }
 */
function makeGroupWorld(preset) {
    const seeded = Object.assign({}, preset);
    const factory = groupId => ({
        connect: (options, cb) => process.nextTick(() => cb(null)),
        disconnect: cb => process.nextTick(cb),
        assign: () => {},
        commitSync(toppars) {
            seeded[groupId] = seeded[groupId] || {};
            toppars.forEach(tp => {
                seeded[groupId][tp.partition] = tp.offset;
            });
        },
        committed(toppars, timeout, cb) {
            const offsets = seeded[groupId] || {};
            return process.nextTick(() => cb(null, toppars.map(tp =>
                Object.assign({}, tp, {
                    offset: offsets[tp.partition] !== undefined ?
                        offsets[tp.partition] : -1001,
                }))));
        },
    });
    return { factory, seeded };
}

describe('WorkgroupCutover', () => {
    let tmpDir;

    beforeEach(() => {
        delete process.env.KAFKA_TOPIC_PREFIX;
        tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'wg-cutover-'));
    });

    afterEach(() => {
        fs.rmSync(tmpDir, { recursive: true, force: true });
    });

    describe('buildDocument', () => {
        it('should build a document from the workgroup options', () => {
            const tool = makeTool({
                options: {
                    modulo: '4',
                    workgroup: ['wg-a:0,1', 'wg-b:2,3'],
                    static: ['wg-whale:acme-prod-events'],
                },
            });
            const { error, doc } = tool.buildDocument(null);
            assert.ifError(error);
            assert.strictEqual(doc.configVersion, 1);
            assert.strictEqual(doc.topic, TOPIC);
            assert.deepStrictEqual(doc.workgroups, [
                { id: 'wg-a',
                    rule: { type: 'hashmod', modulo: 4, remainders: [0, 1] } },
                { id: 'wg-b',
                    rule: { type: 'hashmod', modulo: 4, remainders: [2, 3] } },
                { id: 'wg-whale',
                    rule: { type: 'static',
                        destinationIds: ['acme-prod-events'] } },
            ]);
        });

        it('should build a document from a spec file', () => {
            const specPath = path.join(tmpDir, 'workgroups.json');
            const workgroups = [
                { id: 'wg-a',
                    rule: { type: 'hashmod', modulo: 1, remainders: [0] } },
            ];
            fs.writeFileSync(specPath, JSON.stringify(workgroups));
            const tool = makeTool({ options: { spec: specPath } });
            const { error, doc } = tool.buildDocument(null);
            assert.ifError(error);
            assert.deepStrictEqual(doc.workgroups, workgroups);
        });

        it('should refuse a spec file that does not hold an array', () => {
            const specPath = path.join(tmpDir, 'workgroups.json');
            fs.writeFileSync(specPath, JSON.stringify({ modulo: 4 }));
            const tool = makeTool({ options: { spec: specPath } });
            const { error } = tool.buildDocument(null);
            assert(error);
            assert(error.description.includes('workgroups array'));
        });

        it('should refuse --workgroup without --modulo', () => {
            const tool = makeTool({ options: { workgroup: ['wg-a:0'] } });
            const { error } = tool.buildDocument(null);
            assert(error);
            assert(error.description.includes('--modulo'));
        });

        it('should start at generation 1 without a current document', () => {
            const tool = makeTool({
                options: { modulo: '1', workgroup: ['wg-a:0'] },
            });
            const { doc } = tool.buildDocument(null);
            assert.strictEqual(doc.generation, 1);
        });

        it('should follow the generation in zookeeper', () => {
            const tool = makeTool({
                options: { modulo: '1', workgroup: ['wg-a:0'] },
            });
            const { doc } = tool.buildDocument(
                { generation: 4, workgroups: [{ id: 'wg-a' }] });
            assert.strictEqual(doc.generation, 5);
        });

        it('should refuse a generation that skips ahead', () => {
            const tool = makeTool({
                options: { modulo: '1', workgroup: ['wg-a:0'], generation: '7' },
            });
            const { error } = tool.buildDocument(
                { generation: 4, workgroups: [{ id: 'wg-a' }] });
            assert(error);
            assert(error.description.includes('does not follow generation 4'));
        });

        it('should record the groups the new generation replaces', () => {
            const tool = makeTool({
                options: { modulo: '1', workgroup: ['wg-merged:0'] },
            });
            const { doc } = tool.buildDocument({
                generation: 1,
                workgroups: [{ id: 'wg-a' }, { id: 'wg-b' }],
            });
            assert.deepStrictEqual(doc.previousGroups, [
                `${BASE_GROUP}-wg-a-gen1`,
                `${BASE_GROUP}-wg-b-gen1`,
            ]);
        });

        it('should record the single pool as what a first cutover replaces',
        () => {
            const tool = makeTool({
                options: { modulo: '1', workgroup: ['wg-a:0'] },
            });
            const { doc } = tool.buildDocument(null);
            assert.deepStrictEqual(doc.previousGroups, [BASE_GROUP]);
        });

        it('should allow a generation that skips ahead with --force', () => {
            const tool = makeTool({
                options: {
                    modulo: '1',
                    workgroup: ['wg-a:0'],
                    generation: '7',
                    force: true,
                },
            });
            const { error, doc } = tool.buildDocument(
                { generation: 4, workgroups: [{ id: 'wg-a' }] });
            assert.ifError(error);
            assert.strictEqual(doc.generation, 7);
        });
    });

    describe('group id derivation', () => {
        const currentDoc = {
            generation: 4,
            workgroups: [{ id: 'wg-a' }, { id: 'wg-b' }],
        };

        it('should derive the previous groups from the current document',
        () => {
            const tool = makeTool({});
            assert.deepStrictEqual(tool.previousGroupIds(currentDoc), [
                `${BASE_GROUP}-wg-a-gen4`,
                `${BASE_GROUP}-wg-b-gen4`,
            ]);
        });

        it('should fall back to the single pool with no current document',
        () => {
            const tool = makeTool({});
            assert.deepStrictEqual(tool.previousGroupIds(null), [BASE_GROUP]);
        });

        it('should honour --from-group over the derived set', () => {
            const tool = makeTool({ options: { fromGroup: ['legacy-group'] } });
            assert.deepStrictEqual(tool.previousGroupIds(currentDoc),
                ['legacy-group']);
        });

        it('should prefer the groups the document records having replaced',
        () => {
            const tool = makeTool({});
            assert.deepStrictEqual(tool.previousGroupIdsOfRunning({
                generation: 2,
                workgroups: [{ id: 'wg-merged' }],
                previousGroups: [`${BASE_GROUP}-wg-a-gen1`,
                    `${BASE_GROUP}-wg-b-gen1`],
            }), [`${BASE_GROUP}-wg-a-gen1`, `${BASE_GROUP}-wg-b-gen1`]);
        });

        it('should step one generation back when the document records ' +
        'nothing', () => {
            const tool = makeTool({});
            assert.deepStrictEqual(tool.previousGroupIdsOfRunning({
                generation: 5,
                workgroups: [{ id: 'wg-a' }],
            }), [`${BASE_GROUP}-wg-a-gen4`]);
        });

        it('should verify against the single pool at generation 1', () => {
            const tool = makeTool({});
            assert.deepStrictEqual(tool.previousGroupIdsOfRunning({
                generation: 1,
                workgroups: [{ id: 'wg-a' }],
            }), [BASE_GROUP]);
        });

        it('should let --from-group override the recorded groups', () => {
            const tool = makeTool({ options: { fromGroup: ['legacy-group'] } });
            assert.deepStrictEqual(tool.previousGroupIdsOfRunning({
                generation: 2,
                workgroups: [{ id: 'wg-merged' }],
                previousGroups: [`${BASE_GROUP}-wg-a-gen1`],
            }), ['legacy-group']);
        });
    });

    describe('buildDrainReport', () => {
        it('should report what each group has left to drain', () => {
            const report = WorkgroupCutover.buildDrainReport({
                barriers: { 0: 154023, 1: 154990 },
                committedByGroup: {
                    [BASE_GROUP]: { 0: 154023, 1: 154771 },
                },
            });
            assert.deepStrictEqual(report.rows, [
                { groupId: BASE_GROUP, partition: 0, barrier: 154023,
                    committed: 154023, remaining: 0, overshoot: 0 },
                { groupId: BASE_GROUP, partition: 1, barrier: 154990,
                    committed: 154771, remaining: 219, overshoot: 0 },
            ]);
            assert.strictEqual(report.drained, false);
        });

        it('should clamp remaining at 0 when the group is past the barrier',
        () => {
            const report = WorkgroupCutover.buildDrainReport({
                barriers: { 0: 100 },
                committedByGroup: { g: { 0: 250 } },
            });
            assert.strictEqual(report.rows[0].remaining, 0);
            assert.strictEqual(report.drained, true);
        });

        it('should count everything as remaining when a group never ' +
        'committed', () => {
            const report = WorkgroupCutover.buildDrainReport({
                barriers: { 0: 100 },
                committedByGroup: { g: { 0: -1001 } },
            });
            assert.strictEqual(report.rows[0].committed, -1001);
            assert.strictEqual(report.rows[0].remaining, 100);
            assert.strictEqual(report.drained, false);
        });

        it('should count no overshoot for a group stopped exactly at its ' +
        'barrier', () => {
            const report = WorkgroupCutover.buildDrainReport({
                barriers: { 0: 100, 1: 200 },
                committedByGroup: { g: { 0: 100, 1: 200 } },
            });
            assert.deepStrictEqual(report.rows.map(row => row.overshoot),
                [0, 0]);
            assert.strictEqual(report.overshootByGroup.g, 0);
            assert.strictEqual(report.overshoot, 0);
        });

        it('should count the records consumed past the barrier as ' +
        'duplicates', () => {
            const report = WorkgroupCutover.buildDrainReport({
                barriers: { 0: 100, 1: 200 },
                committedByGroup: { g: { 0: 118, 1: 203 } },
            });
            assert.deepStrictEqual(report.rows.map(row => row.overshoot),
                [18, 3]);
            assert.strictEqual(report.overshootByGroup.g, 21);
            assert.strictEqual(report.overshoot, 21);
            // the exit code keys off remaining alone, so a group that ran on
            // past its barriers still counts as drained
            assert.strictEqual(report.drained, true);
        });

        it('should count no overshoot for a group that never committed',
        () => {
            const report = WorkgroupCutover.buildDrainReport({
                barriers: { 0: 100 },
                committedByGroup: { g: { 0: -1001 } },
            });
            assert.strictEqual(report.rows[0].overshoot, 0);
            assert.strictEqual(report.overshootByGroup.g, 0);
        });

        it('should total the overshoot of each group separately', () => {
            const report = WorkgroupCutover.buildDrainReport({
                barriers: { 0: 100 },
                committedByGroup: { 'g-a': { 0: 110 }, 'g-b': { 0: 100 } },
            });
            assert.deepStrictEqual(report.overshootByGroup,
                { 'g-a': 10, 'g-b': 0 });
            assert.strictEqual(report.overshoot, 10);
        });

        it('should render the report as a table with the overshoot totals',
        () => {
            const report = WorkgroupCutover.buildDrainReport({
                barriers: { 0: 100, 1: 100 },
                committedByGroup: { g: { 0: 90, 1: 130 } },
            });
            const rendered = WorkgroupCutover.formatDrainReport(report);
            const lines = rendered.split('\n');
            const cells = line => line.trim().split(/\s+/);
            assert.deepStrictEqual(cells(lines[0]), ['group', 'partition',
                'barrier', 'committed', 'remaining', 'overshoot']);
            assert.deepStrictEqual(cells(lines[1]),
                ['g', '0', '100', '90', '10', '0']);
            assert.deepStrictEqual(cells(lines[2]),
                ['g', '1', '100', '130', '0', '30']);
            assert(rendered.includes('delivers a second time'));
            assert.deepStrictEqual(cells(lines[lines.length - 1]), ['g', '30']);
        });
    });

    describe('barriers', () => {
        it('should produce one barrier per partition, keyed for every ' +
        'worker to skip', done => {
            const producer = makeProducer();
            const tool = makeTool({ producer });
            tool._produceBarriers([0, 1], 5, (err, barriers) => {
                assert.ifError(err);
                assert.deepStrictEqual(barriers, { 0: 1000, 1: 1001 });
                assert.strictEqual(producer.produced.length, 2);
                producer.produced.forEach(record => {
                    assert.strictEqual(record.topic, TOPIC);
                    assert.strictEqual(record.key, BARRIER_KEY);
                    const parsed = membership
                        .parseBarrierRecord(record.message.toString());
                    assert(parsed);
                    assert.strictEqual(parsed.generation, 5);
                    assert.strictEqual(parsed.partition, record.partition);
                });
                done();
            });
        });

        it('should fail when a barrier is acknowledged without an offset',
        done => {
            const producer = makeProducer();
            producer.produce = (topic, partition) =>
                process.nextTick(() =>
                    producer.handlers['delivery-report'](null, {
                        topic, partition, offset: -1,
                    }));
            const tool = makeTool({ producer });
            tool._produceBarriers([0], 5, err => {
                assert(err);
                assert(err.description.includes('offset -1'));
                done();
            });
        });
    });

    describe('pre-seed', () => {
        const barriers = { 0: 154023, 1: 154990 };

        it('should build one toppar per partition at its barrier offset',
        () => {
            const tool = makeTool({});
            assert.deepStrictEqual(tool.buildPreseedToppars(barriers), [
                { topic: TOPIC, partition: 0, offset: 154023 },
                { topic: TOPIC, partition: 1, offset: 154990 },
            ]);
        });

        it('should commit the barrier offsets into every new group', done => {
            const consumers = {};
            const tool = makeTool({
                groupClientFactory: groupId => {
                    consumers[groupId] = makeGroupConsumer({ committed: {} });
                    return consumers[groupId];
                },
            });
            const doc = {
                generation: 2,
                barriers,
                workgroups: [{ id: 'wg-a' }, { id: 'wg-b' }],
            };
            tool._seedGroups(doc, (err, groupIds) => {
                assert.ifError(err);
                assert.deepStrictEqual(groupIds, [
                    `${BASE_GROUP}-wg-a-gen2`,
                    `${BASE_GROUP}-wg-b-gen2`,
                ]);
                groupIds.forEach(groupId => {
                    assert.deepStrictEqual(
                        consumers[groupId].committedOffsets,
                        tool.buildPreseedToppars(barriers));
                    assert.deepStrictEqual(consumers[groupId].assigned, [
                        { topic: TOPIC, partition: 0 },
                        { topic: TOPIC, partition: 1 },
                    ]);
                });
                done();
            });
        });

        it('should turn a commitSync throw into a callback error', done => {
            const commitSyncError = new Error('Local: Erroneous state');
            const tool = makeTool({
                groupClientFactory: () => makeGroupConsumer({
                    commitSyncError,
                    committed: {},
                }),
            });
            tool._seedGroup('a-group', tool.buildPreseedToppars(barriers),
                err => {
                    assert(err);
                    assert(err.description.includes('a-group'));
                    assert(err.description.includes('Erroneous state'));
                    done();
                });
        });

        it('should name the running workers when the group has members',
        done => {
            const commitSyncError = new Error('Broker: Unknown member');
            commitSyncError.code = ERR_UNKNOWN_MEMBER_ID;
            const tool = makeTool({
                groupClientFactory: () => makeGroupConsumer({
                    commitSyncError,
                    committed: {},
                }),
            });
            tool._seedGroup('a-group', tool.buildPreseedToppars(barriers),
                err => {
                    assert(err);
                    assert(err.description.includes('already has members'));
                    done();
                });
        });

        it('should fail when a group is not seeded at its barriers', done => {
            const tool = makeTool({
                groupClientFactory: () => makeGroupConsumer({
                    committed: { 0: 154023, 1: 1 },
                }),
            });
            tool._verifySeeded({
                generation: 2,
                barriers,
                workgroups: [{ id: 'wg-a' }],
            }, err => {
                assert(err);
                assert(err.description.includes('partitions 1'));
                done();
            });
        });
    });

    describe('cutover', () => {
        it('should commit a document carrying the groups it replaces',
        done => {
            const zkClient = makeZkClient({
                configVersion: 1,
                generation: 1,
                topic: TOPIC,
                workgroups: [{ id: 'wg-a' }, { id: 'wg-b' }],
                barriers: { 0: 5, 1: 5 },
            });
            const tool = makeTool({
                options: { modulo: '1', workgroup: ['wg-merged:0'] },
                zkClient,
                consumer: makeMetadataConsumer([0, 1]),
                producer: makeProducer(),
                groupClientFactory: makeGroupWorld({}).factory,
            });
            tool.cutover((err, result) => {
                assert.ifError(err);
                assert.strictEqual(zkClient.writes.length, 1);
                const written = zkClient.writes[0];
                assert.strictEqual(written.generation, 2);
                assert.deepStrictEqual(written.barriers, { 0: 1000, 1: 1001 });
                assert.deepStrictEqual(written.previousGroups, [
                    `${BASE_GROUP}-wg-a-gen1`,
                    `${BASE_GROUP}-wg-b-gen1`,
                ]);
                assert.deepStrictEqual(result.groupIds,
                    [`${BASE_GROUP}-wg-merged-gen2`]);
                done();
            });
        });
    });

    describe('verify', () => {
        it('should report on the groups the document records having ' +
        'replaced', done => {
            const previousGroups = [
                `${BASE_GROUP}-wg-a-gen1`,
                `${BASE_GROUP}-wg-b-gen1`,
            ];
            const tool = makeTool({
                zkClient: makeZkClient({
                    configVersion: 1,
                    generation: 2,
                    topic: TOPIC,
                    workgroups: [{ id: 'wg-merged' }],
                    barriers: { 0: 100 },
                    previousGroups,
                }),
                groupClientFactory: makeGroupWorld({
                    [previousGroups[0]]: { 0: 100 },
                    [previousGroups[1]]: { 0: 40 },
                }).factory,
            });
            tool.verify((err, report) => {
                assert.ifError(err);
                assert.deepStrictEqual(report.rows.map(row => row.groupId),
                    previousGroups);
                assert.deepStrictEqual(report.rows.map(row => row.remaining),
                    [0, 60]);
                assert.strictEqual(report.drained, false);
                done();
            });
        });

        it('should fall back to derivation for a document that records ' +
        'nothing', done => {
            const logger = makeLogger();
            const derived = `${BASE_GROUP}-wg-merged-gen1`;
            const tool = makeTool({
                logger,
                zkClient: makeZkClient({
                    configVersion: 1,
                    generation: 2,
                    topic: TOPIC,
                    workgroups: [{ id: 'wg-merged' }],
                    barriers: { 0: 100 },
                }),
                groupClientFactory: makeGroupWorld({
                    [derived]: { 0: 100 },
                }).factory,
            });
            tool.verify((err, report) => {
                assert.ifError(err);
                assert.deepStrictEqual(report.rows.map(row => row.groupId),
                    [derived]);
                assert.strictEqual(report.drained, true);
                assert(logger.warns.some(entry =>
                    entry.msg.includes('does not record the groups it ' +
                        'replaced')));
                done();
            });
        });
    });

    describe('configuration', () => {
        it('should refuse to run without a workgroups block', done => {
            const tool = makeTool({
                notifConfig: {
                    deliveryPool: {
                        enabled: true,
                        topic: TOPIC,
                        groupId: BASE_GROUP,
                    },
                },
            });
            tool.show(err => {
                assert(err);
                assert(err.description.includes('workgroups'));
                done();
            });
        });

        it('should refuse to run with the delivery pool disabled', done => {
            const tool = makeTool({
                notifConfig: { deliveryPool: { enabled: false } },
            });
            tool.verify(err => {
                assert(err);
                assert(err.description.includes('enabled'));
                done();
            });
        });
    });
});
