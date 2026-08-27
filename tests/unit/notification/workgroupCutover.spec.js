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
            const { doc } = tool.buildDocument({ generation: 4 });
            assert.strictEqual(doc.generation, 5);
        });

        it('should refuse a generation that skips ahead', () => {
            const tool = makeTool({
                options: { modulo: '1', workgroup: ['wg-a:0'], generation: '7' },
            });
            const { error } = tool.buildDocument({ generation: 4 });
            assert(error);
            assert(error.description.includes('does not follow generation 4'));
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
            const { error, doc } = tool.buildDocument({ generation: 4 });
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

        it('should step one generation back for a verify run', () => {
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
                    committed: 154023, remaining: 0 },
                { groupId: BASE_GROUP, partition: 1, barrier: 154990,
                    committed: 154771, remaining: 219 },
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

        it('should render the report as a table', () => {
            const report = WorkgroupCutover.buildDrainReport({
                barriers: { 0: 100 },
                committedByGroup: { g: { 0: 90 } },
            });
            const lines = WorkgroupCutover.formatDrainReport(report)
                .split('\n');
            assert.strictEqual(lines.length, 2);
            assert(lines[0].startsWith('group'));
            assert(lines[1].startsWith('g '));
            assert(lines[1].endsWith('10'));
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
