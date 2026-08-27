const assert = require('assert');

const {
    BARRIER_KEY,
    BARRIER_RECORD_TYPE,
    SKIP_BARRIER,
    SKIP_NOT_IN_SLICE,
    CONFIG_VERSION,
    WORKGROUP_ID_PATTERN,
    validateWorkgroupsDoc,
    buildOwnershipIndex,
    ownerOfToken,
    workgroupIdForDestination,
    encodeDestinationToken,
    destinationTokenFromKey,
    isBarrierKey,
    buildBarrierRecord,
    parseBarrierRecord,
    createSliceFilter,
    buildGroupId,
} = require('../../../extensions/notification/utils/workgroups');
const { buildDeliveryKey } = require('../../../extensions/notification/utils/deliveryKey');

// mirrors OWNER_CACHE_MAX in the module, which is deliberately not exported:
// the module's exported surface is the interface between two agents
const OWNER_CACHE_MAX = 10000;

const hashmod = (modulo, remainders) => ({ type: 'hashmod', modulo, remainders });
const staticRule = destinationIds => ({ type: 'static', destinationIds });

/**
 * Build a workgroups document around a workgroups array
 * @param {object[]} workgroups - workgroup entries
 * @param {object} [overrides] - fields to override on the document
 * @return {object} workgroups document
 */
function makeDoc(workgroups, overrides) {
    return {
        configVersion: CONFIG_VERSION,
        generation: 3,
        topic: 'bucket-notification-delivery',
        updatedAt: '2026-08-27T11:04:18.221Z',
        workgroups,
        ...overrides,
    };
}

const sliceDoc = makeDoc([
    { id: 'wg-bulk-a', rule: hashmod(8, [0, 1, 2]) },
    { id: 'wg-bulk-b', rule: hashmod(8, [3, 4, 5, 6, 7]) },
    { id: 'wg-whale', rule: staticRule(['acme-prod-events']) },
]);

const sliceIds = sliceDoc.workgroups.map(wg => wg.id);

/**
 * Build one slice filter per workgroup of a document
 * @param {object} doc - workgroups document
 * @return {object[]} one { workgroupId, classify } per workgroup
 */
function filtersOf(doc) {
    return doc.workgroups.map(wg =>
        createSliceFilter({ doc, workgroupId: wg.id }));
}

/**
 * Count how many workgroups of a document accept a record key
 * @param {object[]} filters - slice filters
 * @param {string} key - record key
 * @return {string[]} ids of the workgroups that own the key
 */
function ownersAccepting(filters, key) {
    return filters.filter(filter => filter.classify(key) === null)
        .map(filter => filter.workgroupId);
}

describe('notification workgroups membership ::', () => {
    describe('validateWorkgroupsDoc', () => {
        it('should accept a minimal single workgroup document', () => {
            const { error, value } = validateWorkgroupsDoc(makeDoc([
                { id: 'wg-only', rule: hashmod(1, [0]) },
            ]));
            assert.ifError(error);
            assert.strictEqual(value.workgroups.length, 1);
            assert.strictEqual(value.generation, 3);
        });

        it('should accept a document with no barriers', () => {
            const { error, value } = validateWorkgroupsDoc(makeDoc([
                { id: 'wg-only', rule: hashmod(1, [0]) },
            ]));
            assert.ifError(error);
            assert.strictEqual(value.barriers, undefined);
        });

        it('should accept a document with barriers', () => {
            const { error, value } = validateWorkgroupsDoc(makeDoc([
                { id: 'wg-only', rule: hashmod(1, [0]) },
            ], { barriers: { 0: 154023, 1: 0 } }));
            assert.ifError(error);
            assert.deepStrictEqual(value.barriers, { 0: 154023, 1: 0 });
        });

        const rejected = [
            {
                description: 'a config version other than 1',
                doc: makeDoc([{ id: 'wg-only', rule: hashmod(1, [0]) }],
                    { configVersion: 2 }),
            },
            {
                description: 'generation 0',
                doc: makeDoc([{ id: 'wg-only', rule: hashmod(1, [0]) }],
                    { generation: 0 }),
            },
            {
                description: 'a missing topic',
                doc: makeDoc([{ id: 'wg-only', rule: hashmod(1, [0]) }],
                    { topic: undefined }),
            },
            {
                description: 'an empty workgroups array',
                doc: makeDoc([]),
            },
            {
                description: 'duplicate workgroup ids',
                doc: makeDoc([
                    { id: 'wg-dup', rule: hashmod(2, [0]) },
                    { id: 'wg-dup', rule: hashmod(2, [1]) },
                ]),
            },
            {
                description: 'two hashmod rules with different moduli',
                doc: makeDoc([
                    { id: 'wg-a', rule: hashmod(2, [0]) },
                    { id: 'wg-b', rule: hashmod(4, [1, 2, 3]) },
                ]),
            },
            {
                description: 'remainders that do not cover 0..modulo-1',
                doc: makeDoc([
                    { id: 'wg-a', rule: hashmod(4, [0, 1]) },
                    { id: 'wg-b', rule: hashmod(4, [2]) },
                ]),
            },
            {
                description: 'a remainder claimed by two workgroups',
                doc: makeDoc([
                    { id: 'wg-a', rule: hashmod(2, [0, 1]) },
                    { id: 'wg-b', rule: hashmod(2, [1]) },
                ]),
            },
            {
                description: 'a remainder at or above the modulo',
                doc: makeDoc([
                    { id: 'wg-a', rule: hashmod(2, [0, 1]) },
                    { id: 'wg-b', rule: hashmod(2, [2]) },
                ]),
            },
            {
                description: 'a negative remainder',
                doc: makeDoc([{ id: 'wg-a', rule: hashmod(1, [-1]) }]),
            },
            {
                description: 'a document with only static workgroups',
                doc: makeDoc([
                    { id: 'wg-whale', rule: staticRule(['acme-prod-events']) },
                ]),
            },
            {
                description: 'a static destination id containing a pipe',
                doc: makeDoc([
                    { id: 'wg-a', rule: hashmod(1, [0]) },
                    { id: 'wg-whale', rule: staticRule(['acme|events']) },
                ]),
            },
            {
                description: 'duplicate static ids across workgroups',
                doc: makeDoc([
                    { id: 'wg-a', rule: hashmod(1, [0]) },
                    { id: 'wg-w1', rule: staticRule(['acme-prod-events']) },
                    { id: 'wg-w2', rule: staticRule(['acme-prod-events']) },
                ]),
            },
            {
                description: 'a workgroup id that fails the pattern',
                doc: makeDoc([{ id: '-wg-bad', rule: hashmod(1, [0]) }]),
            },
            {
                description: 'a negative barrier offset',
                doc: makeDoc([{ id: 'wg-a', rule: hashmod(1, [0]) }],
                    { barriers: { 0: -1 } }),
            },
            {
                description: 'a barrier key that is not a partition number',
                doc: makeDoc([{ id: 'wg-a', rule: hashmod(1, [0]) }],
                    { barriers: { notAPartition: 12 } }),
            },
            {
                description: 'an unknown rule type',
                doc: makeDoc([{ id: 'wg-a', rule: { type: 'lottery' } }]),
            },
            {
                description: 'a value that is not an object',
                doc: 'not a document',
            },
        ];

        rejected.forEach(testCase =>
            it(`should reject ${testCase.description}`, () => {
                const { error } = validateWorkgroupsDoc(testCase.doc);
                assert(error instanceof Error,
                    `expected an error for ${testCase.description}`);
                assert(error.message.length > 0);
            })
        );

        it('should pin the workgroup id pattern', () => {
            ['a', 'wg-bulk-a', 'WG_1', '0'].forEach(id =>
                assert(WORKGROUP_ID_PATTERN.test(id), `rejected "${id}"`));
            ['', '-wg', '_wg', 'wg a', 'wg.a', 'a'.repeat(65)].forEach(id =>
                assert(!WORKGROUP_ID_PATTERN.test(id), `accepted "${id}"`));
        });
    });

    describe('destinationTokenFromKey', () => {
        it('should return a plain key untouched', () => {
            assert.strictEqual(destinationTokenFromKey('dest'), 'dest');
        });

        it('should cut a sub key off', () => {
            assert.strictEqual(destinationTokenFromKey('dest%7C3'), 'dest');
        });

        it('should read a Buffer key', () => {
            assert.strictEqual(
                destinationTokenFromKey(Buffer.from('dest%7C3')), 'dest');
        });

        it('should return an empty token for a missing key', () => {
            assert.strictEqual(destinationTokenFromKey(null), '');
            assert.strictEqual(destinationTokenFromKey(undefined), '');
        });

        it('should leave other percent escapes alone', () => {
            assert.strictEqual(destinationTokenFromKey('my%20dest'), 'my%20dest');
        });

        it('should return an empty token for a key that is only a sub key', () => {
            assert.strictEqual(destinationTokenFromKey('%7C0'), '');
        });
    });

    describe('isBarrierKey', () => {
        it('should recognise the barrier key as a Buffer and as a string', () => {
            assert.strictEqual(isBarrierKey(Buffer.from(BARRIER_KEY)), true);
            assert.strictEqual(isBarrierKey(BARRIER_KEY), true);
        });

        it('should not recognise a normal key', () => {
            assert.strictEqual(isBarrierKey('dest'), false);
            assert.strictEqual(isBarrierKey(Buffer.from('dest')), false);
        });

        it('should not recognise a key shorter than the prefix', () => {
            assert.strictEqual(isBarrierKey(Buffer.from('%0')), false);
            assert.strictEqual(isBarrierKey('%0'), false);
        });

        it('should not recognise a missing or empty key', () => {
            assert.strictEqual(isBarrierKey(null), false);
            assert.strictEqual(isBarrierKey(undefined), false);
            assert.strictEqual(isBarrierKey(''), false);
            assert.strictEqual(isBarrierKey(Buffer.alloc(0)), false);
        });

        it('should only look at the start of the key', () => {
            assert.strictEqual(isBarrierKey('dest%00tail'), false);
            assert.strictEqual(isBarrierKey(Buffer.from('dest%00tail')), false);
        });
    });

    describe('ownership', () => {
        it('should give every token in a large sample exactly one owner', () => {
            const filters = filtersOf(sliceDoc);
            const hit = new Set();
            for (let i = 0; i < 2000; i++) {
                const key = encodeDestinationToken(`dest-${i}`);
                const owners = ownersAccepting(filters, key);
                assert.strictEqual(owners.length, 1,
                    `"${key}" is owned by ${owners.length} workgroups`);
                hit.add(owners[0]);
            }
            // the sample must actually spread, otherwise the assertion above
            // would hold for a filter that always says yes
            assert(hit.size >= 2);
        });

        it('should give an unknown destination exactly one owner', () => {
            const owners = ownersAccepting(filtersOf(sliceDoc),
                encodeDestinationToken('never-configured-anywhere'));
            assert.strictEqual(owners.length, 1);
            assert(sliceIds.includes(owners[0]));
        });

        it('should give the empty token exactly one owner', () => {
            const owners = ownersAccepting(filtersOf(sliceDoc), '');
            assert.strictEqual(owners.length, 1);
            assert(sliceIds.includes(owners[0]));
        });

        it('should let a static rule beat the hashmod that would claim it', () => {
            const destinationId = 'acme-prod-events';
            assert.strictEqual(
                workgroupIdForDestination(sliceDoc, destinationId), 'wg-whale');

            // the same document without the static carve out: whichever
            // hashmod workgroup the destination hashes into must refuse it
            // once the static rule is back
            const hashmodDoc = makeDoc(sliceDoc.workgroups.slice(0, 2));
            const hashmodOwner =
                workgroupIdForDestination(hashmodDoc, destinationId);
            assert(['wg-bulk-a', 'wg-bulk-b'].includes(hashmodOwner));
            const filter = createSliceFilter({
                doc: sliceDoc,
                workgroupId: hashmodOwner,
            });
            assert.strictEqual(
                filter.classify(encodeDestinationToken(destinationId)),
                SKIP_NOT_IN_SLICE);
        });

        it('should agree between ownerOfToken and workgroupIdForDestination', () => {
            const index = buildOwnershipIndex(sliceDoc);
            ['dest-1', 'dest-2', 'acme-prod-events', 'my dest'].forEach(id =>
                assert.strictEqual(
                    ownerOfToken(index, encodeDestinationToken(id)),
                    workgroupIdForDestination(sliceDoc, id), `for "${id}"`));
        });
    });

    describe('delivery keys', () => {
        it('should classify a destination the way its record key does', () => {
            const destination = { resource: 'my dest', spreadFactor: 1 };
            const key = encodeURIComponent(
                buildDeliveryKey(destination, 'bucket', 'objectKey'));
            const expected =
                workgroupIdForDestination(sliceDoc, destination.resource);
            const owners = ownersAccepting(filtersOf(sliceDoc), key);
            assert.deepStrictEqual(owners, [expected]);
        });

        it('should keep every sub key of a spread destination together', () => {
            const destination = { resource: 'dest-spread', spreadFactor: 4 };
            const keys = new Set();
            for (let i = 0; i < 400; i++) {
                keys.add(encodeURIComponent(
                    buildDeliveryKey(destination, 'bucket', `objectKey-${i}`)));
            }
            assert.strictEqual(keys.size, 4,
                'the sample did not reach all four sub keys');

            const expected =
                workgroupIdForDestination(sliceDoc, destination.resource);
            const filters = filtersOf(sliceDoc);
            keys.forEach(key => assert.deepStrictEqual(
                ownersAccepting(filters, key), [expected], `for "${key}"`));
        });
    });

    describe('createSliceFilter', () => {
        it('should accept its own keys and refuse foreign ones', () => {
            const filter = createSliceFilter({
                doc: sliceDoc,
                workgroupId: 'wg-whale',
            });
            assert.strictEqual(
                filter.classify(encodeDestinationToken('acme-prod-events')), null);
            assert.strictEqual(filter.classify('dest-1'),
                workgroupIdForDestination(sliceDoc, 'dest-1') === 'wg-whale' ?
                    null : SKIP_NOT_IN_SLICE);
            assert.strictEqual(filter.workgroupId, 'wg-whale');
            assert.strictEqual(filter.generation, sliceDoc.generation);
        });

        it('should skip the barrier key even for the workgroup it hashes into', () => {
            const owner = ownerOfToken(buildOwnershipIndex(sliceDoc), BARRIER_KEY);
            const filter = createSliceFilter({
                doc: sliceDoc,
                workgroupId: owner,
            });
            assert.strictEqual(filter.classify(BARRIER_KEY), SKIP_BARRIER);
            assert.strictEqual(
                filter.classify(Buffer.from(BARRIER_KEY)), SKIP_BARRIER);
        });

        it('should keep answering correctly once the owner cache is cleared', () => {
            const filter = createSliceFilter({
                doc: sliceDoc,
                workgroupId: 'wg-whale',
            });
            const own = encodeDestinationToken('acme-prod-events');
            assert.strictEqual(filter.classify(own), null);
            for (let i = 0; i <= OWNER_CACHE_MAX; i++) {
                filter.classify(`garbage-${i}`);
            }
            assert.strictEqual(filter.classify(own), null);
            assert.strictEqual(filter.classify('dest-not-mine-at-all'),
                workgroupIdForDestination(sliceDoc, 'dest-not-mine-at-all') ===
                    'wg-whale' ? null : SKIP_NOT_IN_SLICE);
        });
    });

    describe('buildGroupId', () => {
        it('should join the base group id, the workgroup and the generation', () => {
            assert.strictEqual(
                buildGroupId('backbeat-notification-delivery', 'wg-bulk-a', 3),
                'backbeat-notification-delivery-wg-bulk-a-gen3');
        });

        it('should be stable for the same inputs', () => {
            assert.strictEqual(buildGroupId('base', 'wg', 1),
                buildGroupId('base', 'wg', 1));
        });
    });

    describe('barrier records', () => {
        it('should round trip a barrier record', () => {
            const parsed = parseBarrierRecord(
                buildBarrierRecord({ generation: 4, partition: 2 }));
            assert.strictEqual(parsed.type, BARRIER_RECORD_TYPE);
            assert.strictEqual(parsed.generation, 4);
            assert.strictEqual(parsed.partition, 2);
            assert.strictEqual(typeof parsed.createdAt, 'string');
        });

        it('should read a barrier record out of a Buffer', () => {
            const parsed = parseBarrierRecord(Buffer.from(
                buildBarrierRecord({ generation: 1, partition: 0 })));
            assert.strictEqual(parsed.generation, 1);
        });

        it('should return null without throwing on a payload that is not one', () => {
            ['this is not json', '', '[]', 'null',
                JSON.stringify({ type: 'something-else', generation: 1 }),
                JSON.stringify({ generation: 1 })].forEach(value =>
                assert.strictEqual(parseBarrierRecord(value), null,
                    `for "${value}"`));
        });
    });
});
