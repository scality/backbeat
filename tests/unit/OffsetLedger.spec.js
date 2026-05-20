const assert = require('assert');

const OffsetLedger = require('../../lib/OffsetLedger');

describe('OffsetLedger', () => {
    it('should get no committable offset if no offset registered yet', () => {
        const ledger = new OffsetLedger();

        assert.strictEqual(ledger.getCommittableOffset('topic', 0), null);

        assert.strictEqual(ledger.getProcessingCount(), 0);
        assert.strictEqual(ledger.getProcessingCount('topic'), 0);
        assert.strictEqual(ledger.getProcessingCount('topic', 0), 0);
    });

    it('should get correct offset with one consumed message', () => {
        const ledger = new OffsetLedger();

        ledger.onOffsetConsumed('topic', 0, 42);
        assert.strictEqual(ledger.getCommittableOffset('topic', 0), 42);
        assert.strictEqual(ledger.getProcessingCount(), 1);
        assert.strictEqual(ledger.getProcessingCount('topic'), 1);
        assert.strictEqual(ledger.getProcessingCount('topic', 0), 1);
        assert.strictEqual(ledger.getProcessingCount('topic', 1), 0);
        assert.strictEqual(ledger.getProcessingCount('topic2'), 0);
        assert.strictEqual(ledger.getProcessingCount('topic2', 0), 0);

        const committableOffset = ledger.onOffsetProcessed('topic', 0, 42);
        assert.strictEqual(ledger.getCommittableOffset('topic', 0), 43);
        assert.strictEqual(committableOffset, 43);
    });

    it('should get correct offset with two messages processed in-order', () => {
        const ledger = new OffsetLedger();

        ledger.onOffsetConsumed('topic', 0, 42);
        assert.strictEqual(ledger.getCommittableOffset('topic', 0), 42);
        ledger.onOffsetConsumed('topic', 0, 43);
        assert.strictEqual(ledger.getCommittableOffset('topic', 0), 42);
        assert.strictEqual(ledger.getProcessingCount(), 2);
        assert.strictEqual(ledger.getProcessingCount('topic'), 2);
        assert.strictEqual(ledger.getProcessingCount('topic', 0), 2);
        ledger.onOffsetProcessed('topic', 0, 42);
        assert.strictEqual(ledger.getProcessingCount(), 1);
        assert.strictEqual(ledger.getProcessingCount('topic'), 1);
        assert.strictEqual(ledger.getProcessingCount('topic', 0), 1);
        assert.strictEqual(ledger.getCommittableOffset('topic', 0), 43);
        const committableOffset = ledger.onOffsetProcessed('topic', 0, 43);
        assert.strictEqual(ledger.getCommittableOffset('topic', 0), 44);
        assert.strictEqual(committableOffset, 44);
    });

    it('should get correct offset with two messages processed out-of-order',
    () => {
        const ledger = new OffsetLedger();

        ledger.onOffsetConsumed('topic', 0, 42);
        assert.strictEqual(ledger.getCommittableOffset('topic', 0), 42);
        ledger.onOffsetConsumed('topic', 0, 43);
        assert.strictEqual(ledger.getCommittableOffset('topic', 0), 42);
        ledger.onOffsetProcessed('topic', 0, 43);
        assert.strictEqual(ledger.getCommittableOffset('topic', 0), 42);
        ledger.onOffsetProcessed('topic', 0, 42);
        assert.strictEqual(ledger.getCommittableOffset('topic', 0), 44);
    });

    it('should get correct offset with ten messages processed out-of-order',
    () => {
        const ledger = new OffsetLedger();

        const finishOrder = [46, 49, 50, 42, 51, 44, 48, 47, 43, 45];
        const expectedCommittableOffsets =
              [42, 42, 42, 43, 43, 43, 43, 43, 45, 52];

        for (let i = 0; i < 10; ++i) {
            ledger.onOffsetConsumed('topic', 0, 42 + i);
            assert.strictEqual(ledger.getCommittableOffset('topic', 0), 42);
        }
        assert.strictEqual(ledger.getProcessingCount('topic', 0), 10);
        for (let i = 0; i < 10; ++i) {
            ledger.onOffsetProcessed('topic', 0, finishOrder[i]);
            assert.strictEqual(ledger.getCommittableOffset('topic', 0),
                               expectedCommittableOffsets[i]);
        }
        assert.strictEqual(ledger.getProcessingCount('topic', 0), 0);
    });

    it('should get correct offset with ten messages processed intermingled ' +
    'with ten new messages consumed', () => {
        const ledger = new OffsetLedger();

        const finishOrder =
              [44, 50, 46, 53, 45, 42, 51, 57, 49, 56,
               47, 43, 55, 61, 59, 54, 48, 60, 52, 58];

        const expectedCommittableOffsets =
              [42, 42, 42, 42, 42, 43, 43, 43, 43, 43,
               43, 48, 48, 48, 48, 48, 52, 52, 58, 62];

        for (let i = 0; i < 10; ++i) {
            ledger.onOffsetConsumed('topic', 0, 42 + i);
            assert.strictEqual(ledger.getCommittableOffset('topic', 0), 42);
        }
        for (let i = 0; i < 20; ++i) {
            ledger.onOffsetProcessed('topic', 0, finishOrder[i]);
            if (i < 10) {
                ledger.onOffsetConsumed('topic', 0, 52 + i);
            }
            assert.strictEqual(ledger.getCommittableOffset('topic', 0),
                               expectedCommittableOffsets[i]);
        }
    });

    it('should get correct offset with replay of some consumed entries',
    () => {
        const ledger = new OffsetLedger();

        const finishOrder = [46, 44, 50, 42, 51, 49, 43, 45, 47, 48];
        const expectedCommittableOffsets =
              [42, 42, 42, 43, 43, 43, 45, 47, 48, 49];

        for (let i = 0; i < 10; ++i) {
            ledger.onOffsetConsumed('topic', 0, 42 + i);
            assert.strictEqual(ledger.getCommittableOffset('topic', 0), 42);
        }
        for (let i = 0; i < 5; ++i) {
            ledger.onOffsetProcessed('topic', 0, finishOrder[i]);
            assert.strictEqual(ledger.getCommittableOffset('topic', 0),
                               expectedCommittableOffsets[i]);
        }
        for (let i = 5; i < 10; ++i) {
            // introduce replayed messages
            ledger.onOffsetConsumed('topic', 0, 42 + i);
            ledger.onOffsetProcessed('topic', 0, finishOrder[i]);
            assert.strictEqual(ledger.getCommittableOffset('topic', 0),
                               expectedCommittableOffsets[i]);
        }
    });

    it('should manage independent offsets per topic/partition', () => {
        const ledger = new OffsetLedger();

        ['topic1', 'topic2'].forEach(topic => {
            [0, 1].forEach(partition => {
                ledger.onOffsetConsumed(topic, partition, 42 + partition);
                ledger.onOffsetConsumed(topic, partition, 43 + partition);
                assert.strictEqual(
                    ledger.getCommittableOffset(topic, partition),
                    42 + partition);
                assert.strictEqual(
                    ledger.getProcessingCount(topic, partition), 2);
                assert.strictEqual(
                    ledger.getProcessingCount(topic, partition + 1), 0);
                assert.strictEqual(
                    ledger.getProcessingCount('foo', partition), 0);
            });
        });
        ['topic2', 'topic1'].forEach(topic => {
            [1, 0].forEach(partition => {
                ledger.onOffsetProcessed(topic, partition, 42 + partition);
                assert.strictEqual(
                    ledger.getCommittableOffset(topic, partition),
                    43 + partition);
                ledger.onOffsetProcessed(topic, partition, 43 + partition);
                assert.strictEqual(
                    ledger.getCommittableOffset(topic, partition),
                    44 + partition);
            });
        });
    });

    describe(`'empty' event`, () => {
        it('should emit "empty" when the in-flight count for a topic ' +
           'transitions to zero', done => {
            const ledger = new OffsetLedger();
            ledger.on('empty', topic => {
                assert.strictEqual(topic, 'topic');
                assert.strictEqual(ledger.getProcessingCount('topic'), 0);
                done();
            });
            ledger.onOffsetConsumed('topic', 0, 1);
            ledger.onOffsetProcessed('topic', 0, 1);
        });

        it('should not emit "empty" when count goes from N to N-1 (still ' +
           'non-zero)', done => {
            const ledger = new OffsetLedger();
            ledger.on('empty', () => {
                done(new Error('unexpected empty emit'));
            });
            ledger.onOffsetConsumed('topic', 0, 1);
            ledger.onOffsetConsumed('topic', 0, 2);
            ledger.onOffsetProcessed('topic', 0, 1);
            // Wait a tick to confirm no deferred emit fires.
            setImmediate(() => {
                assert.strictEqual(ledger.getProcessingCount('topic'), 1);
                done();
            });
        });

        it('should only emit for the topic whose count went to zero', done => {
            const ledger = new OffsetLedger();
            const emits = [];
            ledger.on('empty', topic => emits.push(topic));
            ledger.onOffsetConsumed('topic1', 0, 1);
            ledger.onOffsetConsumed('topic2', 0, 1);
            ledger.onOffsetProcessed('topic1', 0, 1);
            setImmediate(() => {
                assert.deepStrictEqual(emits, ['topic1']);
                assert.strictEqual(ledger.getProcessingCount('topic2'), 1);
                done();
            });
        });

        it('should defer emit via nextTick so callers can finish their ' +
           'synchronous work', done => {
            const ledger = new OffsetLedger();
            let processedReturned = false;
            ledger.on('empty', () => {
                assert.strictEqual(processedReturned, true,
                    'emit must fire after onOffsetProcessed returns');
                done();
            });
            ledger.onOffsetConsumed('topic', 0, 1);
            ledger.onOffsetProcessed('topic', 0, 1);
            processedReturned = true;
        });

        it('should not emit if a new entry is consumed between scheduling ' +
           'and firing the deferred emit', done => {
            const ledger = new OffsetLedger();
            ledger.on('empty', () => {
                done(new Error('emit should have been suppressed'));
            });
            ledger.onOffsetConsumed('topic', 0, 1);
            ledger.onOffsetProcessed('topic', 0, 1);
            // Push a new entry before the deferred emit fires.
            ledger.onOffsetConsumed('topic', 0, 2);
            setImmediate(() => {
                assert.strictEqual(ledger.getProcessingCount('topic'), 1);
                done();
            });
        });

        it('should not schedule a nextTick when no listeners are ' +
           'registered (steady-state hot path)', () => {
            const ledger = new OffsetLedger();
            // No listener attached — would-be emits should be skipped
            // entirely. We can't directly observe the absence of the
            // nextTick, but we can verify nothing throws and behavior
            // is correct.
            ledger.onOffsetConsumed('topic', 0, 1);
            const committable = ledger.onOffsetProcessed('topic', 0, 1);
            assert.strictEqual(committable, 2);
            assert.strictEqual(ledger.getProcessingCount('topic'), 0);
        });
    });

    it('should be able to export the ledger in JSON format', () => {
        const ledger = new OffsetLedger();

        ['topic1', 'topic2'].forEach(topic => {
            [0, 1].forEach(partition => {
                ledger.onOffsetConsumed(topic, partition, 42 + partition);
                ledger.onOffsetConsumed(topic, partition, 43 + partition);
            });
        });
        ledger.onOffsetProcessed('topic1', 0, 42);
        ledger.onOffsetProcessed('topic2', 0, 43);
        ledger.onOffsetProcessed('topic2', 0, 42);

        assert.deepStrictEqual(JSON.parse(ledger.toString()), {
            topic1: { 0: { processing: [43], latestConsumed: 43 },
                      1: { processing: [43, 44], latestConsumed: 44 } },
            topic2: { 0: { processing: [], latestConsumed: 43 },
                      1: { processing: [43, 44], latestConsumed: 44 } } });
    });
});
