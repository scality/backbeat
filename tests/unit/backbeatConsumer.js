const assert = require('assert');
const sinon = require('sinon');

const BackbeatConsumer = require('../../lib/BackbeatConsumer');
const KafkaBacklogMetrics = require('../../lib/KafkaBacklogMetrics');
const { CODES } = require('node-rdkafka');

const { kafka } = require('../config.json');
const { unassignStatus } = require('../../lib/constants');
const { BreakerState } = require('breakbeat').CircuitBreaker;

class BackbeatConsumerMock extends BackbeatConsumer {
    _init() {}
}

describe('backbeatConsumer', () => {
    afterEach(() => {
        process.env.KAFKA_TOPIC_PREFIX = '';
    });

    it('should use default topic name without prefix', () => {
        const backbeatConsumer = new BackbeatConsumer({
            kafka,
            groupId: 'unittest-group',
            topic: 'my-test-topic',
        });
        assert.strictEqual(backbeatConsumer._topic, 'my-test-topic');
    });

    it('should use default topic name with prefix', () => {
        process.env.KAFKA_TOPIC_PREFIX = 'testing.';
        const backbeatConsumer = new BackbeatConsumer({
            kafka,
            groupId: 'unittest-group',
            topic: 'my-test-topic',
        });
        assert.strictEqual(backbeatConsumer._topic, 'testing.my-test-topic');
    });
    
    describe('pause/resume topic partitions on circuit breaker', () => {
        let consumer;

        beforeEach(() => {
            consumer = new BackbeatConsumerMock({
                kafka,
                groupId: 'unittest-group',
                topic: 'my-test-topic',
            });

            const mockConsumer = {
                pause: sinon.stub().returns(),
                resume: sinon.stub().returns(),
                isConnected: () => false,
            };
            consumer._consumer = mockConsumer;
        });

        afterEach(() => {
            sinon.restore();
        });

        describe('_pauseAssignments', () => {
            it('should not pause when consumer not connected', () => {
                consumer._consumer.isConnected = () => false;
                consumer._consumer.subscription = () => ['example-topic'];
                consumer._pauseAssignments();
                assert(consumer._consumer.pause.notCalled);
            });

            it('should not call pause when no paritions assigned', () => {
                consumer._consumer.isConnected = () => true;
                consumer._consumer.subscription = () => ['example-topic'];
                consumer._consumer.assignments = () => [];
                consumer._pauseAssignments();
                assert(consumer._consumer.pause.notCalled);
            });

            it('should pause all assignments', () => {
                consumer._consumer.isConnected = () => true;
                consumer._consumer.subscription = () => ['example-topic'];

                const assignments = [
                    { topic: 'my-test-topic', partition: 0 },
                    { topic: 'my-test-topic', partition: 1 },
                    { topic: 'my-test-topic', partition: 2 },
                ];
                consumer._consumer.assignments = () => assignments;

                consumer._pauseAssignments();
                assert(consumer._consumer.pause.calledWithMatch([
                    { topic: 'my-test-topic', partition: 0 },
                    { topic: 'my-test-topic', partition: 1 },
                    { topic: 'my-test-topic', partition: 2 },
                ]));
            });
        });

        describe('_resumePausedPartitions', () => {
            it('should not resume when consumer not connected', () => {
                consumer._consumer.isConnected = () => false;
                consumer._consumer.subscription = () => ['example-topic'];
                consumer._resumePausedPartitions();
                assert(consumer._consumer.resume.notCalled);
            });

            it('should not call resume when no paritions are paused', () => {
                consumer._consumer.isConnected = () => true;
                consumer._consumer.subscription = () => ['example-topic'];
                consumer._consumer.assignments = () => [];
                consumer._resumePausedPartitions();
                assert(consumer._consumer.resume.notCalled);
            });

            it('should resume all paused partitions', () => {
                consumer._consumer.isConnected = () => true;
                consumer._consumer.subscription = () => ['example-topic'];
                consumer._consumer.assignments = () => [
                    { topic: 'my-test-topic', partition: 0 },
                    { topic: 'my-test-topic', partition: 1 },
                    { topic: 'my-test-topic', partition: 2 },
                ];

                consumer._resumePausedPartitions();
                assert(consumer._consumer.resume.calledWithMatch([
                    { topic: 'my-test-topic', partition: 0 },
                    { topic: 'my-test-topic', partition: 1 },
                    { topic: 'my-test-topic', partition: 2 },
                ]));
            });
        });

        describe('_onCircuitBreakerStateChanged', () => {
            it('should resume consumption when circuit breaker state is nominal', () => {
                const stub = sinon.stub(consumer, '_resumePausedPartitions');
                consumer._onCircuitBreakerStateChanged(BreakerState.Nominal);
                assert(stub.calledOnce);
            });

            it('should pause consumption when circuit breaker state got tripped', () => {
                const stub = sinon.stub(consumer, '_pauseAssignments');
                consumer._onCircuitBreakerStateChanged(BreakerState.Tripped);
                assert(stub.calledOnce);
            });

            it('should keep old state when circuit breaker state is stabilizing', () => {
                const resumeStub = sinon.stub(consumer, '_resumePausedPartitions');
                const pauseStub = sinon.stub(consumer, '_pauseAssignments');
                consumer._onCircuitBreakerStateChanged(BreakerState.Stabilizing);
                assert(resumeStub.notCalled);
                assert(pauseStub.notCalled);
            });

            it('should do nothing when state is unknown', () => {
                const resumeStub = sinon.stub(consumer, '_resumePausedPartitions');
                const pauseStub = sinon.stub(consumer, '_pauseAssignments');
                consumer._onCircuitBreakerStateChanged(-1);
                assert(resumeStub.notCalled);
                assert(pauseStub.notCalled);
            });
        });
    });

    describe('sequentialy consume from topic', () => {
        let consumer;

        beforeEach(() => {
            consumer = new BackbeatConsumerMock({
                kafka,
                groupId: 'unittest-group',
                topic: 'my-test-topic',
            });
        });

        afterEach(() => {
            sinon.restore();
            clearTimeout(consumer._tryConsumedTimeout);
        });

        it('should not consume new messages when a call to consume is already in progress', () => {
            consumer._concurrency = 2;
            consumer._consumer = {
                consume: sinon.stub(),
            };
            consumer._processingQueue = {
                running: sinon.stub()
                    .returns(0)
                    .onFirstCall().returns(1),
                length: () => 0,
            };
            // consume() is called here but hangs as we never return
            consumer._tryConsume();
            // this call should return without calling consume()
            consumer._tryConsume();
            assert(consumer._consumer.consume.calledOnce);
            assert.strictEqual(consumer._tasksCompletedSinceLastConsume, true);
        });

        it('should immediatly try to consume if a task completed since the last consume', done => {
            consumer._concurrency = 2;
            // setting to true to simulate a task completed
            consumer._tasksCompletedSinceLastConsume = true;
            consumer._consumer = {
                consume: sinon.stub().yields(null, [{}, {}]),
            };
            consumer._processingQueue = {
                running: sinon.stub()
                    .returns(0)
                    .onFirstCall().returns(1),
                length: () => 0,
            };
            const tryConsumeSpy = sinon.spy(consumer, '_tryConsume');
            consumer._tryConsume();
            consumer._consumer.consume.onSecondCall().callsFake(() => {
                assert(tryConsumeSpy.calledTwice);
                assert.strictEqual(consumer._tasksCompletedSinceLastConsume, false);
                done();
            });
        });

        it('should only consume once if no tasks completed since last consume', done => {
            consumer._concurrency = 2;
            consumer._tasksCompletedSinceLastConsume = false;
            consumer._consumer = {
                consume: sinon.stub().yields(null, [{}, {}]),
            };
            consumer._processingQueue = {
                running: sinon.stub()
                    .returns(0)
                    .onFirstCall().returns(1),
                length: () => 0,
            };
            const tryConsumeSpy = sinon.spy(consumer, '_tryConsume');
            consumer._tryConsume();
            // consume is called asynchronously, waiting 500ms to make
            // sure it didn't get called at the end of the first one.
            setTimeout(() => {
                assert(tryConsumeSpy.calledOnce);
                assert.strictEqual(consumer._tasksCompletedSinceLastConsume, false);
                done();
            }, 500);
        });
    });

    describe('onEntryCommittable', () => {
        let consumer;
        let mockConsumer;

        const entry = {
            topic: 'my-test-topic',
            partition: 2,
            offset: 280,
            key: null,
            timestamp: Date.now(),
        };

        beforeEach(() => {
            consumer = new BackbeatConsumerMock({
                kafka,
                groupId: 'unittest-group',
                topic: 'my-test-topic',
            });

            mockConsumer = {
                offsetsStore: sinon.stub(),
                subscription: sinon.stub().returns(['my-test-topic']),
                isConnected: sinon.stub().returns(true),
            };
            consumer._consumer = mockConsumer;

            // pre-register the offset as consumed so onOffsetProcessed returns a value
            consumer._offsetLedger.onOffsetConsumed(entry.topic, entry.partition, entry.offset);
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should call offsetsStore when consumer is active and connected', () => {
            consumer.onEntryCommittable(entry);
            assert(mockConsumer.offsetsStore.calledOnce);
        });

        it('should not call offsetsStore when consumer is paused (unsubscribed)', () => {
            mockConsumer.subscription.returns([]);
            consumer.onEntryCommittable(entry);
            assert(mockConsumer.offsetsStore.notCalled);
        });

        it('should not call offsetsStore when consumer is not connected', () => {
            mockConsumer.isConnected.returns(false);
            consumer.onEntryCommittable(entry);
            assert(mockConsumer.offsetsStore.notCalled);
        });

        it('should not throw and always log at error level when offsetsStore throws', () => {
            const errState = new Error('Local: Erroneous state');
            errState.code = CODES.ERRORS.ERR__STATE;
            mockConsumer.offsetsStore.throws(errState);

            const errorSpy = sinon.spy(consumer._log, 'error');

            assert.doesNotThrow(() => consumer.onEntryCommittable(entry));
            assert(errorSpy.calledOnce);
        });

        it('should not throw and log at error level when offsetsStore throws an unexpected error', () => {
            const unexpectedErr = new Error('unexpected kafka error');
            unexpectedErr.code = -1;
            mockConsumer.offsetsStore.throws(unexpectedErr);

            const errorSpy = sinon.spy(consumer._log, 'error');

            assert.doesNotThrow(() => consumer.onEntryCommittable(entry));
            assert(errorSpy.calledOnce);
        });
    });

    describe('_tryConsume', () => {
        let consumer;

        beforeEach(() => {
            consumer = new BackbeatConsumerMock({
                kafka,
                groupId: 'unittest-group',
                topic: 'my-test-topic',
            });
            consumer._processingQueue = {
                length: () => 0,
                running: () => 0,
            };
            consumer._consumer = { consume: sinon.stub() };
        });

        it('should fetch while the consumer is running', () => {
            consumer._tryConsume();

            assert.strictEqual(consumer._consumer.consume.calledOnce, true);
        });

        it('should not fetch once the shutdown has started', () => {
            consumer._shuttingDown = true;

            consumer._tryConsume();

            assert.strictEqual(consumer._consumer.consume.called, false);
        });
    });

    describe('_getAvailableSlotsInPipeline', () => {
        let consumer;

        beforeEach(() => {
            consumer = new BackbeatConsumerMock({
                kafka,
                groupId: 'unittest-group',
                topic: 'my-test-topic',
            });
        });

        [
            {
                // should take into account pending requests
                state: {
                    maxQueued : 10,
                    concurrency : 10,
                    processingQueue: {
                        length: () => 0,
                        running: () => 0,
                    },
                    nConsumePendingRequests: 5,
                },
                expectedSlots: 5,
            },{
                // should not exceed max running tasks
                state: {
                    maxQueued : 10,
                    concurrency : 10,
                    processingQueue: {
                        length: () => 0,
                        running: () => 9,
                    },
                    nConsumePendingRequests: 0,
                },
                expectedSlots: 1,
            },{
                // should not exceed max queued
                state: {
                    maxQueued : 10,
                    concurrency : 10,
                    processingQueue: {
                        length: () => 9,
                        running: () => 1,
                    },
                    nConsumePendingRequests: 0,
                },
                expectedSlots: 1,
            },{
                // should return 0 when exceeding max queued tasks
                state: {
                    maxQueued : 10,
                    concurrency : 10,
                    processingQueue: {
                        length: () => 12,
                        running: () => 1,
                    },
                    nConsumePendingRequests: 0,
                },
                expectedSlots: 0,
            },{
                // should return 0 when exceeding max running tasks
                state: {
                    maxQueued : 10,
                    concurrency : 10,
                    processingQueue: {
                        length: () => 3,
                        running: () => 13,
                    },
                    nConsumePendingRequests: 0,
                },
                expectedSlots: 0,
            },
        ].forEach((params, i) => {
            it(`should return available slots in pipeline (scenario ${i})`, () => {
                consumer._maxQueued = params.state.maxQueued;
                consumer._concurrency = params.state.concurrency;
                consumer._processingQueue = params.state.processingQueue;
                consumer._nConsumePendingRequests = params.state.nConsumePendingRequests;
                const availableSlots = consumer._getAvailableSlotsInPipeline();
                assert.strictEqual(availableSlots, params.expectedSlots);
            });
        });
    });

    describe('_onRebalance deferred un-assign', () => {
        const REVOKE = { code: CODES.ERRORS.ERR__REVOKE_PARTITIONS };
        const ASSIGN = { code: CODES.ERRORS.ERR__ASSIGN_PARTITIONS };
        const partitions = [
            { topic: 'my-test-topic', partition: 0 },
            { topic: 'my-test-topic', partition: 1 },
        ];

        let consumer;
        let drainCallbacks;
        let queueIdle;
        let ledgerCount;

        beforeEach(() => {
            consumer = new BackbeatConsumerMock({
                kafka,
                groupId: 'unittest-group',
                topic: 'my-test-topic',
            });

            consumer._consumer = {
                assign: sinon.stub(),
                unassign: sinon.stub(),
                disconnect: sinon.stub(),
                commit: sinon.stub(),
                pause: sinon.stub(),
                resume: sinon.stub(),
                isConnected: () => true,
                assignments: () => [],
                subscription: () => ['my-test-topic'],
            };

            queueIdle = false;
            ledgerCount = 1;
            drainCallbacks = [];
            consumer._processingQueue = {
                length: () => 0,
                running: () => (queueIdle ? 0 : 1),
                idle: () => queueIdle,
                setDrain: func => drainCallbacks.push(func),
            };
            consumer._offsetLedger.getProcessingCount = () => ledgerCount;

            sinon.stub(KafkaBacklogMetrics, 'onRebalance');
        });

        afterEach(() => {
            clearTimeout(consumer._drainProcessQueueTimeout);
            sinon.restore();
        });

        const completeDrain = () => {
            queueIdle = true;
            ledgerCount = 0;
            consumer._drainCallback();
        };

        it('should un-assign once the drain completes', done => {
            consumer.on('unassign', status => {
                assert.strictEqual(status, unassignStatus.DRAINED);
                assert(consumer._consumer.unassign.calledOnce);
                done();
            });

            consumer._onRebalance(REVOKE, partitions);
            assert(consumer._consumer.unassign.notCalled);
            completeDrain();
        });

        it('should un-assign immediately when nothing is in flight', done => {
            queueIdle = true;
            ledgerCount = 0;

            consumer.on('unassign', status => {
                assert.strictEqual(status, unassignStatus.IDLE);
                assert(consumer._consumer.unassign.calledOnce);
                done();
            });

            consumer._onRebalance(REVOKE, partitions);
        });

        it('should not un-assign when a new assignment arrived while draining', () => {
            consumer._onRebalance(REVOKE, partitions);
            const deferredUnassign = drainCallbacks[drainCallbacks.length - 1];

            // the next generation grants the partitions back mid-drain
            consumer._onRebalance(ASSIGN, partitions);
            assert(consumer._consumer.assign.calledOnce);

            queueIdle = true;
            ledgerCount = 0;
            deferredUnassign();

            assert(consumer._consumer.unassign.notCalled);
            assert(KafkaBacklogMetrics.onRebalance.calledWith(
                'my-test-topic', 'unittest-group', unassignStatus.SUPERSEDED));
        });

        it('should synchronise the assignment on an arbitrary rebalance error',
        () => {
            // the bump above superseded whatever revoke was pending and
            // dropped its watchdog, so nothing else answers this callback
            consumer._onRebalance({ code: -1 }, partitions);

            assert(consumer._consumer.unassign.calledOnce);
        });

        it('should not un-assign when a later revoke superseded the drain', () => {
            consumer._onRebalance(REVOKE, partitions);
            const firstUnassign = drainCallbacks[drainCallbacks.length - 1];

            consumer._onRebalance(REVOKE, partitions);

            queueIdle = true;
            ledgerCount = 0;
            firstUnassign();

            assert(consumer._consumer.unassign.notCalled);
        });

        it('should not un-assign when the partitions were granted back while ' +
        'offsets were being published', () => {
            let publishDone;
            consumer._kafkaBacklogMetricsConfig = { zkPath: '/test', intervalS: 5 };
            consumer._publishOffsetsCron = cb => {
                publishDone = cb;
            };

            consumer._onRebalance(REVOKE, partitions);
            completeDrain();
            assert.strictEqual(typeof publishDone, 'function');
            assert(consumer._consumer.unassign.notCalled);

            consumer._onRebalance(ASSIGN, partitions);
            publishDone();

            assert(consumer._consumer.unassign.notCalled);
            assert(KafkaBacklogMetrics.onRebalance.calledWith(
                'my-test-topic', 'unittest-group', unassignStatus.SUPERSEDED));
        });

        it('should not leave a superseded revoke watchdog armed', () => {
            const clock = sinon.useFakeTimers();
            try {
                consumer._onRebalance(REVOKE, partitions);
                consumer._onRebalance(REVOKE, partitions);

                clock.tick(consumer._maxPollIntervalMs + 1000);
                assert(consumer._consumer.disconnect.calledOnce);
            } finally {
                clock.restore();
            }
        });

        it('should decline a partition grant arriving during shutdown', () => {
            consumer._shuttingDown = true;
            consumer._onRebalance(ASSIGN, partitions);

            // taking the grant would leave the disconnect an assignment to
            // revoke all over again, which is what wedges it
            assert(consumer._consumer.assign.calledOnce);
            assert.deepStrictEqual(
                consumer._consumer.assign.firstCall.args[0], []);
        });

        it('should still answer the grant when assign is refused mid-close',
        () => {
            // the binding gates assign() on isConnected(), which is already
            // false once disconnect() has started, so the decline throws;
            // unassign() stays permitted and is a valid answer
            consumer._shuttingDown = true;
            consumer._consumer.assign = sinon.stub().throws(
                new Error('KafkaConsumer is not connected'));

            consumer._onRebalance(ASSIGN, partitions);

            assert(consumer._consumer.unassign.calledOnce);
        });

        it('should answer a revoke arriving during shutdown', () => {
            consumer._shuttingDown = true;
            consumer._onRebalance(REVOKE, partitions);

            // an unanswered rebalance callback leaves the client in the
            // rebalance and wedges the disconnect; close() cannot be relied
            // on to answer one raised after it has already un-assigned
            assert(consumer._consumer.unassign.calledOnce);
            assert.strictEqual(consumer._drainCallback, null);
            assert.strictEqual(consumer._drainProcessQueueTimeout, null);
            assert(KafkaBacklogMetrics.onRebalance.calledWith(
                'my-test-topic', 'unittest-group', unassignStatus.SHUTDOWN));
        });

        it('should ignore a deferred un-assign once the shutdown owns the ' +
        'departure', () => {
            consumer._onRebalance(REVOKE, partitions);
            const deferredUnassign = drainCallbacks[drainCallbacks.length - 1];

            // close() takes over while the drain is outstanding
            consumer._shuttingDown = true;

            queueIdle = true;
            ledgerCount = 0;
            deferredUnassign();

            assert(consumer._consumer.unassign.notCalled);
        });

        it('should leave the current drain and timeout armed when a superseded ' +
        'un-assign fires', () => {
            consumer._onRebalance(REVOKE, partitions);
            const supersededUnassign = drainCallbacks[drainCallbacks.length - 1];

            consumer._onRebalance(ASSIGN, partitions);
            consumer._onRebalance(REVOKE, partitions);

            const currentDrain = consumer._drainCallback;
            const currentTimeout = consumer._drainProcessQueueTimeout;
            assert.notStrictEqual(currentDrain, null);
            assert.notStrictEqual(currentTimeout, null);

            // or the callback returns before reaching the guard
            queueIdle = true;
            ledgerCount = 0;
            supersededUnassign();

            assert(KafkaBacklogMetrics.onRebalance.calledWith(
                'my-test-topic', 'unittest-group', unassignStatus.SUPERSEDED));

            assert.strictEqual(consumer._drainCallback, currentDrain);
            assert.strictEqual(consumer._drainProcessQueueTimeout, currentTimeout);
            assert(consumer._consumer.unassign.notCalled);
        });
    });

    describe('close', () => {
        let consumer;
        let queueIdle;
        let ledgerCount;
        let drainCallbacks;
        let onDisconnected;

        beforeEach(() => {
            consumer = new BackbeatConsumerMock({
                kafka,
                groupId: 'unittest-group',
                topic: 'my-test-topic',
            });

            queueIdle = true;
            ledgerCount = 0;
            drainCallbacks = [];
            consumer._processingQueue = {
                length: () => 0,
                running: () => (queueIdle ? 0 : 1),
                idle: () => queueIdle,
                setDrain: func => drainCallbacks.push(func),
            };
            consumer._offsetLedger.getProcessingCount = () => ledgerCount;

            onDisconnected = null;
            consumer._consumer = {
                assign: sinon.stub(),
                commit: sinon.stub(),
                unassign: sinon.stub(),
                unsubscribe: sinon.stub(),
                // only a real disconnect emits 'disconnected'
                disconnect: sinon.stub().callsFake(() => {
                    if (onDisconnected) {
                        setImmediate(onDisconnected);
                    }
                }),
                assignments: () => [],
                subscription: () => ['my-test-topic'],
                isConnected: () => true,
                once: (event, handler) => {
                    if (event === 'disconnected') {
                            setImmediate(handler);
                    }
                },
            };
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should leave the group before disconnecting', done => {
            consumer.close(() => {
                const { commit, unassign, unsubscribe, disconnect } = consumer._consumer;
                assert(commit.calledOnce);
                assert(unassign.calledOnce);
                assert(unsubscribe.calledOnce);
                assert(disconnect.calledOnce);
                // committing after un-assign would commit nothing, and it is
                // the un-assign following unsubscribe that sends the LeaveGroup
                assert(commit.calledBefore(unsubscribe));
                assert(unsubscribe.calledBefore(unassign));
                assert(unassign.calledBefore(disconnect));
                done();
            });
        });

        it('should keep waiting for its drain when a grant arrives mid-close', done => {
            queueIdle = false;
            ledgerCount = 1;

            let closed = false;
            consumer.close(() => {
                closed = true;
            });

            setImmediate(() => {
                // a grant must not tear down the wait close() installed
                consumer._onRebalance(
                    { code: CODES.ERRORS.ERR__ASSIGN_PARTITIONS },
                    [{ topic: 'my-test-topic', partition: 0 }]);
                assert.strictEqual(closed, false);

                queueIdle = true;
                ledgerCount = 0;
                drainCallbacks[drainCallbacks.length - 1]();

                setImmediate(() => {
                    assert.strictEqual(closed, true);
                    done();
                });
            });
        });

        it('should complete when the consumer was never created', done => {
            // close() can race startup, before _initConsumer() has run
            consumer._consumer = null;
            consumer.close(done);
        });

        it('should keep leaving the group when a call throws', done => {
            consumer._consumer.commit.throws(new Error('Local: Erroneous state'));
            consumer.close(() => {
                assert(consumer._consumer.unsubscribe.calledOnce);
                assert(consumer._consumer.unassign.calledOnce);
                assert(consumer._consumer.disconnect.calledOnce);
                done();
            });
        });

        it('should wait for in-flight work before releasing the partitions', done => {
            queueIdle = false;
            ledgerCount = 1;

            let closed = false;
            consumer.close(() => {
                closed = true;
            });

            setImmediate(() => {
                assert.strictEqual(closed, false);
                assert(consumer._consumer.unassign.notCalled);

                queueIdle = true;
                ledgerCount = 0;
                drainCallbacks[drainCallbacks.length - 1]();

                setImmediate(() => {
                    assert.strictEqual(closed, true);
                    assert(consumer._consumer.unassign.calledOnce);
                    done();
                });
            });
        });

        it('should still complete when a revoke arrives while it waits for ' +
        'the drain', done => {
            queueIdle = false;
            ledgerCount = 1;

            let closed = false;
            consumer.close(() => {
                closed = true;
            });

            setImmediate(() => {
                // releasing partitions here must not tear down close()'s wait
                consumer._onRebalance(
                    { code: CODES.ERRORS.ERR__REVOKE_PARTITIONS },
                    [{ topic: 'my-test-topic', partition: 0 }]);

                queueIdle = true;
                ledgerCount = 0;
                drainCallbacks[drainCallbacks.length - 1]();

                setImmediate(() => {
                    assert.strictEqual(closed, true);
                    done();
                });
            });
        });

        it('should leave the group anyway when the drain never completes', done => {
            const clock = sinon.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
            queueIdle = false;
            ledgerCount = 1;

            let closed = false;
            consumer.close(() => {
                closed = true;
            });

            setImmediate(() => {
                assert.strictEqual(closed, false);
                clock.tick(consumer._maxPollIntervalMs);

                setImmediate(() => {
                    clock.restore();
                    assert.strictEqual(closed, true);
                    assert(consumer._consumer.unassign.calledOnce);
                    assert(consumer._consumer.unsubscribe.calledOnce);
                    done();
                });
            });
        });

        it('should leave the group when only the ledger is stuck', done => {
            // the replication queue processor's shape: its stop() closes the
            // status producer first, and a task's offset is only committable
            // from that producer's delivery callback, so a report that never
            // arrives leaves a ledger entry outstanding with the queue idle
            const clock = sinon.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
            queueIdle = true;
            ledgerCount = 1;

            let closed = false;
            consumer.close(() => {
                closed = true;
            });

            setImmediate(() => {
                assert.strictEqual(closed, false);
                clock.tick(consumer._maxPollIntervalMs);

                setImmediate(() => {
                    clock.restore();
                    assert.strictEqual(closed, true);
                    assert(consumer._consumer.unassign.calledOnce);
                    done();
                });
            });
        });

        it('should answer every caller once when closed twice', done => {
            queueIdle = false;
            ledgerCount = 1;

            let first = false;
            let second = 0;
            consumer.close(() => {
                first = true;
            });
            consumer.close(() => {
                second++;
            });

            setImmediate(() => {
                queueIdle = true;
                ledgerCount = 0;
                drainCallbacks[drainCallbacks.length - 1]();

                setTimeout(() => {
                    assert.strictEqual(first, true);
                    assert.strictEqual(second, 1);
                    assert(consumer._consumer.disconnect.calledOnce);
                    done();
                }, 20);
            });
        });

        it('should not wait for the drain once the consumer is disconnected', done => {
            // the drain watchdog fires on a wedged task and disconnects, so the
            // work it is waiting on can never complete
            queueIdle = false;
            ledgerCount = 1;
            consumer._consumer.isConnected = () => false;

            consumer.close(() => {
                assert(consumer._consumer.disconnect.notCalled);
                done();
            });
        });

        it('should not wait for an in-flight offset publish', done => {
            consumer._publishOffsetsCronActive = true;
            consumer.close(() => {
                assert(consumer._consumer.disconnect.calledOnce);
                done();
            });
        });

        it('should stop waiting for a disconnect that never completes', done => {
            // setImmediate has to stay real, the test drives the flow with it
            const clock = sinon.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
            consumer._consumer.once = () => {};

            let closed = false;
            consumer.close(() => {
                closed = true;
            });

            setImmediate(() => {
                clock.tick(10000);
                setImmediate(() => {
                    clock.restore();
                    assert.strictEqual(closed, true);
                    assert(consumer._consumer.unsubscribe.calledOnce);
                    done();
                });
            });
        });

        it('should drop a rebalance watchdog armed before the shutdown', done => {
            consumer._drainProcessQueueTimeout = setTimeout(() => {
                done(new Error('the rebalance watchdog fired during shutdown'));
            }, 20);

            consumer.close(() => {
                assert.strictEqual(consumer._drainProcessQueueTimeout, null);
                setTimeout(done, 40);
            });
        });
    });
});
