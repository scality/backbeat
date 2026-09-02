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
        let queueIdle;
        let ledgerCount;
        let unassigns;

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
            consumer._processingQueue = {
                length: () => 0,
                running: () => (queueIdle ? 0 : 1),
                idle: () => queueIdle,
                setDrain: () => {},
            };
            consumer._offsetLedger.getProcessingCount = () => ledgerCount;

            unassigns = [];
            consumer.on('unassign', status => unassigns.push(status));

            sinon.stub(KafkaBacklogMetrics, 'onRebalance');
        });

        afterEach(() => {
            clearTimeout(consumer._drainProcessQueueTimeout);
            sinon.restore();
        });

        // fire the drain through the live callback, which a superseding
        // rebalance nulls out
        const completeDrain = () => {
            queueIdle = true;
            ledgerCount = 0;
            if (consumer._drainCallback) {
                consumer._drainCallback();
            }
        };

        it('should defer the un-assign until the drain completes', () => {
            consumer._onRebalance(REVOKE, partitions);
            assert(consumer._consumer.unassign.notCalled);

            completeDrain();

            assert(consumer._consumer.unassign.calledOnce);
            assert.deepStrictEqual(unassigns, [unassignStatus.DRAINED]);
        });

        it('should un-assign immediately when nothing is in flight', () => {
            queueIdle = true;
            ledgerCount = 0;

            consumer._onRebalance(REVOKE, partitions);

            assert(consumer._consumer.unassign.calledOnce);
            assert.deepStrictEqual(unassigns, [unassignStatus.IDLE]);
        });

        it('should not un-assign when a new assignment arrived while ' +
        'draining', () => {
            consumer._onRebalance(REVOKE, partitions);

            // the next generation grants the partitions back mid-drain
            consumer._onRebalance(ASSIGN, partitions);
            assert(consumer._consumer.assign.calledOnce);
            assert.strictEqual(consumer._drainCallback, null);
            assert.strictEqual(consumer._drainProcessQueueTimeout, null);

            completeDrain();
            // the ledger going empty must not fire anything either
            consumer._offsetLedger.emit('empty', 'my-test-topic');

            assert(consumer._consumer.unassign.notCalled);
            assert.deepStrictEqual(unassigns, []);
        });

        it('should not un-assign when the partitions were granted back ' +
        'while offsets were being published', () => {
            let publishDone;
            consumer._kafkaBacklogMetricsConfig =
                { zkPath: '/test', intervalS: 5 };
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
            assert.deepStrictEqual(unassigns, []);
        });

        it('should not leave a superseded revoke watchdog armed', () => {
            const clock = sinon.useFakeTimers();
            try {
                consumer._onRebalance(REVOKE, partitions);
                consumer._onRebalance(REVOKE, partitions);

                completeDrain();
                assert(consumer._consumer.unassign.calledOnce);
                assert.deepStrictEqual(unassigns, [unassignStatus.DRAINED]);

                // the first revoke's watchdog must not fire late and
                // disconnect a consumer that already answered
                clock.tick(consumer._maxPollIntervalMs + 1000);
                assert(consumer._consumer.disconnect.notCalled);
                assert(consumer._consumer.unassign.calledOnce);
            } finally {
                clock.restore();
            }
        });
    });
});
