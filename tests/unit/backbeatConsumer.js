const assert = require('assert');
const sinon = require('sinon');

const BackbeatConsumer = require('../../lib/BackbeatConsumer');

const { kafka } = require('../config.json');
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
