'use strict';

const assert = require('assert');
const sinon = require('sinon');

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const KafkaBacklogMetrics = require('../../../lib/KafkaBacklogMetrics');

describe('BackbeatConsumer._processTask', () => {
    let savedEnv;
    beforeEach(() => {
        savedEnv = process.env.ENABLE_OTEL;
        sinon.stub(KafkaBacklogMetrics, 'onTaskStarted');
    });
    afterEach(() => {
        sinon.restore();
        if (savedEnv === undefined) {
            delete process.env.ENABLE_OTEL;
        } else {
            process.env.ENABLE_OTEL = savedEnv;
        }
    });

    function makeSelf(queueProcessor) {
        return {
            _startProcessingTask: () => () => {},
            _groupId: 'test-group',
            _queueProcessor: queueProcessor,
        };
    }

    const entry = { topic: 'test-topic', partition: 0 };

    describe('with OTEL enabled', () => {
        beforeEach(() => { process.env.ENABLE_OTEL = 'true'; });

        it('runs the queue processor and forwards the result to done', done => {
            const self = makeSelf((e, cb) => cb(null, { ok: true }));
            BackbeatConsumer.prototype._processTask.call(self, entry, (err, args) => {
                assert.ifError(err);
                assert.deepStrictEqual(args, { ok: true });
                done();
            });
        });

        it('forwards the error when the queue processor fails', done => {
            const self = makeSelf((e, cb) => cb(new Error('fail')));
            BackbeatConsumer.prototype._processTask.call(self, entry, err => {
                assert(err && err.message === 'fail');
                done();
            });
        });

        it('ends the span and rethrows on a synchronous throw', () => {
            const self = makeSelf(() => { throw new Error('boom'); });
            assert.throws(
                () => BackbeatConsumer.prototype._processTask.call(self, entry, () => {}),
                /boom/);
        });
    });

    describe('with OTEL disabled', () => {
        beforeEach(() => { delete process.env.ENABLE_OTEL; });

        it('runs the queue processor with no span machinery', done => {
            const self = makeSelf((e, cb) => cb(null, { ok: true }));
            BackbeatConsumer.prototype._processTask.call(self, entry, (err, args) => {
                assert.ifError(err);
                assert.deepStrictEqual(args, { ok: true });
                done();
            });
        });
    });
});

describe('BackbeatConsumer subscription state', () => {
    const proto = BackbeatConsumer.prototype;

    function makeSelf(subscription) {
        return {
            _topic: 'test-topic',
            _groupId: 'test-group',
            _log: { debug: () => {}, error: () => {} },
            _consumer: { subscription },
            _getSubscription: proto._getSubscription,
            isPaused: proto.isPaused,
        };
    }

    it('should report paused when the consumer has no subscription', () => {
        const self = makeSelf(() => []);
        assert.strictEqual(proto.isPaused.call(self), true);
        assert.strictEqual(proto.getServiceStatus.call(self), false);
    });

    it('should report active when the consumer is subscribed', () => {
        const self = makeSelf(() => ['test-topic']);
        assert.strictEqual(proto.isPaused.call(self), false);
        assert.strictEqual(proto.getServiceStatus.call(self), true);
    });

    it('should report paused rather than throw when subscription() fails', () => {
        // node-rdkafka throws ERR__STATE when the consumer is connected but
        // mid-unassign or closing
        const self = makeSelf(() => { throw new Error('Local: Erroneous state'); });
        assert.strictEqual(proto.isPaused.call(self), true);
        assert.strictEqual(proto.getServiceStatus.call(self), false);
    });

    it('should not throw out of onEntryCommittable when subscription() fails', () => {
        const self = makeSelf(() => { throw new Error('Local: Erroneous state'); });
        self._offsetLedger = {
            onOffsetProcessed: () => 42,
            toString: () => '',
        };
        self._consumer.isConnected = () => true;
        self._consumer.offsetsStore =
            () => assert.fail('offsetsStore must not be called while unavailable');

        assert.doesNotThrow(() => proto.onEntryCommittable.call(self,
            { topic: 'test-topic', partition: 0, offset: 42 }));
    });

    it('should subscribe on resume when the subscription is unavailable', () => {
        const self = makeSelf(() => { throw new Error('Local: Erroneous state'); });
        let subscribed = null;
        self._consumer.subscribe = topics => { subscribed = topics; };

        assert.doesNotThrow(() => proto.resume.call(self, 'test-site'));
        assert.deepStrictEqual(subscribed, ['test-topic']);
    });

    it('should not subscribe on resume when already subscribed', () => {
        const self = makeSelf(() => ['test-topic']);
        self._consumer.subscribe = () => assert.fail('should not re-subscribe');

        assert.doesNotThrow(() => proto.resume.call(self, 'test-site'));
    });
});
