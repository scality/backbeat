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
