const assert = require('assert');
const sinon = require('sinon');
const config = require('../../config.json');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const LifecycleObjectTransitionProcessor =
    require('../../../extensions/lifecycle/objectProcessor/LifecycleObjectTransitionProcessor');
const LifecycleColdStatusArchiveTask =
    require('../../../extensions/lifecycle/tasks/LifecycleColdStatusArchiveTask');
const { LifecycleMetrics } =
    require('../../../extensions/lifecycle/LifecycleMetrics');

describe('LifecycleObjectTransitionProcessor', () => {
    let objectProcessor;

    beforeEach(() => {
        objectProcessor = new LifecycleObjectTransitionProcessor(
            config.zookeeper,
            config.kafka,
            config.extensions.lifecycle,
            config.s3,
        );
    });

    it('should contain transition tasks topic in consumer params', () => {
        const consumerParams = objectProcessor.getConsumerParams();
        assert.deepStrictEqual(Object.keys(consumerParams), [config.extensions.lifecycle.transitionTasksTopic]);
        assert.strictEqual(
            consumerParams[config.extensions.lifecycle.transitionTasksTopic].topic,
            config.extensions.lifecycle.transitionTasksTopic
        );
    });

    it('should set up gcProducer when start is called', done => {
        objectProcessor.start(err => {
            assert.ifError(err);
            assert(objectProcessor._gcProducer, 'gcProducer should be set');
            done();
        });
    });

    describe('processColdStorageStatusEntry failure metric', () => {
        const kafkaEntry = {
            topic: `${config.extensions.lifecycle.coldStorageStatusTopicPrefix}glacier`,
            value: Buffer.from(JSON.stringify({
                op: 'archive',
                bucketName: 'testBucket',
                objectKey: 'testObj',
                objectVersion: 'testversion',
                accountId: '834789881858',
                archiveInfo: { archiveId: 'x', archiveVersion: 1 },
                requestId: 'r1',
            })),
        };

        beforeEach(() => {
            // Fast retry config so the test doesn't burn seconds on backoff.
            objectProcessor.retryWrapper = new BackbeatTask({
                maxRetries: 1,
                backoff: { min: 10, max: 20, factor: 1, jitter: 0 },
            });
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should fire onLifecycleFailed when the task exhausts retries', done => {
            const err = new Error('backend outage');
            err.retryable = true;
            sinon.stub(LifecycleColdStatusArchiveTask.prototype, 'processEntry').yields(err);
            const spy = sinon.spy(LifecycleMetrics, 'onLifecycleFailed');

            objectProcessor.processColdStorageStatusEntry(kafkaEntry, cbErr => {
                assert.strictEqual(cbErr, err);
                assert.strictEqual(spy.callCount, 1);
                assert.deepStrictEqual(spy.firstCall.args.slice(1),
                    ['transition-processor', 'archive', 'glacier']);
                done();
            });
        });

        it('should fire onLifecycleFailed when the task returns a non-retryable error', done => {
            const err = new Error('bad state');
            // No err.retryable set -> shouldRetryFunc returns undefined -> immediate give-up
            sinon.stub(LifecycleColdStatusArchiveTask.prototype, 'processEntry').yields(err);
            const spy = sinon.spy(LifecycleMetrics, 'onLifecycleFailed');

            objectProcessor.processColdStorageStatusEntry(kafkaEntry, cbErr => {
                assert.strictEqual(cbErr, err);
                assert.strictEqual(spy.callCount, 1);
                assert.deepStrictEqual(spy.firstCall.args.slice(1),
                    ['transition-processor', 'archive', 'glacier']);
                done();
            });
        });

        it('should not fire onLifecycleFailed when the task succeeds', done => {
            sinon.stub(LifecycleColdStatusArchiveTask.prototype, 'processEntry').yields();
            const spy = sinon.spy(LifecycleMetrics, 'onLifecycleFailed');

            objectProcessor.processColdStorageStatusEntry(kafkaEntry, cbErr => {
                assert.ifError(cbErr);
                assert.strictEqual(spy.callCount, 0);
                done();
            });
        });
    });
});
