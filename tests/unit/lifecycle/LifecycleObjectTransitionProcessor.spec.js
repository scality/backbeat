const assert = require('assert');
const sinon = require('sinon');
const { errors } = require('arsenal');
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

    it('should consume the transition tasks topic from the earliest offset', () => {
        const consumerParams = objectProcessor.getConsumerParams();
        assert.strictEqual(
            consumerParams[config.extensions.lifecycle.transitionTasksTopic].fromOffset,
            'earliest'
        );
    });

    it('should consume the cold status topics from the earliest offset', () => {
        const coldTopic = `${config.extensions.lifecycle.coldStorageStatusTopicPrefix}location-dmf-v1`;
        const coldProcessor = new LifecycleObjectTransitionProcessor(
            config.zookeeper,
            config.kafka,
            { ...config.extensions.lifecycle, coldStorageTopics: [coldTopic] },
            config.s3,
        );
        const consumerParams = coldProcessor.getConsumerParams();
        assert.strictEqual(consumerParams[coldTopic].fromOffset, 'earliest');
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

    describe('getAccountId', () => {
        const ownerId = 'canonical-id-1';
        const accountId = '834789881858';
        let processor;
        let log;

        beforeEach(() => {
            processor = new LifecycleObjectTransitionProcessor(
                config.zookeeper,
                config.kafka,
                {
                    ...config.extensions.lifecycle,
                    transitionProcessor: {
                        ...config.extensions.lifecycle.transitionProcessor,
                        auth: { type: 'assumeRole', roleName: 'role' },
                    },
                },
                config.s3,
            );
            log = { debug: () => {}, error: () => {} };
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should skip the lookup when auth type is not assume role', done => {
            const spy = sinon.spy(objectProcessor.vaultClientWrapper, 'getAccountId');
            objectProcessor.getAccountId(ownerId, log, (err, id) => {
                assert.ifError(err);
                assert.strictEqual(id, undefined);
                assert.strictEqual(spy.callCount, 0);
                done();
            });
        });

        it('should resolve through vault and cache the result', done => {
            const stub = sinon.stub(processor.vaultClientWrapper, 'getAccountId')
                .yields(null, accountId);

            processor.getAccountId(ownerId, log, (err, id) => {
                assert.ifError(err);
                assert.strictEqual(id, accountId);
                assert.strictEqual(stub.callCount, 1);

                processor.getAccountId(ownerId, log, (err2, id2) => {
                    assert.ifError(err2);
                    assert.strictEqual(id2, accountId);
                    assert.strictEqual(stub.callCount, 1);
                    done();
                });
            });
        });

        it('should fail on a cached miss instead of returning no account id', done => {
            const stub = sinon.stub(processor.vaultClientWrapper, 'getAccountId')
                .yields(errors.NoSuchEntity);

            processor.getAccountId(ownerId, log, err => {
                assert(err.NoSuchEntity);
                assert.strictEqual(stub.callCount, 1);

                // the miss is cached, but must still surface as an error
                processor.getAccountId(ownerId, log, (err2, id2) => {
                    assert(err2.NoSuchEntity);
                    assert.strictEqual(id2, undefined);
                    assert.strictEqual(stub.callCount, 1);
                    done();
                });
            });
        });

        it('should propagate other vault errors without caching them', done => {
            const stub = sinon.stub(processor.vaultClientWrapper, 'getAccountId')
                .yields(errors.InternalError);

            processor.getAccountId(ownerId, log, err => {
                assert(err.InternalError);

                processor.getAccountId(ownerId, log, err2 => {
                    assert(err2.InternalError);
                    assert.strictEqual(stub.callCount, 2);
                    done();
                });
            });
        });
    });
});
