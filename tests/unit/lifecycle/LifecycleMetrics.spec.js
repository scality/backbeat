const assert = require('assert');
const sinon = require('sinon');
const { LifecycleMetrics } = require('../../../extensions/lifecycle/LifecycleMetrics');
const { ZenkoMetrics } = require('arsenal').metrics;

describe('LifecycleMetrics', () => {
    let log;

    beforeEach(() => {
        log = {
            error: sinon.stub(),
        };
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('error handling', () => {
        it('should catch errors in onProcessBuckets', () => {
            const metric = ZenkoMetrics.getMetric('s3_lifecycle_latest_batch_start_time');
            sinon.stub(metric, 'set').throws(new Error('Metric error'));

            LifecycleMetrics.onProcessBuckets(log);

            assert(log.error.calledOnce);
            assert(log.error.calledWithMatch('failed to update prometheus metrics', {
                method: 'LifecycleMetrics.onProcessBuckets',
            }));
        });

        it('should catch errors in onBucketListing', () => {
            const metric = ZenkoMetrics.getMetric('s3_lifecycle_conductor_bucket_list_success_total');
            sinon.stub(metric, 'inc').throws(new Error('Metric error'));

            LifecycleMetrics.onBucketListing(log, null);

            assert(log.error.calledOnce);
            assert(log.error.calledWithMatch('failed to update prometheus metrics', {
                method: 'LifecycleMetrics.onBucketListing',
            }));
        });

        it('should catch errors in onActiveIndexingJobsFailed', () => {
            const metric = ZenkoMetrics.getMetric('s3_lifecycle_active_indexing_jobs');
            sinon.stub(metric, 'reset').throws(new Error('Metric error'));

            LifecycleMetrics.onActiveIndexingJobsFailed(log);

            assert(log.error.calledOnce);
            assert(log.error.calledWithMatch('failed to update prometheus metrics', {
                method: 'LifecycleMetrics.onActiveIndexingJobsFailed',
            }));
        });

        it('should catch errors in onActiveIndexingJobs', () => {
            const metric = ZenkoMetrics.getMetric('s3_lifecycle_active_indexing_jobs');
            sinon.stub(metric, 'set').throws(new Error('Metric error'));

            LifecycleMetrics.onActiveIndexingJobs(log, 12);

            assert(log.error.calledOnce);
            assert(log.error.calledWithMatch('failed to update prometheus metrics', {
                method: 'LifecycleMetrics.onActiveIndexingJobs',
                count: 12,
            }));
        });

        it('should catch errors in onLegacyTask', () => {
            const metric = ZenkoMetrics.getMetric('s3_lifecycle_legacy_tasks_total');
            sinon.stub(metric, 'inc').throws(new Error('Metric error'));

            LifecycleMetrics.onLegacyTask(log, 'success');

            assert(log.error.calledOnce);
            assert(log.error.calledWithMatch('failed to update prometheus metrics', {
                method: 'LifecycleMetrics.onLegacyTask',
                status: 'success',
            }));
        });

        it('should catch errors in onLifecycleTriggered', () => {
            LifecycleMetrics.onLifecycleTriggered(log, 'conductor', 'expiration', 'us-east-1', NaN);

            assert(log.error.calledOnce);
            assert(log.error.calledWithMatch('failed to update prometheus metrics', {
                method: 'LifecycleMetrics.onLifecycleTriggered',
                process: 'conductor',
                type: 'expiration',
                location: 'us-east-1',
                latencyMs: NaN,
            }));
        });

        it('should catch errors in onLifecycleStarted', () => {
            LifecycleMetrics.onLifecycleStarted(log, 'transition', 'us-west-2', NaN);

            assert(log.error.calledOnce);
            assert(log.error.calledWithMatch('failed to update prometheus metrics', {
                method: 'LifecycleMetrics.onLifecycleStarted',
                type: 'transition',
                location: 'us-west-2',
                durationMs: NaN,
            }));
        });

        it('should catch errors in onLifecycleCompleted', () => {
            const metric = ZenkoMetrics.getMetric('s3_lifecycle_duration_seconds');
            sinon.stub(metric, 'observe').throws(new Error('Metric error'));

            LifecycleMetrics.onLifecycleCompleted(log, 'expiration', 'eu-west-1', NaN);

            assert(log.error.calledOnce);
            assert(log.error.calledWithMatch('failed to update prometheus metrics', {
                method: 'LifecycleMetrics.onLifecycleCompleted',
            }));
        });

        it('should catch errors in onS3Request', () => {
            const metric = ZenkoMetrics.getMetric('s3_lifecycle_s3_operations_total');
            sinon.stub(metric, 'inc').throws(new Error('Metric error'));

            LifecycleMetrics.onS3Request(log, 'deleteObject', 'processor', null);

            assert(log.error.calledOnce);
            assert(log.error.calledWithMatch('failed to update prometheus metrics', {
                method: 'LifecycleMetrics.onS3Request',
                op: 'deleteObject',
                process: 'processor',
                statusCode: '200',
            }));
        });

        it('should pass err.statusCode in onS3Request', () => {
            const metric = ZenkoMetrics.getMetric('s3_lifecycle_s3_operations_total');
            const incStub = sinon.stub(metric, 'inc');

            const fakeError = new Error('S3 Error');
            fakeError.statusCode = '503';

            LifecycleMetrics.onS3Request(log, 'putObject', 'conductor', fakeError);

            assert(incStub.calledOnce);
            assert(incStub.calledWithMatch({ op: 'putObject', origin: 'conductor', status: '503' }));
            assert(log.error.notCalled);
        });

        it('should pass 200 in onS3Request when no error', () => {
            const metric = ZenkoMetrics.getMetric('s3_lifecycle_s3_operations_total');
            const incStub = sinon.stub(metric, 'inc');

            LifecycleMetrics.onS3Request(log, 'putObject', 'conductor', null);

            assert(incStub.calledOnce);
            assert(incStub.calledWithMatch({ op: 'putObject', origin: 'conductor', status: '200' }));
            assert(log.error.notCalled);
        });

        it('should catch errors in onKafkaPublish with NaN count', () => {
            const metric = ZenkoMetrics.getMetric('s3_lifecycle_kafka_publish_success_total');
            sinon.stub(metric, 'inc').throws(new Error('Invalid value'));

            LifecycleMetrics.onKafkaPublish(log, 'publish', 'conductor', null, 1);

            assert(log.error.calledOnce);
            assert(log.error.calledWithMatch('failed to update prometheus metrics', {
                method: 'LifecycleMetrics.onKafkaPublish',
                op: 'publish',
                process: 'conductor',
            }));
        });
    });
});
