const assert = require('assert');
const sinon = require('sinon');
const {
    LifecycleMetrics,
    resetLifecycleScanMetricCleanupTimers,
} = require('../../../extensions/lifecycle/LifecycleMetrics');
const { ZenkoMetrics } = require('arsenal').metrics;

describe('LifecycleMetrics', () => {
    let log;

    beforeEach(() => {
        log = {
            error: sinon.stub(),
        };
    });

    afterEach(() => {
        resetLifecycleScanMetricCleanupTimers();
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

        it('should catch errors in onConductorScanComplete', () => {
            const metric = ZenkoMetrics.getMetric('s3_lifecycle_latest_batch_end_time');
            sinon.stub(metric, 'set').throws(new Error('Metric error'));

            LifecycleMetrics.onConductorScanComplete(log, 10);

            assert(log.error.calledOnce);
            assert(log.error.calledWithMatch('failed to update prometheus metrics', {
                method: 'LifecycleMetrics.onConductorScanComplete',
                bucketCount: 10,
            }));
        });

        it('should increment bucket processor messages received counter by scan id', () => {
            const messagesMetric = ZenkoMetrics.getMetric(
                's3_lifecycle_bucket_processor_scan_messages_total');
            const messagesInc = sinon.stub(messagesMetric, 'inc');

            LifecycleMetrics.onBucketProcessorScanMessageReceived(log, 'scan-A');
            assert(messagesInc.calledOnce);
            assert(messagesInc.calledWithMatch({
                origin: 'bucket_processor',
                ['conductor_scan_id']: 'scan-A',
            }));

            assert(log.error.notCalled);
        });

        it('should observe bucket processor scan message age from conductor start timestamp', () => {
            const messageAgeMetric = ZenkoMetrics.getMetric(
                's3_lifecycle_bucket_processor_scan_message_age_seconds');
            const observeStub = sinon.stub(messageAgeMetric, 'observe');
            sinon.stub(ZenkoMetrics.getMetric(
                's3_lifecycle_bucket_processor_scan_messages_total'), 'inc');
            sinon.useFakeTimers(1700000010000);

            LifecycleMetrics.onBucketProcessorScanMessageReceived(
                log, 'scan-A', 1700000000000);

            assert(observeStub.calledOnce);
            assert(observeStub.calledWithMatch(
                { origin: 'bucket_processor' }, 10));
            assert(log.error.notCalled);
        });

        it('should skip bucket processor scan message age observation on negative age', () => {
            const messageAgeMetric = ZenkoMetrics.getMetric(
                's3_lifecycle_bucket_processor_scan_message_age_seconds');
            const observeStub = sinon.stub(messageAgeMetric, 'observe');
            sinon.stub(ZenkoMetrics.getMetric(
                's3_lifecycle_bucket_processor_scan_messages_total'), 'inc');
            sinon.useFakeTimers(1700000000000);

            LifecycleMetrics.onBucketProcessorScanMessageReceived(
                log, 'scan-A', 1700000001000);

            assert(observeStub.notCalled);
            assert(log.error.notCalled);
        });

        it('should catch errors in onBucketProcessorScanMessageReceived', () => {
            const messagesMetric = ZenkoMetrics.getMetric(
                's3_lifecycle_bucket_processor_scan_messages_total');
            sinon.stub(messagesMetric, 'inc').throws(new Error('Metric error'));

            LifecycleMetrics.onBucketProcessorScanMessageReceived(log, 'scan-A');

            assert(log.error.calledOnce);
            assert(log.error.calledWithMatch('failed to update prometheus metrics', {
                method: 'LifecycleMetrics.onBucketProcessorScanMessageReceived',
                conductorScanId: 'scan-A',
            }));
        });

        it('should skip per-scan metrics when scan id is missing', () => {
            const messagesMetric = ZenkoMetrics.getMetric(
                's3_lifecycle_bucket_processor_scan_messages_total');
            const incStub = sinon.stub(messagesMetric, 'inc');

            LifecycleMetrics.onBucketProcessorScanMessageReceived(log, undefined);
            LifecycleMetrics.onBucketProcessorScanMessageReceived(log, '');

            assert(incStub.notCalled);
            assert(log.error.notCalled);
        });

        it('should remove stale bucket processor scan series after retention', () => {
            const messagesMetric = ZenkoMetrics.getMetric(
                's3_lifecycle_bucket_processor_scan_messages_total');
            const removeStub = sinon.stub(messagesMetric, 'remove');
            sinon.stub(messagesMetric, 'inc');
            const clock = sinon.useFakeTimers(1700000000000);

            LifecycleMetrics.onBucketProcessorScanMessageReceived(log, 'scan-A');
            clock.tick(24 * 60 * 60 * 1000 + 1);

            assert(removeStub.calledOnce);
            assert(removeStub.calledWithMatch({
                origin: 'bucket_processor',
                ['conductor_scan_id']: 'scan-A',
            }));
            assert(log.error.notCalled);
        });

        it('should reset bucket processor scan cleanup timer on update', () => {
            const messagesMetric = ZenkoMetrics.getMetric(
                's3_lifecycle_bucket_processor_scan_messages_total');
            const removeStub = sinon.stub(messagesMetric, 'remove');
            sinon.stub(messagesMetric, 'inc');
            const clock = sinon.useFakeTimers(1700000000000);

            LifecycleMetrics.onBucketProcessorScanMessageReceived(log, 'scan-A');
            clock.tick(12 * 60 * 60 * 1000);
            LifecycleMetrics.onBucketProcessorScanMessageReceived(log, 'scan-A');
            clock.tick(12 * 60 * 60 * 1000 + 1);

            assert(removeStub.notCalled);
            clock.tick(12 * 60 * 60 * 1000);

            assert(removeStub.calledOnce);
            assert(removeStub.calledWithMatch({
                origin: 'bucket_processor',
                ['conductor_scan_id']: 'scan-A',
            }));
            assert(log.error.notCalled);
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

        it('should set latest batch bucket count metric', () => {
            const endMetric = ZenkoMetrics.getMetric(
                's3_lifecycle_latest_batch_end_time');
            const countMetric = ZenkoMetrics.getMetric(
                's3_lifecycle_latest_batch_bucket_count');
            const endSet = sinon.stub(endMetric, 'set');
            const countSet = sinon.stub(countMetric, 'set');

            LifecycleMetrics.onConductorScanComplete(log, 7);

            assert(endSet.calledOnce);
            assert(endSet.calledWithMatch(
                { origin: 'conductor' }));
            assert.strictEqual(countSet.callCount, 1);
            assert(countSet.getCall(0).calledWithMatch(
                { origin: 'conductor' }, 7));
            assert(log.error.notCalled);
        });

        it('should set latest batch start time', () => {
            const latestStartSet = sinon.stub(ZenkoMetrics.getMetric(
                's3_lifecycle_latest_batch_start_time'), 'set');

            LifecycleMetrics.onProcessBuckets(log, 1700000000000);

            assert(latestStartSet.calledWithMatch(
                { origin: 'conductor' }, 1700000000000));
            assert(log.error.notCalled);
        });

    });
});
