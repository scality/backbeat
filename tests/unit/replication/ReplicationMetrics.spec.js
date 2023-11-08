const assert = require('assert');

const { ZenkoMetrics } = require('arsenal').metrics;
const ReplicationMetrics =
      require('../../../extensions/replication/ReplicationMetrics');
const { promMetricNames } =
      require('../../../extensions/replication/constants');

describe('ReplicationMetrics', () => {
    let totalMetric;
    let queuedBytesMetric;
    let processedBytesMetric;
    let elapsedMetric;

    beforeEach(() => {
        totalMetric = ZenkoMetrics.getMetric(
            promMetricNames.replicationQueuedTotal);

        queuedBytesMetric = ZenkoMetrics.getMetric(
            promMetricNames.replicationQueuedBytes);

        processedBytesMetric = ZenkoMetrics.getMetric(
            promMetricNames.replicationProcessedBytes);

        elapsedMetric = ZenkoMetrics.getMetric(
                promMetricNames.replicationElapsedSeconds);

        // reset counter not to be altered by other tests
        totalMetric.reset();
        queuedBytesMetric.reset();
        processedBytesMetric.reset();
        elapsedMetric.reset();
    });

    afterEach(() => {
        // reset counter not to alter other tests
        totalMetric.reset();
        queuedBytesMetric.reset();
        processedBytesMetric.reset();
        elapsedMetric.reset();
    });

    [123456, '123456'].forEach(contentLength => {
        it('should maintain replication queuing metrics with ' +
        `content-length as ${typeof contentLength}`, async () => {
            ReplicationMetrics.onReplicationQueued(
                'testOrigin', 'fromLoc', 'toLoc', contentLength, 2);

            const totalValues = (await totalMetric.get()).values;
            // only one metric value exists because we published with one
            // distinct label set
            assert.strictEqual(totalValues.length, 1);
            const totalValue = totalValues[0];
            assert.strictEqual(totalValue.value, 1);
            assert.deepStrictEqual(totalValue.labels, {
                origin: 'testOrigin',
                fromLocation: 'fromLoc',
                fromLocationType: 'local',
                toLocation: 'toLoc',
                toLocationType: 'local',
                partition: 2,
            });

            const bytesValues = (await queuedBytesMetric.get()).values;
            // only one metric value exists because we published with one
            // distinct label set
            assert.strictEqual(bytesValues.length, 1);
            const bytesValue = bytesValues[0];
            assert.strictEqual(bytesValue.value, 123456);
            assert.deepStrictEqual(bytesValue.labels, {
                origin: 'testOrigin',
                fromLocation: 'fromLoc',
                fromLocationType: 'local',
                toLocation: 'toLoc',
                toLocationType: 'local',
                partition: 2,
            });
        });
    });

    [12345678, '12345678'].forEach(contentLength => {
        it('should maintain replication processed metrics with ' +
        `content-length as ${typeof contentLength}`, async () => {
            // Push a few "processed" metrics
            // object of 123456 bytes processed successfully in 300ms
            ReplicationMetrics.onReplicationProcessed(
                'testOrigin', 'fromLoc', 'toLoc', 123456,
                'success', 300);
            // object of 12345678 bytes processed successfully in 2s
            ReplicationMetrics.onReplicationProcessed(
                'testOrigin', 'fromLoc', 'toLoc', contentLength,
                'success', 2000);
            // object of 12345678 bytes processed with error in 5s
            ReplicationMetrics.onReplicationProcessed(
                'testOrigin', 'fromLoc', 'toLoc', contentLength,
                'error', 5000);

            // Check that the byte count is accurate
            const bytesValues = (await processedBytesMetric.get()).values;

            // only one metric value exists because we published with one
            // distinct label set
            assert.strictEqual(bytesValues.length, 2);
            const successBytes = bytesValues.find(
                value => value.labels.status === 'success');
            const errorBytes = bytesValues.find(
                value => value.labels.status === 'error');
            assert.strictEqual(successBytes.value, 12469134);
            assert.deepStrictEqual(successBytes.labels, {
                origin: 'testOrigin',
                fromLocation: 'fromLoc',
                fromLocationType: 'local',
                toLocation: 'toLoc',
                toLocationType: 'local',
                status: 'success',
            });
            assert.strictEqual(errorBytes.value, 12345678);
            assert.deepStrictEqual(errorBytes.labels, {
                origin: 'testOrigin',
                fromLocation: 'fromLoc',
                fromLocationType: 'local',
                toLocation: 'toLoc',
                toLocationType: 'local',
                status: 'error',
            });

            // Check that the elapsed time histogram is accurate.
            //
            // Focusing on the metric labeled as "success" and "10MB..30MB"
            // range, we have pushed one metric in this category with 2
            // seconds of elapsed time, check that time buckets reflect
            // this. We will not check the other values pushed to keep the
            // test short, as they basically share the same logic.

            const elapsedValues = (await elapsedMetric.get()).values;

            // check that all histogram values which "less-or-equal" timing
            // criteria is below 2 seconds for this size range is 0 (as no
            // metric was pushed with timing less than 2 seconds)
            const leBelowTwoSeconds10M = elapsedValues.filter(
                value => (value.labels.status === 'success' &&
                          value.labels.contentLengthRange === '10MB..30MB' &&
                          value.labels.le < 2));
            assert(leBelowTwoSeconds10M.length > 0);
            assert(leBelowTwoSeconds10M.every(value => value.value === 0));

            // check that all histogram values which "less-or-equal" timing
            // criteria is above 2 seconds for this size range is 1 (as we
            // pushed a metric in this size range with 2 seconds elapsed)
            const leAboveTwoSeconds10M = elapsedValues.filter(
                value => (value.labels.status === 'success' &&
                          value.labels.contentLengthRange === '10MB..30MB' &&
                          value.labels.le > 2));
            assert(leAboveTwoSeconds10M.length > 0);
            assert(leAboveTwoSeconds10M.every(value => value.value === 1));
        });
    });
});
