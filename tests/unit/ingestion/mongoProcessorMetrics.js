const assert = require('assert');

const { ZenkoMetrics } = require('arsenal').metrics;
const MongoProcessorMetrics =
      require('../../../extensions/mongoProcessor/MongoProcessorMetrics');
const { promMetricNames } =
      require('../../../extensions/mongoProcessor/constants');

describe('IngestionMetrics', () => {
    describe('onIngestionProcessed', () => {
        it('should observe values', () => {
            const bucketname = 'bucketname';
            const locationName = 'loc-1';
            const elapsedTimeinMS = 5000;
            const status = 'success';
            MongoProcessorMetrics.onIngestionProcessed(
                locationName, bucketname, elapsedTimeinMS, status);

            const elapsedMetric = ZenkoMetrics.getMetric(
                promMetricNames.ingestionProcessedElapsedSeconds);

            const totalValues = elapsedMetric.get().values;
            totalValues.forEach(v => {
                assert.strictEqual(v.labels.bucketName, bucketname);
                assert.strictEqual(v.labels.locationName, locationName);
                assert.strictEqual(v.labels.locationType, 'local');
                assert.strictEqual(v.labels.status, status);
            });
            assert.strictEqual(totalValues.length, 14);

            const bucketMetrics = totalValues.filter(v =>
                v.metricName === `${promMetricNames.ingestionProcessedElapsedSeconds}_bucket`);

            // It should return one metric value for each bucket defined (11) plus one for '+Inf'
            assert.strictEqual(bucketMetrics.length, 12);
            // "le" stands for less than or equal to.
            // So 0 processed ingestion took less than 5ms, 10ms, 25ms, 50ms, 100ms 250ms, 500ms, 750ms, 1s
            // So 1 processed ingestion took less than or equal to 5s, 10s ans +Inf (biggest value)
            bucketMetrics.forEach(v => {
                if (v.labels.le >= (elapsedTimeinMS / 1000) || v.labels.le === '+Inf') {
                    assert.strictEqual(v.value, 1, `value for ${v.labels.le} is invalid`);
                } else {
                    assert.strictEqual(v.value, 0, `value for ${v.labels.le} is invalid`);
                }
            });

            // The sum of observations
            const sumMetrics = totalValues.filter(v =>
                v.metricName === `${promMetricNames.ingestionProcessedElapsedSeconds}_sum`);
            assert.strictEqual(sumMetrics.length, 1);
            assert.strictEqual(sumMetrics[0].value, 5);

            // The number of observations
            const countMetrics = totalValues.filter(v =>
                v.metricName === `${promMetricNames.ingestionProcessedElapsedSeconds}_count`);
            assert.strictEqual(countMetrics.length, 1);
            assert.strictEqual(countMetrics[0].value, 1);

            elapsedMetric.reset();
        });
    });
});
