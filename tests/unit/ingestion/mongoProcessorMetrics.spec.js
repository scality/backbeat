const assert = require('assert');

const { ZenkoMetrics } = require('arsenal').metrics;
const MongoProcessorMetrics =
      require('../../../extensions/mongoProcessor/MongoProcessorMetrics');
const { promMetricNames } =
      require('../../../extensions/mongoProcessor/constants');

describe('MongoProcessorMetrics', () => {
    it('onProcessKafkaEntry should increase value', async () => {
        const countMetric = ZenkoMetrics.getMetric(
            promMetricNames.ingestionKafkaPulledTotal);

        // before increase.
        const initialValues = (await countMetric.get()).values;
        assert.strictEqual(initialValues.length, 1);
        assert.strictEqual(initialValues[0].value, 0);

        MongoProcessorMetrics.onProcessKafkaEntry();

        // after increase.
        const totalValues = (await countMetric.get()).values;
        assert.strictEqual(totalValues.length, 1);
        assert.strictEqual(totalValues[0].value, 1);
        countMetric.reset();
    });

    it('onIngestionProcessed should observe values', async () => {
        const elapsedTimeinMS = 5000;
        const status = 'success';

        const elapsedMetric = ZenkoMetrics.getMetric(
            promMetricNames.ingestionProcessedElapsedSeconds);

        // before observe.
        const initialValues = (await elapsedMetric.get()).values;
        assert.strictEqual(initialValues.length, 0);

        MongoProcessorMetrics.onIngestionProcessed(elapsedTimeinMS, status);

        // after observe.
        const totalValues = (await elapsedMetric.get()).values;
        totalValues.forEach(v => {
            assert.strictEqual(v.labels.status, status);
        });
        assert.strictEqual(totalValues.length, 14);

        const bucketMetrics = totalValues.filter(v =>
            v.metricName === `${promMetricNames.ingestionProcessedElapsedSeconds}_bucket`);

        // It should return one metric value for each bucket defined (11) plus one for '+Inf'
        assert.strictEqual(bucketMetrics.length, 12);
        // "le" stands for less than or equal to.
        // So 0 processed ingestion took less than 5ms, 10ms, 25ms, 50ms, 100ms 250ms, 500ms, 750ms, 1s
        // So 1 processed ingestion took less than or equal to 5s, 10s and +Inf (biggest value)
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
