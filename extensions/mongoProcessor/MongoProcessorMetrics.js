const { ZenkoMetrics } = require('arsenal').metrics;

const { promMetricNames } = require('./constants');

const ingestionProcessedElapsedSeconds = ZenkoMetrics.createHistogram({
    name: promMetricNames.ingestionProcessedElapsedSeconds,
    help: 'Duration of the process ingestion jobs in seconds',
    buckets: promMetricNames.ingestionBucketTimes,
    labelNames: ['status'],
});

const ingestionKafkaPulledTotal = ZenkoMetrics.createCounter({
    name: promMetricNames.ingestionKafkaPulledTotal,
    help: 'Number of kafka entries pulled for ingestion',
});

class MongoProcessorMetrics {
    static onProcessKafkaEntry() {
        ingestionKafkaPulledTotal.inc();
    }

    static onIngestionProcessed(elapsedMs, status) {
        ingestionProcessedElapsedSeconds.observe({
            status,
        }, elapsedMs / 1000);
    }
}

module.exports = MongoProcessorMetrics;
