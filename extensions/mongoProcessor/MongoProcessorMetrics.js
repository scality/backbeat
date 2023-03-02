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
    help: 'Total number of kafka entries pulled for ingestion',
});

const ingestionKafkaConsume = ZenkoMetrics.createCounter({
    name: promMetricNames.ingestionKafkaConsume,
    help: 'Total number of operations by the ingestion processor kafka consumer',
    labelNames: ['origin', 'status'],
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

    static onIngestionKafkaConsume(status) {
        ingestionKafkaConsume.inc({
            origin: 'ingestion',
            status,
        });
    }
}

module.exports = MongoProcessorMetrics;
