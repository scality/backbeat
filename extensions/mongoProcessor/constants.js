'use strict'; // eslint-disable-line


const constants = {
    promMetricNames: {
        ingestionProcessedElapsedSeconds: 'zenko_ingestion_mongo_processed_elapsed_seconds',
        ingestionKafkaPulledTotal: 'zenko_ingestion_kafka_message_pulled_total',
        ingestionBucketTimes: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 5, 10]
    },
};

module.exports = constants;
