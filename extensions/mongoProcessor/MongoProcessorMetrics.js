const { ZenkoMetrics } = require('arsenal').metrics;

const config = require('../../lib/Config');
const { promMetricNames } = require('./constants');

const ingestionProcessedElapsedSeconds = ZenkoMetrics.createHistogram({
    name: promMetricNames.ingestionProcessedElapsedSeconds,
    help: 'Ingestion jobs elapsed time in seconds',
    buckets: promMetricNames.ingestionBucketTimes,
    labelNames: ['bucketName', 'locationName', 'locationType', 'status'],
});

let ingestionBuckets = config.getIngestionBuckets();
config.on('ingestion-buckets-update', () => {
    ingestionBuckets = config.getIngestionBuckets();
});


function _getLocationType(bucketName) {
    const locationInfo = ingestionBuckets
          .find(bucket => bucket.zenkoBucket === bucketName);
    return (locationInfo && locationInfo.locationType) || 'local';
}

class MongoProcessorMetrics {
    static onIngestionProcessed(locationName, bucketName, elapsedMs, status) {
        const locationType = _getLocationType(bucketName);

        ingestionProcessedElapsedSeconds.observe({
            bucketName,
            locationName,
            locationType,
            status,
        }, elapsedMs / 1000);
    }
}

module.exports = MongoProcessorMetrics;
