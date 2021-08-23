const { ZenkoMetrics } = require('arsenal').metrics;

const config = require('../../lib/Config');
const { promMetricNames } = require('./constants');

const ingestionQueuedTotal = ZenkoMetrics.createCounter({
    name: promMetricNames.ingestionQueuedTotal,
    help: 'Number of objects queued for ingestion',
    labelNames: ['topic'],
});

const ingestionQueuedBytes = ZenkoMetrics.createCounter({
    name: promMetricNames.ingestionQueuedBytes,
    help: 'Number of bytes queued for ingestion',
    labelNames: ['bucketName', 'methodType'],
});

const ingestionProcessedBytes = ZenkoMetrics.createCounter({
    name: promMetricNames.ingestionProcessedBytes,
    help: 'Number of bytes ingested',
    labelNames: ['location', 'locationType'],
});

let bootstrapList = config.getBootstrapList();
config.on('bootstrap-list-update', () => {
    bootstrapList = config.getBootstrapList();
});

function _getReplicationEndpointType(location) {
    const replicationEndpoint = bootstrapList
          .find(endpoint => endpoint.site === location);
    return (replicationEndpoint && replicationEndpoint.type) || 'local';
}

class IngestionMetrics extends ZenkoMetrics {
    static onIngestionQueued(topic, topicEntries) {
        topicEntries.forEach(e => {
            // TODO: add try catch
            const entry = JSON.parse(e.message);
            const bucketName = entry.bucket;
            // TODO: we might want location and location type instead of bucket name.
            const methodType = entry.type;
            ingestionQueuedTotal.inc({
                topic,
                methodType,
            });
            if (methodType === 'put') {
                const value = JSON.parse(entry.value);
                const contentLength = value['content-length'];

                ingestionQueuedBytes.inc({
                    topic,
                    bucketName,
                    methodType,
                }, Number.parseInt(contentLength, 10));
            }
        });
    }


    static onIngestionProcessed(location, contentLength) {
        const locationType = _getReplicationEndpointType(location);
        const contentLengthAsNumber = Number.parseInt(contentLength, 10);

        ingestionProcessedBytes.inc({
            location,
            locationType,
        }, contentLengthAsNumber);
    }
}

module.exports = IngestionMetrics;
