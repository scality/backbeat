const { ZenkoMetrics } = require('arsenal').metrics;

const config = require('../../lib/Config');
const { promMetricNames } = require('./constants');

const SIZE_BUCKETS = [
    { label: '<10KB', lt: 1e+4 },
    { label: '10KB..30KB', lt: 3e+4 },
    { label: '30KB..100KB', lt: 1e+5 },
    { label: '100KB..300KB', lt: 3e+5 },
    { label: '300KB..1MB', lt: 1e+6 },
    { label: '1MB..3MB', lt: 3e+6 },
    { label: '3MB..10MB', lt: 1e+7 },
    { label: '10MB..30MB', lt: 3e+7 },
    { label: '30MB..100MB', lt: 1e+8 },
    { label: '100MB..300MB', lt: 3e+8 },
    { label: '300MB..1GB', lt: 1e+9 },
    { label: '1GB..3GB', lt: 3e+9 },
    { label: '3GB..10GB', lt: 1e+10 },
    { label: '10GB..30GB', lt: 3e+10 },
    { label: '30GB..100GB', lt: 1e+11 },
    { label: '100GB..300GB', lt: 3e+11 },
    { label: '300GB..1TB', lt: 1e+12 },
    { label: '>1TB', lt: Infinity },
];

const TIME_BUCKETS = [0.03, 0.1, 0.3, 1, 3, 10, 30, 100, 300, 1000, 3000];

const replicationQueuedTotal = ZenkoMetrics.createCounter({
    name: promMetricNames.replicationQueuedTotal,
    help: 'Number of objects queued for replication',
    labelNames: ['origin', 'partition', 'fromLocation', 'fromLocationType',
                 'toLocation', 'toLocationType'],
});

const replicationQueuedBytes = ZenkoMetrics.createCounter({
    name: promMetricNames.replicationQueuedBytes,
    help: 'Number of bytes queued for replication',
    labelNames: ['origin', 'partition', 'fromLocation', 'fromLocationType',
                 'toLocation', 'toLocationType'],
});

const replicationProcessedBytes = ZenkoMetrics.createCounter({
    name: promMetricNames.replicationProcessedBytes,
    help: 'Number of bytes replicated',
    labelNames: ['origin', 'fromLocation', 'fromLocationType',
                 'toLocation', 'toLocationType', 'status'],
});

const replicationProcessedElapsedSeconds = ZenkoMetrics.createHistogram({
    name: promMetricNames.replicationElapsedSeconds,
    help: 'Replication jobs elapsed time in seconds',
    buckets: TIME_BUCKETS,
    labelNames: ['origin', 'fromLocation', 'fromLocationType',
                 'toLocation', 'toLocationType', 'status',
                 'contentLengthRange'],
});

let bootstrapList = config.getBootstrapList();
config.on('location-constraints-update', () => {
    bootstrapList = config.getBootstrapList();
});

/**
 * Get the type of this location (see mapping in
 * conf/Config.js:locationTypeMatch)
 *
 * @param {string} location - location name
 * @return {string} location type if set in config, or 'local'
 * otherwise (for non-replicated locations), this is a bit of a
 * shortcut but we can then set the fromLocationType field in metrics
 * to 'local' when reading from a non-cloud location.
 */
function _getReplicationEndpointType(location) {
    const replicationEndpoint = bootstrapList
          .find(endpoint => endpoint.site === location);
    return (replicationEndpoint && replicationEndpoint.type) || 'local';
}

class ReplicationMetrics extends ZenkoMetrics {
    static onReplicationQueued(originLabel, fromLocation, toLocation,
                               contentLength, partition) {
        const fromLocationType = _getReplicationEndpointType(fromLocation);
        const toLocationType = _getReplicationEndpointType(toLocation);

        replicationQueuedTotal.inc({
            origin: originLabel,
            fromLocation, fromLocationType,
            toLocation, toLocationType, partition,
        });

        replicationQueuedBytes.inc({
            origin: originLabel,
            fromLocation, fromLocationType,
            toLocation, toLocationType, partition,
        }, contentLength);
    }

    static onReplicationProcessed(originLabel, fromLocation, toLocation,
                                  contentLength, status, elapsedMs) {
        const fromLocationType = _getReplicationEndpointType(fromLocation);
        const toLocationType = _getReplicationEndpointType(toLocation);

        replicationProcessedBytes.inc({
            origin: originLabel,
            fromLocation, fromLocationType,
            toLocation, toLocationType, status,
        }, contentLength);

        const sizeBucket = SIZE_BUCKETS.find(
            bucket => contentLength < bucket.lt);
        replicationProcessedElapsedSeconds.observe({
            origin: originLabel,
            fromLocation, fromLocationType,
            toLocation, toLocationType,
            status,
            contentLengthRange: sizeBucket.label,
        }, elapsedMs / 1000);
    }
}

module.exports = ReplicationMetrics;
