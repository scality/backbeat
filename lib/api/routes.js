/*
    This file contains Backbeat API routes and route details
*/

/**
 * The metrics route model.
 * @param {Object} locations - Locations by service
 * @param {Array} locations.crr - The list of replication location names
 * @param {Array} locations.ingestion - The list of ingestion location names
 * @param {Array} locations.lifecycle - The list of lifecycle location names
 * @return {Array} The array of route objects
 */
function routes(locations) {
    /* eslint-disable no-param-reassign */
    locations.crr = locations.crr || [];
    locations.ingestion = locations.ingestion || [];
    locations.lifecycle = locations.lifecycle || [];
    /* eslint-enable no-param-reassign */

    return [
        // Route: /_/healthcheck
        {
            httpMethod: 'GET',
            category: 'healthcheck',
            type: 'basic',
            method: 'getHealthcheck',
            extensions: {},
        },
        // Route: /_/metrics/crr/<location>/pending
        // Route: /_/metrics/ingestion/<location>/pending
        {
            httpMethod: 'GET',
            category: 'metrics',
            type: 'pending',
            extensions: {
                crr: [...locations.crr, 'all'],
                ingestion: [...locations.ingestion, 'all'],
            },
            method: 'getPending',
            dataPoints: ['opsPending', 'bytesPending'],
        },
        // Route: /_/metrics/crr/<location>/backlog
        {
            httpMethod: 'GET',
            category: 'metrics',
            type: 'backlog',
            extensions: { crr: [...locations.crr, 'all'] },
            method: 'getBacklog',
            dataPoints: ['opsPending', 'bytesPending'],
        },
        // Route: /_/metrics/crr/<location>/completions
        // Route: /_/metrics/ingestion/<location>/completions
        {
            httpMethod: 'GET',
            category: 'metrics',
            type: 'completions',
            extensions: {
                crr: [...locations.crr, 'all'],
                ingestion: [...locations.ingestion, 'all'],
            },
            method: 'getCompletions',
            dataPoints: ['opsDone', 'bytesDone'],
        },
        // Route: /_/metrics/crr/<location>/failures
        {
            httpMethod: 'GET',
            category: 'metrics',
            type: 'failures',
            extensions: { crr: [...locations.crr, 'all'] },
            method: 'getFailedMetrics',
            dataPoints: ['opsFail', 'bytesFail'],
        },
        // Route: /_/metrics/crr/<location>/throughput
        // Route: /_/metrics/ingestion/<location>/throughput
        {
            httpMethod: 'GET',
            category: 'metrics',
            type: 'throughput',
            extensions: {
                crr: [...locations.crr, 'all'],
                ingestion: [...locations.ingestion, 'all'],
            },
            method: 'getThroughput',
            dataPoints: ['opsDone', 'bytesDone'],
        },
        // Route: /_/metrics/crr/<location>/all
        // Route: /_/metrics/ingestion/<location>/all
        {
            httpMethod: 'GET',
            category: 'metrics',
            type: 'all',
            extensions: {
                crr: [...locations.crr, 'all'],
                ingestion: [...locations.ingestion, 'all'],
            },
            method: 'getAllMetrics',
            dataPoints: ['ops', 'opsDone', 'opsFail', 'bytes', 'bytesDone',
                'bytesFail', 'opsPending', 'bytesPending'],
        },
        // Route: /_/metrics/crr/<site>/progress/<bucket>/<key>
        {
            httpMethod: 'GET',
            category: 'metrics',
            type: 'progress',
            level: 'object',
            extensions: { crr: [...locations.crr] },
            method: 'getObjectProgress',
            dataPoints: ['objectBytes', 'objectBytesDone'],
        },
        // Route: /_/metrics/crr/<site>/throughput/<bucket>/<key>
        {
            httpMethod: 'GET',
            category: 'metrics',
            type: 'throughput',
            level: 'object',
            extensions: { crr: [...locations.crr] },
            method: 'getObjectThroughput',
            dataPoints: ['objectBytesDone'],
        },
        // Route: /_/crr/failed?site=<site>&marker=<marker>
        {
            httpMethod: 'GET',
            type: 'all',
            extensions: { crr: ['failed'] },
            method: 'getSiteFailedCRR',
        },
        // Route: /_/crr/failed/<bucket>/<key>/<versionId>
        {
            httpMethod: 'GET',
            type: 'specific',
            extensions: { crr: ['failed'] },
            method: 'getFailedCRR',
        },
        // Route: /_/crr/failed
        {
            httpMethod: 'POST',
            type: 'all',
            extensions: { crr: ['failed'] },
            method: 'retryFailedCRR',
        },
        // Route: /_/monitoring/metrics
        {
            httpMethod: 'GET',
            category: 'monitoring',
            type: 'metrics',
            extensions: {},
            method: 'monitoringHandler',
        },
        // Route: /_/crr/pause/<location>
        // Route: /_/ingestion/pause/<location>
        // Route: /_/lifecycle/pause/<location>
        // Where <location> is an optional field
        {
            httpMethod: 'POST',
            type: 'pause',
            extensions: {
                crr: [...locations.crr, 'all'],
                ingestion: [...locations.ingestion, 'all'],
                lifecycle: [...locations.lifecycle, 'all'],
            },
            method: 'pauseService',
        },
        // Route: /_/crr/resume/<location>
        // Route: /_/ingestion/resume/<location>
        // Route: /_/lifecycle/resume/<location>
        // Route: /_/crr/resume/<location>/schedule
        // Route: /_/ingestion/resume/<location>/schedule
        // Route: /_/lifecycle/resume/<location>/schedule
        // Where <location> is an optional field unless "schedule" route
        {
            httpMethod: 'POST',
            type: 'resume',
            extensions: {
                crr: [...locations.crr, 'all'],
                ingestion: [...locations.ingestion, 'all'],
                lifecycle: [...locations.lifecycle, 'all'],
            },
            method: 'resumeService',
        },
        {
            httpMethod: 'DELETE',
            type: 'resume',
            extensions: {
                crr: [...locations.crr, 'all'],
                ingestion: [...locations.ingestion, 'all'],
                lifecycle: [...locations.lifecycle, 'all'],
            },
            method: 'deleteScheduledResumeService',
        },
        // Route: /_/crr/resume/<location>
        // Route: /_/ingestion/resume/<location>
        // Route: /_/lifecycle/resume/<location>
        {
            httpMethod: 'GET',
            type: 'resume',
            extensions: {
                crr: [...locations.crr, 'all'],
                ingestion: [...locations.ingestion, 'all'],
                lifecycle: [...locations.lifecycle, 'all'],
            },
            method: 'getResumeSchedule',
        },
        // Route: /_/crr/status/<location>
        // Route: /_/ingestion/status/<location>
        // Route: /_/lifecycle/status/<location>
        // Where <location> is an optional field
        {
            httpMethod: 'GET',
            type: 'status',
            extensions: {
                crr: [...locations.crr, 'all'],
                ingestion: [...locations.ingestion, 'all'],
                lifecycle: [...locations.lifecycle, 'all'],
            },
            method: 'getServiceStatus',
        },
        // Route: /_/configuration/workflows
        {
            httpMethod: 'POST',
            category: 'configuration',
            type: 'workflows',
            method: 'applyWorkflowConfiguration',
            extensions: {},
        },
    ];
}

module.exports = routes;
