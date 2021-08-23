'use strict'; // eslint-disable-line

const testIsOn = process.env.CI === 'true';

const constants = {
    zookeeperNamespace:
        testIsOn ? '/backbeattest/ingestion' : '/backbeat/ingestion',
    zkStatePath: '/state',
    zkStateProperties: ['paused', 'scheduledResume'],
    metricsExtension: 'ingestion',
    metricsTypeQueued: 'queued',
    metricsTypeCompleted: 'completed',
    metricsTypePendingOnly: 'pendingOnly',
    redisKeys: {
        opsDone: testIsOn ? 'test:bb:opsdone' : 'bb:ingestion:opsdone',
        bytesDone: testIsOn ? 'test:bb:bytesdone' : 'bb:ingestion:bytesdone',
        opsPending: testIsOn ? 'test:bb:opspending' : 'bb:ingestion:opspending',
        bytesPending: testIsOn ?
            'test:bb:bytespending' : 'bb:ingestion:bytespending',
    },
    promMetricNames: {
        ingestionQueuedTotal: 'zenko_ingestion_queued_total',
        ingestionQueuedBytes: 'zenko_ingestion_queued_bytes',
        ingestionProcessedBytes: 'zenko_ingestion_processed_bytes',
        ingestionElapsedSeconds: 'zenko_ingestion_elapsed_seconds',
    },
};

module.exports = constants;
