'use strict'; // eslint-disable-line

const testIsOn = process.env.CI === 'true';

const constants = {
    zookeeperReplicationNamespace: '/backbeat/replication',
    proxyVaultPath: '/_/backbeat/vault',
    proxyIAMPath: '/_/backbeat/iam',
    metricsExtension: 'crr',
    metricsTypeQueued: 'queued',
    promMetricNames: {
        replicationQueuedTotal: 's3_replication_queued_objects_total',
        replicationQueuedBytes: 's3_replication_queued_bytes_total',
        replicationProcessedBytes: 's3_replication_processed_bytes_total',
        replicationElapsedSeconds: 's3_replication_elapsed_seconds',
    },
    metricsTypeProcessed: 'processed',
    redisKeys: {
        ops: testIsOn ? 'test:bb:ops' : 'bb:crr:ops',
        bytes: testIsOn ? 'test:bb:bytes' : 'bb:crr:bytes',
        opsDone: testIsOn ? 'test:bb:opsdone' : 'bb:crr:opsdone',
        bytesDone: testIsOn ? 'test:bb:bytesdone' : 'bb:crr:bytesdone',
        failedCRR: testIsOn ? 'test:bb:crr:failed' : 'bb:crr:failed',
    },
    replicationBackends: ['aws_s3', 'azure', 'gcp'],
    replicationStages: {
        sourceDataRead: 'ReplicationSourceDataRead',
        destinationDataWrite: 'ReplicationDestinationDataWrite',
        destinationMetadataWrite: 'ReplicationDestinationMetadataWrite',
    },
};

module.exports = constants;
