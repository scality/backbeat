const constants = {
    kafkaBacklogMetrics: {
        promMetricNames: {
            latestPublishedMessageTimestamp: 's3_zenko_queue_latest_published_message_timestamp',
            deliveryReportsTotal: 's3_zenko_queue_delivery_reports_total',
            latestConsumedMessageTimestamp: 's3_zenko_queue_latest_consumed_message_timestamp',
            latestConsumeEventTimestamp: 's3_zenko_queue_latest_consume_event_timestamp',
        },
    },
    statusReady: 'READY',
    statusUndefined: 'UNDEFINED',
    statusNotReady: 'NOT_READY',
    statusNotConnected: 'NOT_CONNECTED',
    authTypeAssumeRole: 'assumeRole',
    authTypeAccount: 'account',
    authTypeService: 'service',
    services: {
        queuePopulator: 'QueuePopulator',
        replicationQueueProcessor: 'ReplicationQueueProcessor',
        replicationReplayProcessor: 'ReplicationReplayProcessor',
        replicationStatusProcessor: 'ReplicationStatusProcessor',
    },
    compressionType: 'Zstd',
    locationStatusCollection: '__locationStatusStore',
    lifecycleListing: {
        CURRENT_TYPE: 'current',
        NON_CURRENT_TYPE: 'noncurrent',
        ORPHAN_DM_TYPE: 'orphan',
    },
    lifecycleTaskVersions: {
        v1: 'v1',
        v2: 'v2',
    },
    indexesForFeature: {
        lifecycle: {
            v2: [
                {
                    keys: [
                        { key: 'value.last-modified', order: 1 },
                        { key: '_id', order: 1 },
                    ],
                    name: 'V2LifecycleLastModifiedPrefixed',
                },
                {
                    keys: [
                        { key: 'value.dataStoreName', order: 1 },
                        { key: 'value.last-modified', order: 1 },
                        { key: '_id', order: 1 },
                    ],
                    name: 'V2LifecycleDataStoreNamePrefixed',
                },
            ],
        },
    }
};

module.exports = constants;
