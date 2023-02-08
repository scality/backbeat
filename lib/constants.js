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
};

module.exports = constants;
