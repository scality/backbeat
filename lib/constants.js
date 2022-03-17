const constants = {
    kafkaBacklogMetrics: {
        promMetricNames: {
            latestPublishedMessageTimestamp: 'queue_latest_published_message_timestamp',
            deliveryReportsTotal: 'queue_delivery_reports_total',
            latestConsumedMessageTimestamp: 'queue_latest_consumed_message_timestamp',
            latestConsumeEventTimestamp: 'queue_latest_consume_event_timestamp',
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
        replicationStatusProcessor: 'ReplicationStatusProcessor',
    },
    compressionType: 'Zstd',
};

module.exports = constants;
