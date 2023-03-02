const constants = {
    statusReady: 'READY',
    statusUndefined: 'UNDEFINED',
    statusNotReady: 'NOT_READY',
    statusNotConnected: 'NOT_CONNECTED',
    kafkaBacklogMetrics: {
        promMetricNames: {
            latestPublishedMessageTimestamp: 's3_zenko_queue_latest_published_message_timestamp',
            deliveryReportsTotal: 's3_zenko_queue_delivery_reports_total',
            latestConsumedMessageTimestamp: 's3_zenko_queue_latest_consumed_message_timestamp',
            latestConsumeEventTimestamp: 's3_zenko_queue_latest_consume_event_timestamp',
        },
    },
    authTypeAssumeRole: 'assumeRole',
    authTypeAccount: 'account',
    authTypeService: 'service',
    services: {
        queuePopulator: 'QueuePopulator',
        replicationQueueProcessor: 'ReplicationQueueProcessor',
        replicationStatusProcessor: 'ReplicationStatusProcessor',
    },
};

module.exports = constants;
