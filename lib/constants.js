const constants = {
    statusReady: 'READY',
    statusUndefined: 'UNDEFINED',
    statusNotReady: 'NOT_READY',
    statusNotConnected: 'NOT_CONNECTED',
    kafkaBacklogMetrics: {
        promMetricNames: {
            latestPublishedMessageTimestamp:
            'zenko_queue_latest_published_message_timestamp',
            deliveryReportsTotal: 'zenko_queue_delivery_reports_total',
            latestConsumedMessageTimestamp:
            'zenko_queue_latest_consumed_message_timestamp',
            latestConsumeEventTimestamp:
            'zenko_queue_latest_consume_event_timestamp',
        },
    },
};

module.exports = constants;
