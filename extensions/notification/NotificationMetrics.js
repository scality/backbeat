const { ZenkoMetrics } = require('arsenal').metrics;

// Config manager is used in both the QueuePopulator and QueueProcessor
// we use this label to distinguish between the two
const NOTIFICATION_LABEL_ORIGIN =  'origin';
// Bucket Notification target
const NOTIFICATION_LABEL_DESTINATION =  'target';
// should equal true if config manager's cache was hit during a get operation
const CONFIG_MANAGET_CACHE_HIT = 'cache_hit';

const notificationConfigManager = {
    cacheUpdates: ZenkoMetrics.createCounter({
        name: 'notification_config_manager_cache_updates',
        help: 'Total number of cache updates',
        labelNames: [
            NOTIFICATION_LABEL_ORIGIN
        ],
    }),
    configGet: ZenkoMetrics.createCounter({
        name: 'notification_config_manager_cache_get',
        help: 'Total number of config get operations in the notification config manager',
        labelNames: [
            NOTIFICATION_LABEL_ORIGIN,
            CONFIG_MANAGET_CACHE_HIT,
        ],
    }),
    cachedBuckets: ZenkoMetrics.createGauge({
        name: 'notification_config_manager_cached_buckets_count',
        help: 'Total number of cached buckets in the notification config manager',
        labelNames: [
            NOTIFICATION_LABEL_ORIGIN
        ],
    }),
};

const notificationQueueProcessor = {
    succeeded: ZenkoMetrics.createCounter({
        name: 'notification_queue_processor_successful_notification',
        help: 'Total number of successful notifications delivered',
        labelNames: [
            NOTIFICATION_LABEL_DESTINATION
        ],
    }),
    failed: ZenkoMetrics.createCounter({
        name: 'notification_queue_processor_failed_notification',
        help: 'Total number of notifications that failed to delivered',
        labelNames: [
            NOTIFICATION_LABEL_DESTINATION
        ],
    }),
    notificationSize: ZenkoMetrics.createCounter({
        name: 'notification_queue_processor_notification_size',
        help: 'Total size of the notifications that were sent',
        labelNames: [
            NOTIFICATION_LABEL_DESTINATION
        ],
    }),
    deliveryDelay: ZenkoMetrics.createGauge({
        name: 'notification_queue_processor_notification_delivery_delay_sec',
        help: 'Difference between the time a notification is sent and when it gets delivered',
        labelNames: [
            NOTIFICATION_LABEL_DESTINATION
        ],
    }),
    processedEvents: ZenkoMetrics.createCounter({
        name: 'notification_queue_processor_processed_events',
        help: 'Total number of processed oplog events',
        labelNames: [
            NOTIFICATION_LABEL_DESTINATION
        ],
    }),
    ignoredEvents: ZenkoMetrics.createCounter({
        name: 'notification_queue_processor_ignored_events',
        help: 'Total number of unprocessed oplog events',
        labelNames: [
            NOTIFICATION_LABEL_DESTINATION
        ],
    }),
    failedEvents: ZenkoMetrics.createCounter({
        name: 'notification_queue_processor_failed_events',
        help: 'Total number of oplog events that failed to be processed',
        labelNames: [
            NOTIFICATION_LABEL_DESTINATION
        ],
    }),
    processingDelay: ZenkoMetrics.createGauge({
        name: 'notification_queue_processor_processing_lag_sec',
        help: 'Time it takes to process an event and send the notification',
        labelNames: [
            NOTIFICATION_LABEL_DESTINATION
        ],
    }),
};

const notificationQueuePopulator = {
    processingDelay: ZenkoMetrics.createHistogram({
        name: 'notification_queue_populator_processing_lag_sec',
        help: 'Time it takes to process an event before pushing it to kafka',
        labelNames: [],
    }),
    processedEvents: ZenkoMetrics.createCounter({
        name: 'notification_queue_populator_processed_events',
        help: 'Total number of processed oplog events',
        labelNames: [],
    }),
    ignoredEvents: ZenkoMetrics.createCounter({
        name: 'notification_queue_populator_ignored_events',
        help: 'Total number of unprocessed oplog events',
        labelNames: [],
    }),
};

class NotificationMetrics {

    static handleError(log, err, method) {
        if (log) {
            log.error('failed to update prometheus metrics', { error: err, method });
        }
    }

    static onConfigManagerCacheUpdate(log, origin, op) {
        try {
            notificationConfigManager.cacheUpdates.inc({
                [NOTIFICATION_LABEL_ORIGIN]: origin
            });
            if (op === 'add') {
                notificationConfigManager.cachedBuckets.inc({
                    [NOTIFICATION_LABEL_ORIGIN]: origin
                });
            } else if (op === 'remove') {
                notificationConfigManager.cachedBuckets.dec({
                    [NOTIFICATION_LABEL_ORIGIN]: origin
                });
            }
        } catch (err) {
            NotificationMetrics.handleError(log, err, 'NotificationMetrics.onConfigManagerCacheUpdate');
        }
    }

    static onConfigManagerConfigGet(log, origin, hitMiss) {
        try {
            notificationConfigManager.configGet.inc({
                [NOTIFICATION_LABEL_ORIGIN]: origin,
                [CONFIG_MANAGET_CACHE_HIT]: hitMiss,
            });
        } catch (err) {
            NotificationMetrics.handleError(log, err, 'NotificationMetrics.onConfigManagerCacheHit');
        }
    }

    static onNotificationSent(log, destination, status, messages, delay) {
        try {
            notificationQueueProcessor[(status === 'success') ? 'succeeded' : 'failed'].inc({
                [NOTIFICATION_LABEL_DESTINATION]: destination,
            });
            // Update total size of notifications sent
            const messageSize = new TextEncoder().encode(JSON.stringify(messages)).length;
            notificationQueueProcessor.notificationSize.inc({
                [NOTIFICATION_LABEL_DESTINATION]: destination,
            }, messageSize);
            // update notification delivery delay
            if (delay) {
                notificationQueueProcessor.deliveryDelay.set({
                    [NOTIFICATION_LABEL_DESTINATION]: destination,
                }, delay);
            }
        } catch (err) {
            NotificationMetrics.handleError(log, err, 'NotificationMetrics.onNotificationSent');
        }
    }

    static onQueueProcessorEventProcessed(log, destination, delay) {
        try {
            notificationQueueProcessor.processedEvents.inc({
                [NOTIFICATION_LABEL_DESTINATION]: destination,
            });
            notificationQueueProcessor.processingDelay.set({
                [NOTIFICATION_LABEL_DESTINATION]: destination,
            }, delay);
        } catch (err) {
            NotificationMetrics.handleError(log, err, 'NotificationMetrics.onQueueProcessorEventProcessed');
        }
    }

    static onQueueProcessorEventIgnored(log, destination) {
        try {
            notificationQueueProcessor.ignoredEvents.inc({
                [NOTIFICATION_LABEL_DESTINATION]: destination,
            });
        } catch (err) {
            NotificationMetrics.handleError(log, err, 'NotificationMetrics.onQueueProcessorEventIgnored');
        }
    }

    static onQueueProcessorEventFailed(log, destination) {
        try {
            notificationQueueProcessor.failedEvents.inc({
                [NOTIFICATION_LABEL_DESTINATION]: destination,
            });
        } catch (err) {
            NotificationMetrics.handleError(log, err, 'NotificationMetrics.onQueueProcessorEventFailed');
        }
    }

    static onQueuePopulatorEventProcessed(log, delay) {
        try {
            notificationQueuePopulator.processedEvents.inc();
            notificationQueuePopulator.processingDelay.set({}, delay);
        } catch (err) {
            NotificationMetrics.handleError(log, err, 'NotificationMetrics.onQueuePopulatorEventProcessed');
        }
    }

    static onQueuePopulatorEventIgnored(log) {
        try {
            notificationQueuePopulator.ignoredEvents.inc();
        } catch (err) {
            NotificationMetrics.handleError(log, err, 'NotificationMetrics.onQueuePopulatorEventIgnored');
        }
    }
}

module.exports = NotificationMetrics;
