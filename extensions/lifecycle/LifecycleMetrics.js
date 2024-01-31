const { ZenkoMetrics } = require('arsenal').metrics;

const LIFECYCLE_LABEL_ORIGIN =  'origin';
const LIFECYCLE_LABEL_OP = 'op';
const LIFECYCLE_LABEL_STATUS = 'status';
const LIFECYCLE_LABEL_LOCATION = 'location';
const LIFECYCLE_LABEL_TYPE = 'type';

const conductorLatestBatchStartTime = ZenkoMetrics.createGauge({
    name: 's3_lifecycle_latest_batch_start_time',
    help: 'Timestamp of latest lifecycle batch start time',
    labelNames: [LIFECYCLE_LABEL_ORIGIN],
});

// const lifecycleVaultOperations = ZenkoMetrics.createCounter({
//     name: 's3_lifecycle_vault_operations_total',
//     help: 'Total number vault operations by lifecycle processes',
//     labelNames: [LIFECYCLE_LABEL_OP, LIFECYCLE_LABEL_STATUS],
// });

const conductorBucketListings = {
    success: ZenkoMetrics.createCounter({
        name: 's3_lifecycle_conductor_bucket_list_success_total',
        help: 'Total number of successful bucket listings by lifecycle conductor',
        labelNames: [LIFECYCLE_LABEL_ORIGIN],
    }),
    error: ZenkoMetrics.createCounter({
        name: 's3_lifecycle_conductor_bucket_list_error_total',
        help: 'Total number of failed bucket listings by lifecycle conductor',
        labelNames: [LIFECYCLE_LABEL_ORIGIN],
    }),
    throttling: ZenkoMetrics.createCounter({
        name: 's3_lifecycle_conductor_bucket_list_throttling_total',
        help: 'Total number of throttled bucket listings by lifecycle conductor',
        labelNames: [LIFECYCLE_LABEL_ORIGIN],
    }),
};

const lifecycleActiveIndexingJobs = ZenkoMetrics.createGauge({
    name: 's3_lifecycle_active_indexing_jobs',
    help: 'Number of active indexing jobs',
    labelNames: [LIFECYCLE_LABEL_ORIGIN],
});

const lifecycleLegacyTask = ZenkoMetrics.createCounter({
    name: 's3_lifecycle_legacy_tasks_total',
    help: 'Number of legacy tasks triggered by lifecycle',
    labelNames: [LIFECYCLE_LABEL_ORIGIN, LIFECYCLE_LABEL_STATUS],
});

const lifecycleS3Operations = ZenkoMetrics.createCounter({
    name: 's3_lifecycle_s3_operations_total',
    help: 'Total number of S3 operations by the lifecycle processes',
    labelNames: [
        LIFECYCLE_LABEL_ORIGIN,
        LIFECYCLE_LABEL_OP,
        LIFECYCLE_LABEL_STATUS,
    ],
});

const lifecycleTriggerLatency = ZenkoMetrics.createHistogram({
    name: 's3_lifecycle_trigger_latency_seconds',
    help: 'Delay between the theoretical date and identification of the object as eligible for ' +
        'lifecycle operation',
    labelNames: [LIFECYCLE_LABEL_ORIGIN, LIFECYCLE_LABEL_TYPE, LIFECYCLE_LABEL_LOCATION],
    buckets: [60, 600, 3600, 2 * 3600, 4 * 3600, 8 * 3600, 16 * 3600, 24 * 3600, 48 * 3600],
});

const lifecycleLatency = ZenkoMetrics.createHistogram({
    name: 's3_lifecycle_latency_seconds',
    help: 'Delay between the theoretical date and start of the lifecycle operation processing',
    labelNames: [LIFECYCLE_LABEL_TYPE, LIFECYCLE_LABEL_LOCATION],
    buckets: [60, 600, 3600, 2 * 3600, 4 * 3600, 8 * 3600, 16 * 3600, 24 * 3600, 48 * 3600],
});

const lifecycleDuration = ZenkoMetrics.createHistogram({
    name: 's3_lifecycle_duration_seconds',
    help: 'Duration of the lifecycle operation, calculated from the theoretical date to the end ' +
        'of the operation',
    labelNames: [LIFECYCLE_LABEL_TYPE, LIFECYCLE_LABEL_LOCATION],
    buckets: [0.2, 1, 5, 30, 120, 600, 3600, 4 * 3600, 8 * 3600, 16 * 3600, 24 * 3600],
});

const lifecycleKafkaPublish = {
    success: ZenkoMetrics.createCounter({
        name: 's3_lifecycle_kafka_publish_success_total',
        help: 'Total number of messages published by lifecycle processes',
        labelNames: [LIFECYCLE_LABEL_ORIGIN, LIFECYCLE_LABEL_OP],
    }),
    error: ZenkoMetrics.createCounter({
        name: 's3_lifecycle_kafka_publish_error_total',
        help: 'Total number of failed messages by lifecycle processes',
        labelNames: [LIFECYCLE_LABEL_ORIGIN, LIFECYCLE_LABEL_OP],
    }),
};

class LifecycleMetrics {
    static handleError(log, err, method) {
        if (log) {
            log.error('failed to update prometheus metrics', { error: Object.assign({}, err), method });
        }
    }

    static onProcessBuckets(log) {
        try {
            conductorLatestBatchStartTime.set({ origin: 'conductor' }, Date.now());
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onProcessBuckets');
        }
    }

    // TODO: "BB-344 Vaultclient is not returning error with statusCode" fixes me.
    // static onVaultRequest(log, op, err) {
    //     const statusCode = err && err.statusCode ? err.statusCode : '200';
    //     try {
    //         lifecycleVaultOperations.inc({
    //             [LIFECYCLE_LABEL_OP]: op,
    //             [LIFECYCLE_LABEL_STATUS]: statusCode,
    //         });
    //     } catch (err) {
    //         LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onVaultRequest');
    //     }
    // }

    static onBucketListing(log, err) {
        try {
            if (!err) {
                conductorBucketListings.success.inc({ origin: 'conductor' });
            } else if (err.Throttling) {
                conductorBucketListings.throttling.inc({ origin: 'conductor' });
            } else {
                conductorBucketListings.error.inc({ origin: 'conductor' });
            }
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onBucketListing');
        }
    }

    static onActiveIndexingJobsFailed(log) {
        try {
            lifecycleActiveIndexingJobs.reset();
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onActiveIndexingJobsFailed');
        }
    }

    static onActiveIndexingJobs(log, count) {
        try {
            lifecycleActiveIndexingJobs.set({ origin: 'conductor' }, count);
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onActiveIndexingJobs');
        }
    }

    static onLegacyTask(log, status) {
        try {
            lifecycleLegacyTask.inc({ origin: 'conductor', status });
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onLegacyTask');
        }
    }

    static onLifecycleTriggered(log, process, type, location, latencyMs) {
        try {
            lifecycleTriggerLatency.observe({
                [LIFECYCLE_LABEL_ORIGIN]: process,
                [LIFECYCLE_LABEL_TYPE]: type,
                [LIFECYCLE_LABEL_LOCATION]: location,
            }, latencyMs / 1000);
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onLifecycleTriggered');
        }
    }

    static onLifecycleStarted(log, type, location, durationMs) {
        try {
            lifecycleLatency.observe({
                [LIFECYCLE_LABEL_TYPE]: type,
                [LIFECYCLE_LABEL_LOCATION]: location,
            }, durationMs / 1000);
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onLifecycleStarted');
        }
    }

    static onLifecycleCompleted(log, type, location, durationMs) {
        try {
            lifecycleDuration.observe({
                [LIFECYCLE_LABEL_TYPE]: type,
                [LIFECYCLE_LABEL_LOCATION]: location,
            }, durationMs / 1000);
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onLifecycleCompleted');
        }
    }

    static onS3Request(log, op, process, err) {
        const statusCode = err && err.statusCode ? err.statusCode : '200';
        try {
            lifecycleS3Operations.inc({
                [LIFECYCLE_LABEL_ORIGIN]: process,
                [LIFECYCLE_LABEL_OP]: op,
                [LIFECYCLE_LABEL_STATUS]: statusCode,
            });
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onS3Request');
        }
    }

    static onKafkaPublish(log, op, process, err, count) {
        try {
            lifecycleKafkaPublish[err ? 'error' : 'success'].inc({
                [LIFECYCLE_LABEL_ORIGIN]: process,
                [LIFECYCLE_LABEL_OP]: op,
            }, count);
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onKafkaPublish');
        }
    }
}

module.exports = {
    LifecycleMetrics,
};
