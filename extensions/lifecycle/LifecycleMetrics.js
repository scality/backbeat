const { ZenkoMetrics } = require('arsenal').metrics;

const LIFECYCLE_LABEL_ORIGIN =  'origin';
const LIFECYCLE_LABEL_OP = 'op';
const LIFECYCLE_LABEL_STATUS = 'status';

const conductorLatestBatchStartTime = ZenkoMetrics.createGauge({
    name: 'lifecycle_latest_batch_start_time',
    help: 'Timestamp of latest lifecycle batch start time',
    labelNames: [],
});

const lifecycleVaultOperations = ZenkoMetrics.createCounter({
    name: 'lifecycle_vault_operations',
    help: 'Total number vault operations by lifecycle processes',
    labelNames: [LIFECYCLE_LABEL_OP, LIFECYCLE_LABEL_STATUS],
});

const conductorBucketListings = {
    success: ZenkoMetrics.createCounter({
        name: 'lifecycle_conductor_bucket_list_success',
        help: 'Total number of successful bucket listings by lifecycle conductor',
        labelNames: [],
    }),
    error: ZenkoMetrics.createCounter({
        name: 'lifecycle_conductor_bucket_list_error',
        help: 'Total number of failed bucket listings by lifecycle conductor',
        labelNames: [],
    }),
};

const lifecycleS3Operations = ZenkoMetrics.createCounter({
    name: 'lifecycle_s3_operations',
    help: 'Total number of S3 operations by the lifecycle processes',
    labelNames: [
        LIFECYCLE_LABEL_ORIGIN,
        LIFECYCLE_LABEL_OP,
        LIFECYCLE_LABEL_STATUS,
    ],
});

const lifecycleKafkaPublish = {
    success: ZenkoMetrics.createCounter({
        name: 'lifecycle_kafka_publish_success',
        help: 'Total number of messages published by lifecycle processes',
        labelNames: [LIFECYCLE_LABEL_ORIGIN, LIFECYCLE_LABEL_OP],
    }),
    error: ZenkoMetrics.createCounter({
        name: 'lifecycle_kafka_publish_error',
        help: 'Total number of failed messages by lifecycle processes',
        labelNames: [LIFECYCLE_LABEL_ORIGIN, LIFECYCLE_LABEL_OP],
    }),
};

class LifecycleMetrics {
    static handleError(log, err, method) {
        if (log) {
            log.error('failed to update prometheus metrics', { error: err, method });
        }
    }

    static onProcessBuckets(log) {
        try {
            conductorLatestBatchStartTime.set({}, Date.now());
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onProcessBuckets');
        }
    }

    static onVaultRequest(log, op, err) {
        const statusCode = err && err.statusCode ? err.statusCode : '200';
        try {
            lifecycleVaultOperations.inc({
                [LIFECYCLE_LABEL_OP]: op,
                [LIFECYCLE_LABEL_STATUS]: statusCode,
            });
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onVaultRequest');
        }
    }

    static onBucketListing(log, err) {
        try {
            conductorBucketListings[err ? 'error' : 'success'].inc({});
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onBucketListing');
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
