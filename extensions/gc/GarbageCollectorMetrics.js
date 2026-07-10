const { ZenkoMetrics } = require('arsenal').metrics;

const GC_LABEL_ORIGIN =  'origin';
const GC_LABEL_OP = 'op';
const GC_LABEL_LOCATION = 'location';
const GC_LABEL_STATUS = 'status';

const gcS3Operations = ZenkoMetrics.createCounter({
    name: 's3_gc_s3_operations_total',
    help: 'Total number of S3 operations by the garbage collector processes',
    labelNames: [
        GC_LABEL_ORIGIN,
        GC_LABEL_OP,
        GC_LABEL_STATUS,
    ],
});

const gcDuration = ZenkoMetrics.createHistogram({
    name: 's3_gc_duration_seconds',
    help: 'Duration of the garbage collector operation, calculated from the time when the GC is ' +
        'requested to the end of the operation',
    labelNames: [GC_LABEL_ORIGIN, GC_LABEL_OP, GC_LABEL_LOCATION],
    buckets: [0.2, 0.1, 0.5, 2.5, 10, 50],
});

const gcFailed = ZenkoMetrics.createCounter({
    name: 's3_gc_failed_total',
    help: 'Total number of GC tasks that failed permanently after retries ' +
        'were exhausted or on a non-retryable error. Offset is committed to ' +
        'avoid ledger leaks; the underlying operation is not retried.',
    labelNames: [GC_LABEL_ORIGIN, GC_LABEL_LOCATION],
});

class GarbageCollectorMetrics {
    static handleError(log, err, method) {
        if (log) {
            log.error('failed to update prometheus metrics', { error: err, method });
        }
    }

    static onS3Request(log, op, process, err) {
        const statusCode = err && err.statusCode ? err.statusCode : '200';
        try {
            gcS3Operations.inc({
                [GC_LABEL_ORIGIN]: process,
                [GC_LABEL_OP]: op,
                [GC_LABEL_STATUS]: statusCode,
            });
        } catch (err) {
            GarbageCollectorMetrics.handleError(log, err, 'GarbageCollectorMetrics.onS3Request');
        }
    }

    static onGcCompleted(log, process, location, duration) {
        try {
            gcDuration.observe({
                [GC_LABEL_ORIGIN]: process,
                [GC_LABEL_LOCATION]: location,
            }, duration / 1000);
        } catch (err) {
            GarbageCollectorMetrics.handleError(log, err, 'GarbageCollectorMetrics.onGcComplete');
        }
    }

    static onGcFailed(log, process, location) {
        try {
            gcFailed.inc({
                [GC_LABEL_ORIGIN]: process,
                [GC_LABEL_LOCATION]: location,
            });
        } catch (err) {
            GarbageCollectorMetrics.handleError(log, err, 'GarbageCollectorMetrics.onGcFailed');
        }
    }
}

module.exports = {
    GarbageCollectorMetrics,
};
