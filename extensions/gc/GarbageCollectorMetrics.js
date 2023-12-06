const { ZenkoMetrics } = require('arsenal').metrics;

const GC_LABEL_ORIGIN =  'origin';
const GC_LABEL_OP = 'op';
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
    labelNames: [GC_LABEL_ORIGIN],
    buckets: [0.2, 0.1, 0.5, 2.5, 10, 50],
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

    static onGcCompleted(log, process, duration) {
        try {
            gcDuration.observe({
                [GC_LABEL_ORIGIN]: process,
            }, duration / 1000);
        } catch (err) {
            GarbageCollectorMetrics.handleError(log, err, 'GarbageCollectorMetrics.onGcComplete');
        }
    }
}

module.exports = {
    GarbageCollectorMetrics,
};
