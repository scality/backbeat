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
}

module.exports = {
    GarbageCollectorMetrics,
};
