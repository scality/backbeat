const { ZenkoMetrics } = require('arsenal').metrics;
const { getStatusLabel } = require('../utils/metricsUtil');

const GC_LABEL_ORIGIN =  'origin';
const GC_LABEL_OP = 'op';
const GC_LABEL_STATUS = 'status';

const gcS3Operations = ZenkoMetrics.createCounter({
    name: 'gc_s3_operations',
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
        try {
            gcS3Operations.inc({
                [GC_LABEL_ORIGIN]: process,
                [GC_LABEL_OP]: op,
                [GC_LABEL_STATUS]: getStatusLabel(err),
            });
        } catch (err) {
            GarbageCollectorMetrics.handleError(log, err, 'GarbageCollectorMetrics.onS3Request');
        }
    }
}

module.exports = {
    GarbageCollectorMetrics,
};
