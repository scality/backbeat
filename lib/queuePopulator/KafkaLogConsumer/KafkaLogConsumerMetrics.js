const { ZenkoMetrics } = require('arsenal').metrics;

// Counts consumed oplog events that downstream queue populators will
// process but that lack a top-level 'key' field — i.e., the synthesised
// partitioning key produced by the connector pipeline (see BB-768).
// Should stay at zero in steady-state; a non-zero rate signals that
// metadata grew a write path that doesn't $set the whole 'value'
// subdocument, regressing the per-object partitioning fix.
const oplogEventMissingKey = ZenkoMetrics.createCounter({
    name: 's3_oplog_event_missing_key_total',
    help: 'Total number of oplog events processed by queue populators ' +
        'with the synthesised top-level "key" field missing or null',
    labelNames: ['opType'],
});

class KafkaLogConsumerMetrics {
    static onMissingKey(log, opType) {
        try {
            oplogEventMissingKey.inc({
                opType: opType || 'unknown',
            });
        } catch (err) {
            KafkaLogConsumerMetrics.handleError(
                log, err, 'KafkaLogConsumerMetrics.onMissingKey');
        }
    }

    static handleError(log, err, method) {
        if (log && log.error) {
            log.error('failed to update prometheus metrics', {
                method,
                error: err.message,
            });
        }
    }
}

module.exports = KafkaLogConsumerMetrics;
