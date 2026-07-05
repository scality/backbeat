const { ZenkoMetrics } = require('arsenal').metrics;
const { Logger } = require('werelogs');

const lifecycleMetricsLogger = new Logger('Backbeat:LifecycleMetrics');

const LIFECYCLE_LABEL_ORIGIN = 'origin';
const LIFECYCLE_LABEL_OP = 'op';
const LIFECYCLE_LABEL_STATUS = 'status';
const LIFECYCLE_LABEL_LOCATION = 'location';
const LIFECYCLE_LABEL_TYPE = 'type';
const LIFECYCLE_LABEL_CONDUCTOR_SCAN_ID = 'conductor_scan_id';

const LIFECYCLE_MARKER_METRICS_LOCATION = '-delete-marker-';

// Keep per-scan series long enough for scraping and debugging recent overlap,
// but remove them from prom-client after a configurable retention interval.
// We intentionally do not cap the number of tracked scan IDs: if overlapping
// scans happen, hiding older IDs would remove the signal this metric provides.
// Prometheus retains scraped scan-id series until TSDB retention expires.
const DEFAULT_SCAN_METRIC_RETENTION_S = 24 * 60 * 60;
const CONDUCTOR_ORIGIN = 'conductor';
const BUCKET_PROCESSOR_ORIGIN = 'bucket_processor';
const SCAN_METRIC_RETENTION_MS = DEFAULT_SCAN_METRIC_RETENTION_S * 1000;

const conductorLatestBatchStartTime = ZenkoMetrics.createGauge({
    name: 's3_lifecycle_latest_batch_start_time',
    help: 'Conductor scheduling heartbeat: ms-since-epoch timestamp of ' +
        'the most recent scan start. Use to detect that the conductor is ' +
        'still scheduling scans (LifecycleLateScan alert).',
    labelNames: [LIFECYCLE_LABEL_ORIGIN],
});

const conductorLatestBatchEndTime = ZenkoMetrics.createGauge({
    name: 's3_lifecycle_latest_batch_end_time',
    help: 'Timestamp (ms since epoch) of the most recent lifecycle ' +
        'conductor scan end after the scan reached bucket listing.',
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

const conductorLatestBatchBucketCount = ZenkoMetrics.createGauge({
    name: 's3_lifecycle_latest_batch_bucket_count',
    help: 'Number of buckets listed in the latest lifecycle conductor batch',
    labelNames: [LIFECYCLE_LABEL_ORIGIN],
});

const bucketProcessorScanMessagesReceived = ZenkoMetrics.createCounter({
    name: 's3_lifecycle_bucket_processor_scan_messages_total',
    help: 'Total number of bucket-task messages picked up by this bucket ' +
        'processor, grouped by conductor scan id. Per-scan series are ' +
        'auto-removed by a local cleanup timer (see setScanMetricTimeout) ' +
        'to keep label cardinality bounded.',
    labelNames: [LIFECYCLE_LABEL_ORIGIN, LIFECYCLE_LABEL_CONDUCTOR_SCAN_ID],
});

const bucketProcessorScanMessageAgeSeconds = ZenkoMetrics.createHistogram({
    name: 's3_lifecycle_bucket_processor_scan_message_age_seconds',
    help: 'Elapsed wall-clock time in seconds between conductor scan start ' +
        'and bucket-tasks topic message pickup by this bucket processor. ' +
        'This is a dequeue/backlog age signal, not bucket processing duration.',
    labelNames: [LIFECYCLE_LABEL_ORIGIN],
    buckets: [60, 300, 600, 1800, 3600, 7200, 14400, 28800, 43200, 86400],
});

// Map conductor scan ids to the cleanup timer that removes their per-scan
// prom-client series after they stop receiving bucket-task messages.
const scanMetricTimers = new Map();

function removeBucketProcessorScanMetrics(conductorScanId) {
    try {
        bucketProcessorScanMessagesReceived.remove({
            [LIFECYCLE_LABEL_ORIGIN]: BUCKET_PROCESSOR_ORIGIN,
            [LIFECYCLE_LABEL_CONDUCTOR_SCAN_ID]: conductorScanId,
        });
    } catch (err) {
        // Removing a non-existent series should not happen; if it does it
        // likely signals a bad conductorScanId or a missing metric label.
        lifecycleMetricsLogger.warn(
            'failed to remove bucket processor scan metric',
            { conductorScanId, error: err.message });
    }
}

function setScanMetricTimeout(conductorScanId) {
    const previousTimer = scanMetricTimers.get(conductorScanId);
    if (previousTimer) {
        clearTimeout(previousTimer);
    }

    // Reset retention on every message so an active scan remains observable.
    // Cleanup starts only after the scan stops producing bucket-task messages.
    const cleanupTimer = setTimeout(() => {
        removeBucketProcessorScanMetrics(conductorScanId);
        scanMetricTimers.delete(conductorScanId);
    }, SCAN_METRIC_RETENTION_MS);
    cleanupTimer.unref();
    scanMetricTimers.set(conductorScanId, cleanupTimer);
}

function resetLifecycleScanMetricCleanupTimers() {
    scanMetricTimers.forEach((timer, conductorScanId) => {
        clearTimeout(timer);
        removeBucketProcessorScanMetrics(conductorScanId);
    });
    scanMetricTimers.clear();
}

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

// For all practical purposes, this should be a counter; but we have no garantee that the clock is
// monotonic: so this is really a gauge...
const lifecycleLastTimestamp = ZenkoMetrics.createGauge({
    name: 's3_lifecycle_last_timestamp_ms',
    help: 'Timestamp of the lifecycle operation',
    labelNames: [LIFECYCLE_LABEL_TYPE, LIFECYCLE_LABEL_LOCATION],
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
    static handleError(log, err, method, params = {}) {
        if (log) {
            log.error('failed to update prometheus metrics', {
                error: err.toString(), method, ...params
            });
        }
    }

    /**
     * Update the conductor scheduling heartbeat. Called at the start of
     * every conductor scan; consumed by the LifecycleLateScan alert to
     * detect that the conductor has stopped scheduling.
     *
     * @param {Object} log - logger
     * @param {number} scanStartTimestamp - scan start timestamp in ms
     */
    static onProcessBuckets(log, scanStartTimestamp = Date.now()) {
        try {
            conductorLatestBatchStartTime.set(
                { [LIFECYCLE_LABEL_ORIGIN]: CONDUCTOR_ORIGIN },
                scanStartTimestamp);
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onProcessBuckets', {
                scanStartTimestamp,
            });
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
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onActiveIndexingJobs', { count });
        }
    }

    static onLegacyTask(log, status) {
        try {
            lifecycleLegacyTask.inc({ origin: 'conductor', status });
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onLegacyTask', { status });
        }
    }

    /**
     * Record metrics at the end of a full conductor scan.
     * @param {Object} log - logger
     * @param {number} bucketCount - total buckets listed
     */
    static onConductorScanComplete(log, bucketCount) {
        try {
            const endTimestamp = Date.now();
            conductorLatestBatchEndTime.set({
                [LIFECYCLE_LABEL_ORIGIN]: CONDUCTOR_ORIGIN,
            }, endTimestamp);
            conductorLatestBatchBucketCount.set({
                [LIFECYCLE_LABEL_ORIGIN]: CONDUCTOR_ORIGIN,
            }, bucketCount);
        } catch (err) {
            LifecycleMetrics.handleError(
                log, err, 'LifecycleMetrics.onConductorScanComplete', {
                    bucketCount,
                }
            );
        }
    }

    /**
     * Increment the count of bucket-tasks topic messages picked up by this
     * bucket processor for a specific conductor scan. Called before the task
     * is dispatched to the scheduler, once per Kafka message regardless of how
     * many objects it covers or whether processing eventually succeeds.
     *
     * Note: this counts messages (initial + continuation/listing slices),
     * not unique buckets. Keep one time series per conductor_scan_id so that
     * overlapping scans remain visible. Old scan series are removed by a
     * timer after a fixed retention interval without
     * update to avoid unbounded prom-client memory growth.
     *
     * @param {Object} log - logger
     * @param {string} conductorScanId - conductor scan id from contextInfo
     * @param {number} [conductorScanStartTimestamp] - conductor scan start
     *   timestamp from contextInfo
     */
    static onBucketProcessorScanMessageReceived(
        log, conductorScanId, conductorScanStartTimestamp) {
        // Old conductor messages produced during rolling upgrades do not have
        // a scan id. Do not create a synthetic "undefined" scan-id series.
        if (!conductorScanId) {
            return;
        }
        try {
            bucketProcessorScanMessagesReceived.inc({
                [LIFECYCLE_LABEL_ORIGIN]: BUCKET_PROCESSOR_ORIGIN,
                [LIFECYCLE_LABEL_CONDUCTOR_SCAN_ID]: conductorScanId,
            });
            setScanMetricTimeout(conductorScanId);
            // A negative or non-finite age means the conductor scan-start
            // timestamp is missing or the producer/consumer clocks are skewed.
            // Skip the observation and warn instead of recording a misleading
            // 0 in the first histogram bucket.
            const ageSeconds = (Date.now() - conductorScanStartTimestamp) / 1000;
            if (Number.isFinite(ageSeconds) && ageSeconds >= 0) {
                bucketProcessorScanMessageAgeSeconds.observe({
                    [LIFECYCLE_LABEL_ORIGIN]: BUCKET_PROCESSOR_ORIGIN,
                }, ageSeconds);
            } else {
                lifecycleMetricsLogger.warn(
                    'skipping bucket processor scan message age observation',
                    { conductorScanId, conductorScanStartTimestamp, ageSeconds });
            }
        } catch (err) {
            LifecycleMetrics.handleError(
                log, err,
                'LifecycleMetrics.onBucketProcessorScanMessageReceived',
                { conductorScanId, conductorScanStartTimestamp }
            );
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
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onLifecycleTriggered', {
                process, type, location, latencyMs,
            });
        }
    }

    static onLifecycleStarted(log, type, location, durationMs) {
        try {
            lifecycleLatency.observe({
                [LIFECYCLE_LABEL_TYPE]: type,
                [LIFECYCLE_LABEL_LOCATION]: location,
            }, durationMs / 1000);
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onLifecycleStarted', {
                type, location, durationMs,
            });
        }
    }

    static onLifecycleCompleted(log, type, location, durationMs) {
        try {
            lifecycleDuration.observe({
                [LIFECYCLE_LABEL_TYPE]: type,
                [LIFECYCLE_LABEL_LOCATION]: location,
            }, durationMs / 1000);

            lifecycleLastTimestamp.set({
                [LIFECYCLE_LABEL_TYPE]: type,
                [LIFECYCLE_LABEL_LOCATION]: location,
            }, new Date().getTime());
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onLifecycleCompleted', {
                type, location, durationMs,
            });
        }
    }

    static onS3Request(log, op, process, s3Err) {
        const statusCode = s3Err && s3Err.statusCode ? s3Err.statusCode : '200';
        try {
            lifecycleS3Operations.inc({
                [LIFECYCLE_LABEL_ORIGIN]: process,
                [LIFECYCLE_LABEL_OP]: op,
                [LIFECYCLE_LABEL_STATUS]: statusCode,
            });
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onS3Request', {
                op, process, statusCode,
            });
        }
    }

    static onKafkaPublish(log, op, process, kafkaErr, count) {
        try {
            lifecycleKafkaPublish[kafkaErr ? 'error' : 'success'].inc({
                [LIFECYCLE_LABEL_ORIGIN]: process,
                [LIFECYCLE_LABEL_OP]: op,
            }, count);
        } catch (err) {
            LifecycleMetrics.handleError(log, err, 'LifecycleMetrics.onKafkaPublish', {
                op, process, count, kafkaErr,
            });
        }
    }
}

module.exports = {
    DEFAULT_SCAN_METRIC_RETENTION_S,
    LifecycleMetrics,
    LIFECYCLE_MARKER_METRICS_LOCATION,
    resetLifecycleScanMetricCleanupTimers,
};
