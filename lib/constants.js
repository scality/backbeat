const { versioning, algorithms } = require('arsenal');

// Bounds of the v1 master key range in the metadata collection.
// MASTER_KEY_PREFIX is arsenal's DbPrefixes.Master (\x7fM); MASTER_KEY_PREFIX_END
// is the inclusive-exclusive upper bound on master keys, derived the same way
// as arsenal's listingParamsMasterKeysV0ToV1.
const MASTER_KEY_PREFIX = versioning.VersioningConstants.DbPrefixes.Master;
const MASTER_KEY_PREFIX_END = algorithms.listTools.DelimiterTools.inc(MASTER_KEY_PREFIX);

const constants = {
    keyPrefixes: {
        master: MASTER_KEY_PREFIX,
        masterEnd: MASTER_KEY_PREFIX_END,
    },
    kafkaBacklogMetrics: {
        promMetricNames: {
            latestPublishedMessageTimestamp: 's3_backbeat_queue_latest_published_message_timestamp',
            deliveryReportsTotal: 's3_backbeat_queue_delivery_reports_total',
            latestConsumedMessageTimestamp: 's3_backbeat_queue_latest_consumed_message_timestamp',
            latestConsumeEventTimestamp: 's3_backbeat_queue_latest_consume_event_timestamp',
            rebalanceTotal: 's3_backbeat_queue_rebalance_total',
            slowTasksCount: 's3_backbeat_queue_slowtasks_count',
            taskProcessingTime: 's3_backbeat_queue_task_processing_time_seconds',
            taskQueuedCount: 's3_backbeat_queue_queuedtasks_count',
            taskRunningCount: 's3_backbeat_queue_runningtasks_count',
        },
    },
    unassignStatus: {
        IDLE: 'idle',
        DRAINED: 'drained',
        TIMEOUT: 'timeout',
    },
    statusReady: 'READY',
    statusUndefined: 'UNDEFINED',
    statusNotReady: 'NOT_READY',
    statusNotConnected: 'NOT_CONNECTED',
    statusTimedOut: 'TIMED_OUT',
    authTypeAssumeRole: 'assumeRole',
    authTypeAccount: 'account',
    authTypeRole: 'role',
    authTypeService: 'service',
    authTypeNone: 'none',
    services: {
        queuePopulator: 'QueuePopulator',
        replicationQueueProcessor: 'ReplicationQueueProcessor',
        replicationReplayProcessor: 'ReplicationReplayProcessor',
        replicationStatusProcessor: 'ReplicationStatusProcessor',
    },
    locationStatusCollection: '__locationStatusStore',
    lifecycleListing: {
        CURRENT_TYPE: 'current',
        NON_CURRENT_TYPE: 'noncurrent',
        ORPHAN_DM_TYPE: 'orphan',
    },
    lifecycleTaskVersions: {
        v1: 'v1',
        v2: 'v2',
    },
    indexesForFeature: {
        lifecycle: {
            // Restrict to master entries: v2 Current listings only query
            // masters, so excluding versions shrinks the index and skips
            // them at scan time.
            v2: [
                {
                    keys: [
                        { key: 'value.last-modified', order: 1 },
                        { key: '_id', order: 1 },
                    ],
                    name: 'V2LifecycleLastModifiedPrefixed',
                    partialFilterExpression: {
                        _id: { $gte: MASTER_KEY_PREFIX, $lt: MASTER_KEY_PREFIX_END },
                    },
                },
                {
                    keys: [
                        { key: 'value.dataStoreName', order: 1 },
                        { key: 'value.last-modified', order: 1 },
                        { key: '_id', order: 1 },
                    ],
                    name: 'V2LifecycleDataStoreNamePrefixed',
                    partialFilterExpression: {
                        _id: { $gte: MASTER_KEY_PREFIX, $lt: MASTER_KEY_PREFIX_END },
                    },
                },
            ],
        },
    },
    backbeatConsumer: {
        // controls the number of messages to process in parallel
        CONCURRENCY_DEFAULT: 1,
        // controls the max number of messages to queue for processing.
        MAX_QUEUED_DEFAULT: 1000,
    },
};

module.exports = constants;
