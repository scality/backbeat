#!/bin/bash

# set -e stops the execution of a script if a command or pipeline has an error
set -e

# modifying config.json
JQ_FILTERS_CONFIG="."

if [[ "$LIVENESS_PROBE_PORT" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .queuePopulator.probeServer.bindAddress=\"0.0.0.0\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .queuePopulator.probeServer.port=\"$LIVENESS_PROBE_PORT\""

    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.ingestion.probeServer.bindAddress=\"0.0.0.0\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.ingestion.probeServer.port=\"$LIVENESS_PROBE_PORT\""

    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.mongoProcessor.probeServer.bindAddress=\"0.0.0.0\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.mongoProcessor.probeServer.port=\"$LIVENESS_PROBE_PORT\""

    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.probeServer.bindAddress=\"0.0.0.0\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.probeServer.port=\"$LIVENESS_PROBE_PORT\""

    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.replicationStatusProcessor.probeServer.bindAddress=\"0.0.0.0\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.replicationStatusProcessor.probeServer.port=\"$LIVENESS_PROBE_PORT\""

    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.conductor.probeServer.bindAddress=\"0.0.0.0\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.conductor.probeServer.port=\"$LIVENESS_PROBE_PORT\""

    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.bucketProcessor.probeServer.bindAddress=\"0.0.0.0\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.bucketProcessor.probeServer.port=\"$LIVENESS_PROBE_PORT\""

    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.objectProcessor.probeServer.bindAddress=\"0.0.0.0\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.objectProcessor.probeServer.port=\"$LIVENESS_PROBE_PORT\""

    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.gc.probeServer.bindAddress=\"0.0.0.0\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.gc.probeServer.port=\"$LIVENESS_PROBE_PORT\""
fi

if [[ "$LOG_LEVEL" ]]; then
    if [[ "$LOG_LEVEL" == "info" || "$LOG_LEVEL" == "debug" || "$LOG_LEVEL" == "trace" ]]; then
        JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .log.logLevel=\"$LOG_LEVEL\""
        echo "Log level has been modified to $LOG_LEVEL"
    else
        echo "The log level you provided is incorrect (info/debug/trace)"
    fi
fi

if [[ "$ZOOKEEPER_AUTO_CREATE_NAMESPACE" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .zookeeper.autoCreateNamespace=true"
fi

if [[ "$ZOOKEEPER_CONNECTION_STRING" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .zookeeper.connectionString=\"$ZOOKEEPER_CONNECTION_STRING\""
fi

if [[ "$KAFKA_HOSTS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .kafka.hosts=\"$KAFKA_HOSTS\""
fi

if [[ "$KAFKA_BACKLOG_METRICS_ZKPATH" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .kafka.backlogMetrics.zkPath=\"$KAFKA_BACKLOG_METRICS_ZKPATH\""
fi

if [[ "$KAFKA_BACKLOG_METRICS_INTERVALS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .kafka.backlogMetrics.intervalS=\"$KAFKA_BACKLOG_METRICS_INTERVALS\""
fi

if [ -z "$REDIS_HA_NAME" ]; then
    REDIS_HA_NAME='mymaster'
fi

if [[ "$REDIS_SENTINELS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .redis.name=\"$REDIS_HA_NAME\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .redis.sentinels=\"$REDIS_SENTINELS\""
elif [[ "$REDIS_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .redis.host=\"$REDIS_HOST\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .redis.port=6379"
fi

if [[ "$REDIS_LOCALCACHE_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .localCache.host=\"$REDIS_LOCALCACHE_HOST\""
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .localCache.port=6379"
fi

if [[ "$REDIS_LOCALCACHE_PORT" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .localCache.port=$REDIS_LOCALCACHE_PORT"
fi

if [[ "$REDIS_PORT" ]] && [[ -z "$REDIS_SENTINELS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .redis.port=$REDIS_PORT"
fi

if [[ "$QUEUE_POPULATOR_BATCH_MAX_READ" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .queuePopulator.batchMaxRead=\"$QUEUE_POPULATOR_BATCH_MAX_READ\""
fi

if [[ "$QUEUE_POPULATOR_DMD_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .queuePopulator.dmd.host=\"$QUEUE_POPULATOR_DMD_HOST\""
fi

if [[ "$QUEUE_POPULATOR_DMD_PORT" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .queuePopulator.dmd.port=\"$QUEUE_POPULATOR_DMD_PORT\""
fi

if [[ "$MONGODB_HOSTS" ]]; then
   JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .queuePopulator.logSource=\"mongo\""
   JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .queuePopulator.mongo.replicaSetHosts=\"$MONGODB_HOSTS\""
fi

if [[ "$MONGODB_RS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .queuePopulator.mongo.replicaSet=\"$MONGODB_RS\""
fi

if [[ "$MONGODB_DATABASE" ]]; then
   JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .queuePopulator.mongo.database=\"$MONGODB_DATABASE\""
fi

if [[ "$CLOUDSERVER_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .s3.host=\"$CLOUDSERVER_HOST\""
fi

if [[ "$CLOUDSERVER_PORT" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .s3.port=\"$CLOUDSERVER_PORT\""
fi

if [[ "$EXTENSIONS_REPLICATION_SOURCE_S3_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.source.s3.host=\"$EXTENSIONS_REPLICATION_SOURCE_S3_HOST\""
fi

if [[ "$EXTENSIONS_REPLICATION_SOURCE_S3_PORT" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.source.s3.port=\"$EXTENSIONS_REPLICATION_SOURCE_S3_PORT\""
fi

if [[ "$EXTENSIONS_REPLICATION_SOURCE_AUTH_TYPE" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.source.auth.type=\"$EXTENSIONS_REPLICATION_SOURCE_AUTH_TYPE\""
fi

if [[ "$EXTENSIONS_REPLICATION_SOURCE_AUTH_ACCOUNT" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.source.auth.account=\"$EXTENSIONS_REPLICATION_SOURCE_AUTH_ACCOUNT\""
fi

if [[ "$EXTENSIONS_REPLICATION_DEST_AUTH_TYPE" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.destination.auth.type=\"$EXTENSIONS_REPLICATION_DEST_AUTH_TYPE\""
fi

if [[ "$EXTENSIONS_REPLICATION_DEST_AUTH_ACCOUNT" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.destination.auth.account=\"$EXTENSIONS_REPLICATION_DEST_AUTH_ACCOUNT\""
fi

if [[ "$EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST" ]]; then
    if [[ "$EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST_MORE" ]]; then
        JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.destination.bootstrapList=[{\"site\": \"zenko\", \"servers\": [\"$EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST\"]}, $EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST_MORE]"
    else
        JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.destination.bootstrapList=[{\"site\": \"zenko\", \"servers\": [\"$EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST\"]}]"
    fi
fi

# START Retry config

# AWS_S3
if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_TIMEOUT_S" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.aws_s3.timeoutS=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_TIMEOUT_S\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_MAX_RETRIES" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.aws_s3.maxRetries=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_MAX_RETRIES\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_BACKOFF_MIN" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.aws_s3.backoff.min=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_BACKOFF_MIN\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_BACKOFF_MAX" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.aws_s3.backoff.max=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_BACKOFF_MAX\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_BACKOFF_JITTER" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.aws_s3.backoff.jitter=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_BACKOFF_JITTER\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_BACKOFF_FACTOR" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.aws_s3.backoff.factor=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AWS_S3_RETRY_BACKOFF_FACTOR\""
fi

# AZURE
if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_TIMEOUT_S" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.azure.timeoutS=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_TIMEOUT_S\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_MAX_RETRIES" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.azure.maxRetries=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_MAX_RETRIES\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_BACKOFF_MIN" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.azure.backoff.min=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_BACKOFF_MIN\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_BACKOFF_MAX" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.azure.backoff.max=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_BACKOFF_MAX\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_BACKOFF_JITTER" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.azure.backoff.jitter=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_BACKOFF_JITTER\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_BACKOFF_FACTOR" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.azure.backoff.factor=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_AZURE_RETRY_BACKOFF_FACTOR\""
fi

# GCP
if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_TIMEOUT_S" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.gcp.timeoutS=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_TIMEOUT_S\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_MAX_RETRIES" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.gcp.maxRetries=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_MAX_RETRIES\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_BACKOFF_MIN" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.gcp.backoff.min=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_BACKOFF_MIN\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_BACKOFF_MAX" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.gcp.backoff.max=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_BACKOFF_MAX\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_BACKOFF_JITTER" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.gcp.backoff.jitter=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_BACKOFF_JITTER\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_BACKOFF_FACTOR" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retry.gcp.backoff.factor=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_GCP_RETRY_BACKOFF_FACTOR\""
fi

# END Retry Config

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_CONCURRENCY" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.concurrency=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_CONCURRENCY\""
fi

if [[ "$EXTENSIONS_REPLICATION_STATUS_PROCESSOR_CONCURRENCY" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.replicationStatusProcessor.concurrency=\"$EXTENSIONS_REPLICATION_STATUS_PROCESSOR_CONCURRENCY\""
fi

if [[ "$HEALTHCHECKS_ALLOWFROM" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .server.healthChecks.allowFrom=[\"$HEALTHCHECKS_ALLOWFROM\"]"
fi

if [[ "$EXTENSIONS_LIFECYCLE_ZOOKEEPER_PATH" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.zookeeperPath=\"$EXTENSIONS_LIFECYCLE_ZOOKEEPER_PATH\""
fi

if [[ "$EXTENSIONS_LIFECYCLE_BUCKET_TASK_TOPIC" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.bucketTasksTopic=\"$EXTENSIONS_LIFECYCLE_BUCKET_TASK_TOPIC\""
fi

if [[ "$EXTENSIONS_LIFECYCLE_OBJECT_TASK_TOPIC" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.objectTasksTopic=\"$EXTENSIONS_LIFECYCLE_OBJECT_TASK_TOPIC\""
fi

if [[ "$EXTENSIONS_LIFECYCLE_CONDUCTOR_CRONRULE" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.conductor.cronRule=\"$EXTENSIONS_LIFECYCLE_CONDUCTOR_CRONRULE\""
fi

if [[ "$EXTENSIONS_LIFECYCLE_BUCKET_PROCESSOR_GROUP_ID" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.bucketProcessor.groupId=\"$EXTENSIONS_LIFECYCLE_BUCKET_PROCESSOR_GROUP_ID\""
fi

if [[ "$EXTENSIONS_LIFECYCLE_OBJECT_PROCESSOR_GROUP_ID" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.objectProcessor.groupId=\"$EXTENSIONS_LIFECYCLE_OBJECT_PROCESSOR_GROUP_ID\""
fi

if [[ "$EXTENSIONS_LIFECYCLE_RULES_EXPIRATION_ENABLED" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.rules.expiration.enabled=\"$EXTENSIONS_LIFECYCLE_RULES_EXPIRATION_ENABLED\""
fi

if [[ "$EXTENSIONS_LIFECYCLE_RULES_NC_VERSION_EXPIRATION_ENABLED" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.rules.noncurrentVersionExpiration.enabled=\"$EXTENSIONS_LIFECYCLE_RULES_NC_VERSION_EXPIRATION_ENABLED\""
fi

if [[ "$EXTENSIONS_LIFECYCLE_RULES_TRANSITIONS_ENABLED" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.rules.transitions.enabled=\"$EXTENSIONS_LIFECYCLE_RULES_TRANSITIONS_ENABLED\""
fi

if [[ "$EXTENSIONS_LIFECYCLE_RULES_ABORT_INCOMPLETE_MPU_ENABLED" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.rules.abortIncompleteMultipartUpload.enabled=\"$EXTENSIONS_LIFECYCLE_RULES_ABORT_INCOMPLETE_MPU_ENABLED\""
fi

if [[ "$EXTENSIONS_LIFECYCLE_AUTH_TYPE" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.auth.type=\"$EXTENSIONS_LIFECYCLE_AUTH_TYPE\""
fi

if [[ "$EXTENSIONS_LIFECYCLE_AUTH_ACCOUNT" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.auth.account=\"$EXTENSIONS_LIFECYCLE_AUTH_ACCOUNT\""
fi

if [[ "$EXTENSIONS_GC_TOPIC" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.gc.topic=\"$EXTENSIONS_GC_TOPIC\""
fi

if [[ "$EXTENSIONS_INGESTION_AUTH_TYPE" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.ingestion.auth.type=\"$EXTENSIONS_INGESTION_AUTH_TYPE\""
fi

if [[ "$EXTENSIONS_INGESTION_AUTH_ACCOUNT" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.ingestion.auth.account=\"$EXTENSIONS_INGESTION_AUTH_ACCOUNT\""
fi

if [[ "$EXTENSIONS_INGESTION_MAX_PARALLEL_READERS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.ingestion.maxParallelReaders=\"$EXTENSIONS_INGESTION_MAX_PARALLEL_READERS\""
fi

if [[ "$REPLICATION_GROUP_ID" ]] ; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .replicationGroupId=\"$REPLICATION_GROUP_ID\""
fi

if [[ "$EXTENSIONS_WE_DATA_SOURCE_TOPIC" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.workflowEngineDataSource.topic=\"$EXTENSIONS_WE_DATA_SOURCE_TOPIC\""
fi

if [[ $JQ_FILTERS_CONFIG != "." ]]; then
    jq "$JQ_FILTERS_CONFIG" conf/config.json > conf/config.json.tmp
    mv conf/config.json.tmp conf/config.json
fi

exec "$@"
