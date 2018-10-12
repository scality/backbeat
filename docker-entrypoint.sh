#!/bin/bash

# set -e stops the execution of a script if a command or pipeline has an error
set -e

# modifying config.json
JQ_FILTERS_CONFIG="."

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

if [[ "$S3_HOST" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .s3.host=\"$S3_HOST\""
fi

if [[ "$S3_PORT" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .s3.port=\"$S3_PORT\""
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

if [[ "$EXTENSIONS_REPLICATION_REPLICATION_STATUS_PROCESSOR_RETRY_TIMEOUT_S" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.replicationStatusProcessor.retryTimeoutS=\"$EXTENSIONS_REPLICATION_REPLICATION_STATUS_PROCESSOR_RETRY_TIMEOUT_S\""
fi

if [[ "$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_RETRY_TIMEOUT_S" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.queueProcessor.retryTimeoutS=\"$EXTENSIONS_REPLICATION_QUEUE_PROCESSOR_RETRY_TIMEOUT_S\""
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

if [[ "$EXTENSIONS_LIFECYCLE_BACKLOG_METRICS_ZKPATH" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.backlogMetrics.zkPath=\"$EXTENSIONS_LIFECYCLE_BACKLOG_METRICS_ZKPATH\""
fi

if [[ "$EXTENSIONS_LIFECYCLE_BACKLOG_METRICS_INTERVALS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.lifecycle.backlogMetrics.intervalS=\"$EXTENSIONS_LIFECYCLE_BACKLOG_METRICS_INTERVALS\""
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

if [[ $JQ_FILTERS_CONFIG != "." ]]; then
    jq "$JQ_FILTERS_CONFIG" conf/config.json > conf/config.json.tmp
    mv conf/config.json.tmp conf/config.json
fi

exec "$@"
