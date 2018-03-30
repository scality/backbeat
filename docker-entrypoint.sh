#!/bin/bash

# set -e stops the execution of a script if a command or pipeline has an error
set -e

# modifying config.json
JQ_FILTERS_CONFIG="."

if [[ "$ZOOKEEPER_AUTO_CREATE_NAMESPACE" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .zookeeper.autoCreateNamespace=true"
fi

if [[ "$ZOOKEEPER_CONNECTION_STRING" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .zookeeper.connectionString=\"$ZOOKEEPER_CONNECTION_STRING\""
fi

if [[ "$KAFKA_HOSTS" ]]; then
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .kafka.hosts=\"$KAFKA_HOSTS\""
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
    JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .extensions.replication.destination.bootstrapList=[{\"site\": \"zenko\", \"servers\": [\"$EXTENSIONS_REPLICATION_DEST_BOOTSTRAPLIST\"]}]"
fi

if [[ $JQ_FILTERS_CONFIG != "." ]]; then
    jq "$JQ_FILTERS_CONFIG" conf/config.json > conf/config.json.tmp
    mv conf/config.json.tmp conf/config.json
fi

exec "$@"
