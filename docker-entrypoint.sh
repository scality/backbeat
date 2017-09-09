#!/bin/bash

# set -e stops the execution of a script if a command or pipeline has an error
set -e

# modifying config.json
JQ_FILTERS_CONFIG=\
".zookeeper.connectionString=\"zookeeper:2181/backbeat\" | \
 .zookeeper.autoCreateNamespace=true | \
 .kafka.hosts=\"kafka:9092\" | \
 .extensions.replication.source.s3.host=\"cloudserver-front\" | \
 .extensions.replication.source.dmd.host=\"cloudserver-metadata\" | \
 .extensions.replication.destination.bootstrapList=[]"


if [[ "$LOG_LEVEL" ]]; then
    if [[ "$LOG_LEVEL" == "info" || "$LOG_LEVEL" == "debug" || "$LOG_LEVEL" == "trace" ]]; then
        JQ_FILTERS_CONFIG="$JQ_FILTERS_CONFIG | .log.logLevel=\"$LOG_LEVEL\""
        echo "Log level has been modified to $LOG_LEVEL"
    else
        echo "The log level you provided is incorrect (info/debug/trace)"
    fi
fi

if [[ $JQ_FILTERS_CONFIG != "." ]]; then
    jq "$JQ_FILTERS_CONFIG" conf/config.json > conf/config.json.tmp
    mv conf/config.json.tmp conf/config.json
fi

exec "$@"
