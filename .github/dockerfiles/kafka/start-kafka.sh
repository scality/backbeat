#!/bin/bash
set -e

# Configure Kafka broker based on environment variables
if [ -n "$KAFKA_BROKER_ID" ]; then
  echo "broker.id=${KAFKA_BROKER_ID}" >> ${KAFKA_HOME}/config/server.properties
fi

if [ -n "$KAFKA_ZOOKEEPER_CONNECT" ]; then
  echo "zookeeper.connect=${KAFKA_ZOOKEEPER_CONNECT}" >> ${KAFKA_HOME}/config/server.properties
else
  echo "zookeeper.connect=localhost:2181" >> ${KAFKA_HOME}/config/server.properties
fi

if [ -n "$KAFKA_ADVERTISED_LISTENERS" ]; then
  echo "advertised.listeners=${KAFKA_ADVERTISED_LISTENERS}" >> ${KAFKA_HOME}/config/server.properties
fi

if [ -n "$KAFKA_LISTENERS" ]; then
  echo "listeners=${KAFKA_LISTENERS}" >> ${KAFKA_HOME}/config/server.properties
fi

if [ -n "$KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR" ]; then
  echo "offsets.topic.replication.factor=${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR}" >> ${KAFKA_HOME}/config/server.properties
fi

# Handle legacy environment variables for backward compatibility
if [ -n "$ADVERTISED_HOST" ]; then
  echo "advertised.listeners=PLAINTEXT://${ADVERTISED_HOST}:${ADVERTISED_PORT:-9092}" >> ${KAFKA_HOME}/config/server.properties
fi

# Wait for Zookeeper to be ready if KAFKA_ZOOKEEPER_CONNECT is set
if [ -n "$KAFKA_ZOOKEEPER_CONNECT" ]; then
  ZK_HOST=$(echo $KAFKA_ZOOKEEPER_CONNECT | cut -d: -f1)
  ZK_PORT=$(echo $KAFKA_ZOOKEEPER_CONNECT | cut -d: -f2 | cut -d/ -f1)
  echo "Waiting for Zookeeper at ${ZK_HOST}:${ZK_PORT}..."
  while ! nc -z ${ZK_HOST} ${ZK_PORT}; do
    sleep 1
  done
  echo "Zookeeper is ready"
fi

# Start Kafka
echo "Starting Kafka..."
exec ${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties
