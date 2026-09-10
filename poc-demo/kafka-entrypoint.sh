#!/bin/bash
# The poc-ft-kafka image's start-kafka.sh only understands KAFKA_BROKER_ID,
# KAFKA_ZOOKEEPER_CONNECT, KAFKA_ADVERTISED_LISTENERS, KAFKA_LISTENERS and
# KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, and appends them to
# config/server.properties. Two named listeners also need
# listener.security.protocol.map and inter.broker.listener.name, and topic
# data needs a log.dirs under a named volume, so this wrapper appends those
# first and then hands over. Java Properties takes the LAST occurrence of a
# key, so start-kafka.sh's own lines still win for the keys it writes.
set -e
CONF="${KAFKA_HOME}/config/server.properties"

cat >> "$CONF" <<PROPS
listener.security.protocol.map=${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP}
inter.broker.listener.name=${KAFKA_INTER_BROKER_LISTENER_NAME}
log.dirs=${KAFKA_LOG_DIRS}
auto.create.topics.enable=${KAFKA_AUTO_CREATE_TOPICS_ENABLE:-true}
group.initial.rebalance.delay.ms=${KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS:-0}
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
PROPS

mkdir -p "${KAFKA_LOG_DIRS}"
echo "[demo] appended two-listener config to $CONF:"
grep -E '^(listener|inter\.broker|log\.dirs|auto\.create)' "$CONF" | sed 's/^/[demo]   /'
exec /usr/local/bin/start-kafka.sh
