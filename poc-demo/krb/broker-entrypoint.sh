#!/bin/bash
# Render the broker properties with the ports this stack publishes, then
# start Kafka. The offset has to reach inside the container: a client is
# handed advertised.listeners and connects to exactly that port, so the
# container listener and the published host port must be the same number.
set -e
SRC=/demo/server-kerberos.properties.tmpl
DST=/tmp/server-kerberos.properties

: "${KRB_BROKER_PORT:?KRB_BROKER_PORT is required}"
: "${KRB_VERIFY_PORT:?KRB_VERIFY_PORT is required}"

sed -e "s/__KRB_BROKER_PORT__/${KRB_BROKER_PORT}/g" \
    -e "s/__KRB_VERIFY_PORT__/${KRB_VERIFY_PORT}/g" \
    "$SRC" > "$DST"

echo "[demo] kerberos broker listeners:"
grep -E '^(listeners|advertised\.listeners)=' "$DST" | sed 's/^/[demo]   /'

mkdir -p /data
exec /opt/kafka/bin/kafka-server-start.sh "$DST"
