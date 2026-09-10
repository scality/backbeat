#!/usr/bin/env bash
# Create every topic the notification pipeline needs, BEFORE any consumer
# joins, and prove each partition has a leader before returning.
#
# Order matters. A consumer that subscribes to a topic which does not exist
# yet, or whose partitions have no leader yet, can wedge: it becomes a live
# group member holding all its partitions, cycling assign -> revoke about
# once a second, delivering nothing, while its liveness probe still answers
# 200. That fired on 5 of 21 consumer starts during the migration
# experiments. Pre-creating the topics and confirming leadership three
# consecutive times is the mitigation.
#
# Idempotent: --if-not-exists, and an existing topic keeps its partition
# count (kafka cannot reduce partitions, and increasing them under load
# reshuffles keys across partitions, so this script never alters one).
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
load_env

say "creating topics on localhost:${KAFKA_PORT} (project ${PROJECT})"

create_topic() {
    local topic="$1" parts="$2"
    local existing
    existing="$(kafka_cli kafka-topics.sh --describe --topic "$topic" 2>/dev/null \
        | awk -v t="$topic" '$1=="Topic:" && $2==t {print $6; exit}' || true)"
    if [ -n "$existing" ]; then
        if [ "$existing" != "$parts" ]; then
            warn "$topic exists with $existing partitions, wanted $parts. Left alone: changing the partition count of a live topic reshuffles keys."
        else
            info "$topic already exists, $existing partitions"
        fi
        return 0
    fi
    kafka_cli kafka-topics.sh --create --if-not-exists --topic "$topic" \
        --partitions "$parts" --replication-factor 1 >/dev/null
    info "$topic created, $parts partitions"
}

# every partition of every topic must report a leader, three times running
leaders_ok() {
    local topic="$1"
    local out
    out="$(kafka_cli kafka-topics.sh --describe --topic "$topic" 2>/dev/null)" || return 1
    # a partition with no leader prints "Leader: none" or "Leader: -1"
    if printf '%s\n' "$out" | grep -qE 'Leader: (none|-1)'; then
        return 1
    fi
    printf '%s\n' "$out" | grep -q 'Partition:' || return 1
    return 0
}

wait_leaders() {
    local topic="$1" streak=0 attempts=0
    while [ "$streak" -lt 3 ]; do
        if leaders_ok "$topic"; then
            streak=$(( streak + 1 ))
        else
            streak=0
        fi
        attempts=$(( attempts + 1 ))
        [ "$attempts" -gt 60 ] && die "$topic still has a partition without a leader after $attempts checks"
        [ "$streak" -lt 3 ] && sleep 1
    done
    info "$topic: every partition has a leader (3 consecutive checks)"
}

wait_for "kafka broker" 120 kafka_cli kafka-topics.sh --list

create_topic "$LEGACY_TOPIC"   "$LEGACY_TOPIC_PARTITIONS"
create_topic "$FAILED_TOPIC"   1
create_topic "$DELIVERY_TOPIC" "$DELIVERY_TOPIC_PARTITIONS"
for t in $CUSTOMER_TOPICS; do
    create_topic "$t" "$CUSTOMER_TOPIC_PARTITIONS"
done

for t in "$LEGACY_TOPIC" "$FAILED_TOPIC" "$DELIVERY_TOPIC" $CUSTOMER_TOPICS; do
    wait_leaders "$t"
done

say "topics now on the broker"
kafka_cli kafka-topics.sh --list | sed 's/^/    /'
