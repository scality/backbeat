#!/bin/bash
# Every offset the demo cares about, in one dump: topic ends and earliest
# offsets, --describe for the legacy groups, the delivery (pool) group and any
# drainer group, the populator's zookeeper log offset, and the demo backbeat
# processes that are running.
#
# Usage: offsets.sh [outfile] [label]
#
# Reading it: for each group, LAG per partition and CURRENT-OFFSET. A
# CURRENT-OFFSET of '-' on a partition a member holds is the wedge signature,
# not an empty group.
set -u
. "$(dirname "$0")/lib.sh"
need_stack

OUT=${1:-/dev/stdout}
LABEL=${2:-}

# --watch: the compact live view used by demo-layout.sh while recording
if [ "$OUT" = "--watch" ]; then
    while true; do
        printf '\033[H\033[2J'
        printf 'bnaas demo   %s   offset %s   kafka %s\n\n' "$(now)" "$PORT_OFFSET" "$(kafka_container)"
        printf '%-34s %s\n' "$INTERNAL_TOPIC" "$(heads "$INTERNAL_TOPIC" | tr '\n' ' ')"
        printf '%-34s %s\n' "$DELIVERY_TOPIC" "$(heads "$DELIVERY_TOPIC" | tr '\n' ' ')"
        for t in $(kbin kafka-topics.sh --bootstrap-server "$(BS)" --list 2>/dev/null | tr -d '\r' | grep '^customer-topic-'); do
            printf '%-34s %s\n' "$t" "$(heads "$t" | tr '\n' ' ')"
        done
        printf '\n%-34s %s\n' "group" "lag / committed / unknown-offset partitions"
        for g in $(group_list | grep -E "^($LEGACY_GROUP_PREFIX-|$DELIVERY_GROUP|bn-replay-)" || true); do
            printf '%-34s %s / %s / %s\n' "$g" "$(group_lag "$g")" "$(group_committed "$g")" "$(group_unknown "$g")"
        done
        printf '\nworkers (delivered_total, liveness)\n'
        for f in "$DEMO"/run/worker*.pid; do
            [ -e "$f" ] || continue
            n=$(basename "$f" .pid); n=${n#worker}
            printf '  worker%-3s %-8s live=%s pid=%s exits=%s\n' "$n" "$(worker_delivered "$n")" \
                "$(worker_live "$n")" "$(readpid "worker$n")" \
                "$(cnt "$DEMO/run/worker$n.crashes" ' EXIT rc=')"
        done
        printf '\npopulator zk offset: %s\n' "$(zkget "$ZK_POPULATOR_PATH/logState/mongo_s3-recordlog/logOffset" | head -1)"
        zzz 5
    done
fi

TOPICS="$INTERNAL_TOPIC $DELIVERY_TOPIC $FAILED_TOPIC"
for t in $(kbin kafka-topics.sh --bootstrap-server "$(BS)" --list 2>/dev/null | tr -d '\r' | grep '^customer-topic-'); do
    TOPICS="$TOPICS $t"
done

{
    echo "=== demo offsets $LABEL $(now) ==="
    echo "kafka container $(kafka_container), broker port $KAFKA_PORT, port offset $PORT_OFFSET"
    echo
    echo "--- topic end offsets ---"
    for t in $TOPICS; do
        o=$(heads "$t")
        if [ -n "$o" ]; then echo "$o"; else echo "$t: (absent)"; fi
    done
    echo
    echo "--- topic earliest offsets ---"
    for t in $TOPICS; do
        kbin kafka-get-offsets.sh --bootstrap-server "$(BS)" --topic "$t" --time earliest 2>/dev/null | tr -d '\r'
    done
    echo
    echo "--- consumer groups present ---"
    ALL=$(group_list)
    echo "$ALL"
    echo
    for g in $(echo "$ALL" | grep -E "^($LEGACY_GROUP_PREFIX-|$DELIVERY_GROUP|bn-replay-)" || true); do
        echo "--- describe group $g ---"
        group_describe "$g"
        echo
    done
    echo "--- populator zookeeper log offset ---"
    zkget "$ZK_POPULATOR_PATH/logState/mongo_s3-recordlog/logOffset"
    echo
    echo "--- workgroups document ---"
    zkget "$ZK_WORKGROUPS_PATH" || echo "(none)"
    echo
    echo "--- demo backbeat processes ---"
    ps -eo pid,rss,etime,command | grep -E "$SHIM_DIR/oplog-h-shim.js|driver.js.*--endpoint $S3" | grep -v grep
    echo
    echo "--- worker probes ---"
    for f in "$DEMO"/run/worker*.pid; do
        [ -e "$f" ] || continue
        n=$(basename "$f" .pid); n=${n#worker}
        echo "worker$n :$(worker_probe_port "$n") live=$(worker_live "$n") delivered=$(worker_delivered "$n")"
    done
} > "$OUT" 2>&1

[ "$OUT" != "/dev/stdout" ] && say "wrote $OUT"
