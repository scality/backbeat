#!/bin/bash
# A kafka-console-consumer on a customer topic, tee'd into an evidence file.
# This is the customer's end of the pipeline: what a tenant's consumer sees.
# Runs in tmux window consumer-<topic> of session bnaas-demo.
#
# Usage:
#   consumer.sh start <customer-topic> [outfile]   from the current head
#   consumer.sh start <customer-topic> [outfile] --from-beginning
#   consumer.sh stop [customer-topic]              stop one, or all of them
#   consumer.sh status
#
# For evidence that has to be replayable, prefer a recorded start offset and
# demo/bin/check.sh --dump, which is deterministic. A live tail can miss
# records if it is started late.
set -u
. "$(dirname "$0")/lib.sh"

CMD=${1:-status}
TOPIC=${2:-}
OUT=${3:-}
FROM=${4:-}

case "$CMD" in
start)
    [ -n "$TOPIC" ] || die "usage: consumer.sh start <customer-topic> [outfile] [--from-beginning]"
    need_stack
    OUT=${OUT:-$DEMO_LOG_DIR/$TOPIC.jsonl}
    C=$(kafka_container)
    B=$(_kafka_bin)
    if [ "$FROM" = "--from-beginning" ]; then
        OFFSET_ARGS="--from-beginning"
        say "reading $TOPIC from the beginning"
    else
        OFFSET_ARGS=""
        say "reading $TOPIC from its current head ($(head_total "$TOPIC" 2>/dev/null || echo 0))"
    fi
    step "starting a console consumer on $TOPIC"
    say "evidence: $OUT"
    say "each line is tab separated: CreateTime:<ms>, Partition, Offset, record key, event JSON"
    tmux_send "consumer-$TOPIC" "docker exec $C $B/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $TOPIC $OFFSET_ARGS --property print.key=true --property print.timestamp=true --property print.partition=true --property print.offset=true | tee -a $OUT"
    zzz 2
    say "window: tmux attach -t $SESS \\; select-window -t consumer-$TOPIC"
    ;;
stop)
    if [ -n "$TOPIC" ]; then
        step "stopping the console consumer on $TOPIC"
        tmux_interrupt "consumer-$TOPIC"
    else
        step "stopping every console consumer in session $SESS"
        for w in $(tmux list-windows -t "$SESS" -F '#{window_name}' 2>/dev/null | grep '^consumer-' || true); do
            say "  $w"; tmux send-keys -t "$SESS:$w" C-c
        done
    fi
    ;;
status)
    for w in $(tmux list-windows -t "$SESS" -F '#{window_name}' 2>/dev/null | grep '^consumer-' || true); do
        t=${w#consumer-}
        say "$w  topic head: $(head_total "$t" 2>/dev/null || echo '?')"
    done
    ;;
*) die "usage: consumer.sh start <customer-topic> [outfile] [--from-beginning] | stop [topic] | status" ;;
esac
