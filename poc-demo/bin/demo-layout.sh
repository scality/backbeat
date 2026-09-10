#!/bin/bash
# The recording layout: one tmux window, four panes, everything a viewer
# needs to follow a scenario without switching windows.
#
#   top left     a shell in demo/, for driver.sh and the scenario commands
#   top right    the worker log, the delivery side
#   bottom left  the customer topic, what the tenant's consumer receives
#   bottom right the offsets watch: topic heads, group lag, delivered counters
#
# Usage: demo-layout.sh [worker-n] [customer-topic] [events-file]
#
# Defaults to worker 1, customer-topic-1 and demo/logs/customer-topic-1.jsonl.
# Start the console consumer first so the bottom left pane has a file to tail:
#   demo/bin/consumer.sh start customer-topic-1
set -u
. "$(dirname "$0")/lib.sh"

N=${1:-1}
TOPIC=${2:-customer-topic-1}
EVENTS=${3:-$DEMO_LOG_DIR/$TOPIC.jsonl}
WLOG=$DEMO_LOG_DIR/worker$N.log
W=layout

tmux_session
tmux kill-window -t "$SESS:$W" 2>/dev/null || true
tmux new-window -d -t "$SESS" -n "$W" -c "$DEMO"

touch "$WLOG" "$EVENTS" 2>/dev/null || true

tmux split-window -h -t "$SESS:$W" -c "$DEMO"
tmux split-window -v -t "$SESS:$W".0 -c "$DEMO"
tmux split-window -v -t "$SESS:$W".2 -c "$DEMO"

# 0 top left: a shell, with the demo paths already exported
tmux send-keys -t "$SESS:$W".0 "export PATH=$NODE_BIN:\$PATH; clear; echo 'demo shell: bin/driver.sh, bin/bucket.sh, scenarios/*.sh'" Enter
# 1 bottom left: what the customer receives
tmux send-keys -t "$SESS:$W".1 "clear; echo '--- $TOPIC (customer events) ---'; tail -F $EVENTS" Enter
# 2 top right: the delivery side
tmux send-keys -t "$SESS:$W".2 "clear; echo '--- worker$N log ---'; tail -F $WLOG | grep --line-buffered -E 'rdkafka.assign|rdkafka.revoke|delivered|dropped|error|Erroneous|un-assigning|SIGTERM'" Enter
# 3 bottom right: the numbers
tmux send-keys -t "$SESS:$W".3 "clear; bash $DEMO/bin/offsets.sh --watch" Enter

tmux select-pane -t "$SESS:$W".0

step "recording layout ready"
say "attach:  tmux attach -t $SESS \\; select-window -t $W"
say "panes:   0 shell, 1 $TOPIC events, 2 worker$N log, 3 offsets watch"
say "browser: Grafana http://localhost:$GRAFANA_PORT, Kafka UI http://localhost:$KAFKA_UI_PORT, Prometheus http://localhost:$PROMETHEUS_PORT"
say "worker probes: http://localhost:$((WORKER_PROBE_BASE + 1))/metrics ..."
