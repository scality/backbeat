#!/bin/bash
# Shared environment and helpers for the operator scripts under demo/bin and
# for demo/scenarios. Source it, do not execute it:
#
#   . "$(dirname "$0")/lib.sh"
#
# These scripts are for driving the demo by hand: start a process, stop it,
# look at the offsets, open the recording layout. The recorded demo itself is
# the test suite, `yarn ft_test:demo`.
#
# Nothing here is hardcoded to one machine. The backbeat repository is
# resolved from this file's own location, and the knobs that cannot be
# derived live in demo/.env.
#
# It never starts or stops anything by itself, and it refuses to touch the
# older notification rig (compose project ft, container bnaas-mongo), the
# kerberos spike (bnaaskrb) or f9-mongo.

# ---------------------------------------------------------------- paths ----
DEMO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CONF=$DEMO/conf
SHIM_DIR=$CONF/shims
GENERATED=$CONF/generated
EVROOT=$DEMO/evidence
# SUITE is set once BACKBEAT_DIR is known: the suite lives in the repository
# at tests/functional/demo, not under poc-demo.
SESS=${DEMO_TMUX_SESSION:-bnaas-demo}

# ------------------------------------------------------------------ env ----
if [ -f "$DEMO/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "$DEMO/.env"
    set +a
fi

# The backbeat repository: an explicit override, else the repository this
# material lives in, which is what it will be once it is on the branch.
if [ -z "${BACKBEAT_DIR:-}" ]; then
    BACKBEAT_DIR=$(git -C "$DEMO" rev-parse --show-toplevel 2>/dev/null)
fi
# The node 22 bin dir. An explicit NODE_BIN wins when its node is executable;
# otherwise use whatever node is on PATH, because a fresh machine will not
# have the pinned nvm path and the scripts must still find a node. The "not
# 22" caveat stands: node 24 breaks the native modules.
if [ -n "${NODE_BIN:-}" ] && [ -x "${NODE_BIN}/node" ]; then
    NODE="$NODE_BIN/node"
elif command -v node >/dev/null 2>&1; then
    NODE_BIN="$(dirname "$(command -v node)")"
    NODE="$NODE_BIN/node"
else
    NODE_BIN="$HOME/.nvm/versions/node/v22.22.3/bin"
    NODE="$NODE_BIN/node"
fi
SUITE=$BACKBEAT_DIR/tests/functional/demo

# CLOUDSERVER_DIR may be relative in demo/.env: resolve it against the
# backbeat repo, then against demo/.
_resolve_cs() {
    local raw="${CLOUDSERVER_DIR:-}"
    if [ -z "$raw" ]; then
        printf '%s\n' "$(cd "$BACKBEAT_DIR/.." 2>/dev/null && pwd)/cloudserver"
        return
    fi
    case "$raw" in
        /*) printf '%s\n' "$raw"; return ;;
    esac
    local b
    for b in "$BACKBEAT_DIR" "$DEMO" "$PWD"; do
        if [ -d "$b/$raw" ]; then (cd "$b/$raw" && pwd); return; fi
    done
    printf '%s\n' "$BACKBEAT_DIR/$raw"
}
CLOUDSERVER_DIR=$(_resolve_cs)

PORT_OFFSET=${PORT_OFFSET:-0}
KAFKA_PORT=${KAFKA_PORT:-$((9092 + PORT_OFFSET))}
ZK_PORT=${ZK_PORT:-$((2181 + PORT_OFFSET))}
REDIS_PORT=${REDIS_PORT:-$((6379 + PORT_OFFSET))}
MONGO_PORT=${MONGO_PORT:-$((27117 + PORT_OFFSET))}
CLOUDSERVER_PORT=${CLOUDSERVER_PORT:-$((8010 + PORT_OFFSET))}
PROMETHEUS_PORT=${PROMETHEUS_PORT:-$((9090 + PORT_OFFSET))}
GRAFANA_PORT=${GRAFANA_PORT:-$((3000 + PORT_OFFSET))}
KAFKA_UI_PORT=${KAFKA_UI_PORT:-$((8085 + PORT_OFFSET))}
# worker probe port = WORKER_PROBE_BASE + worker index (worker 1 -> 8921)
WORKER_PROBE_BASE=${WORKER_PROBE_BASE:-$((8920 + PORT_OFFSET))}
POPULATOR_PROBE_PORT=${POPULATOR_PROBE_PORT:-$((8910 + PORT_OFFSET))}
BACKBEAT_API_PORT=${BACKBEAT_API_PORT:-$((8901 + PORT_OFFSET))}

# ------------------------------------------------------------- names -------
PROJECT=${COMPOSE_PROJECT_NAME:-bnaasdemo}
INTERNAL_TOPIC=${INTERNAL_TOPIC:-${LEGACY_TOPIC:-backbeat-bucket-notification}}
FAILED_TOPIC=${FAILED_TOPIC:-backbeat-bucket-notification-failed}
# No DELIVERY_TOPIC here. The decided model consumes one topic, the one the
# populator already writes to, and nothing in bin/ names a second one. The
# retired DEMO_SOURCE=delivery path of the demo suite keeps its own default
# in tests/functional/demo/lib/env.js.
DELIVERY_GROUP=${DELIVERY_GROUP:-bucket-notification-delivery-group}
LEGACY_GROUP_PREFIX=${LEGACY_GROUP_PREFIX:-bnaas-demo-notification-group}
ZK_POPULATOR_PATH=${ZK_POPULATOR_PATH:-/bnaas-demo/queue-populator}
ZK_WORKGROUPS_PATH=${ZK_WORKGROUPS_PATH:-/bnaas-demo/delivery-workgroups}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-accessKey1}
S3_SECRET_KEY=${S3_SECRET_KEY:-verySecretKey1}

export AWS_ACCESS_KEY_ID=$S3_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$S3_SECRET_KEY
export AWS_DEFAULT_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true
S3=http://localhost:$CLOUDSERVER_PORT

DEMO_LOG_DIR=${DEMO_LOG_DIR:-$DEMO/logs}

# Every demo backbeat process loads this shim, and its absolute path is on
# the command line, so a demo process is never confused with one belonging to
# another rig.
DEMO_MARK=$SHIM_DIR/oplog-h-shim.js
SHIMS="--require $SHIM_DIR/oplog-h-shim.js --require $SHIM_DIR/oplog-v2diff-shim.js --require $SHIM_DIR/kafka-metadata-refresh-shim.js --require $SHIM_DIR/pidfile-shim.js"

FORBIDDEN_CONTAINERS='^(ft-|bnaaskrb-|f9-mongo$|bnaas-mongo$|wg-mongo$)'

# ----------------------------------------------------------- utilities -----
say()  { printf '%s\n' "$*"; }
step() { printf '\n== %s\n' "$*"; }
warn() { printf 'WARNING: %s\n' "$*" >&2; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
now()  { date -u +%Y-%m-%dT%H:%M:%SZ; }
nowms() { python3 -c 'import time;print(int(time.time()*1000))'; }
zzz()  { python3 -c "import time;time.sleep($1)"; }

tl() { local d="$1"; shift; mkdir -p "$d"; printf '%s %s %s\n' "$(now)" "$(nowms)" "$*" >> "$d/timeline.txt"; }
evdir() { local d="$EVROOT/$1"; mkdir -p "$d"; printf '%s\n' "$d"; }

# cnt <file> <pattern> : how many lines match, always one number, 0 when the
# file is missing. grep -c alone exits 1 on no match, so the usual idiom
# prints two numbers and breaks arithmetic.
cnt()  { local n; n=$(grep -c "$2" "$1" 2>/dev/null | head -1); printf '%s\n' "${n:-0}"; }
cnti() { local n; n=$(grep -ci "$2" "$1" 2>/dev/null | head -1); printf '%s\n' "${n:-0}"; }

# --------------------------------------------------------- containers ------
_resolve_container() {
    local want="$1" explicit="$2" port="$3" name
    if [ -n "$explicit" ]; then
        if docker ps --format '{{.Names}}' | grep -qx "$explicit"; then
            printf '%s\n' "$explicit"; return 0
        fi
        return 1
    fi
    if docker ps --format '{{.Names}}' | grep -qx "$PROJECT-$want-1"; then
        printf '%s\n' "$PROJECT-$want-1"; return 0
    fi
    name=$(docker ps --format '{{.Names}}' \
        | grep -Ev "$FORBIDDEN_CONTAINERS" \
        | grep -i 'demo' | grep -i "$want" \
        | grep -ivE 'kafka-ui|exporter|krb' | head -1)
    if [ -n "$name" ]; then printf '%s\n' "$name"; return 0; fi
    name=$(docker ps --format '{{.Names}}\t{{.Ports}}' \
        | grep ":$port->" | cut -f1 \
        | grep -Ev "$FORBIDDEN_CONTAINERS" | head -1)
    [ -n "$name" ] && { printf '%s\n' "$name"; return 0; }
    return 1
}

kafka_container() {
    [ -n "${_KAFKA_C:-}" ] && { printf '%s\n' "$_KAFKA_C"; return 0; }
    _KAFKA_C=$(_resolve_container kafka "${KAFKA_CONTAINER:-}" "$KAFKA_PORT") || return 1
    printf '%s\n' "$_KAFKA_C"
}

zk_container() {
    [ -n "${_ZK_C:-}" ] && { printf '%s\n' "$_ZK_C"; return 0; }
    _ZK_C=$(_resolve_container zookeeper "${ZK_CONTAINER:-}" "$ZK_PORT") || return 1
    printf '%s\n' "$_ZK_C"
}

mongo_container() {
    [ -n "${_MONGO_C:-}" ] && { printf '%s\n' "$_MONGO_C"; return 0; }
    _MONGO_C=$(_resolve_container mongo "${MONGO_CONTAINER:-}" "$MONGO_PORT") || return 1
    printf '%s\n' "$_MONGO_C"
}

_kafka_bin() {
    [ -n "${_KBIN:-}" ] && { printf '%s\n' "$_KBIN"; return 0; }
    local c d; c=$(kafka_container) || return 1
    for d in /opt/kafka/bin /opt/bitnami/kafka/bin /usr/local/kafka/bin; do
        if docker exec "$c" test -x "$d/kafka-topics.sh" 2>/dev/null; then
            _KBIN=$d; printf '%s\n' "$_KBIN"; return 0
        fi
    done
    return 1
}

kbin() {
    local t="$1"; shift
    local c b; c=$(kafka_container) || die "no demo kafka container is running (bring the stack up, or set KAFKA_CONTAINER in demo/.env)"
    b=$(_kafka_bin) || die "cannot find the kafka CLI inside $c"
    docker exec "$c" "$b/$t" "$@"
}

# The broker's host-facing listener as seen from INSIDE the container: the
# demo broker listens on 0.0.0.0:$KAFKA_PORT and advertises localhost with
# that port, so the CLI inside the container uses it, not 9092.
BS() { printf 'localhost:%s\n' "${KAFKA_INSIDE_PORT:-$KAFKA_PORT}"; }

zkget() {
    local c p; c=$(zk_container) || die "no demo zookeeper container is running"
    p=$(docker exec "$c" sh -c 'ls -d /apache-zookeeper-*/bin/zkCli.sh /opt/zookeeper/bin/zkCli.sh 2>/dev/null | head -1' | tr -d '\r')
    [ -z "$p" ] && die "cannot find zkCli.sh inside $c"
    docker exec "$c" "$p" -server localhost:2181 get "$1" 2>/dev/null \
        | grep -vE '^[0-9]{4}-[0-9]{2}-[0-9]{2} |^WATCHER|^WatchedEvent|^$|^Connecting to |^JLine support|^\[zk:'
}

# ------------------------------------------------------------- kafka -------
topic_exists() { kbin kafka-topics.sh --bootstrap-server "$(BS)" --list 2>/dev/null | grep -qx "$1"; }
heads() { kbin kafka-get-offsets.sh --bootstrap-server "$(BS)" --topic "$1" 2>/dev/null | tr -d '\r'; }
head_total() { heads "$1" | awk -F: '{s+=$3} END {print s+0}'; }
head_p() { heads "$1" | awk -F: -v p="$2" '$2==p {print $3}'; }

group_describe() { kbin kafka-consumer-groups.sh --bootstrap-server "$(BS)" --describe --group "$1" 2>&1 | grep -v '^$'; }
group_list()     { kbin kafka-consumer-groups.sh --bootstrap-server "$(BS)" --list 2>/dev/null | tr -d '\r'; }

# A group holds committed offsets for every topic it ever consumed, and
# --describe prints them all, so a reused group id has a total lag that never
# reaches zero and reads exactly like a wedge. Every figure is per topic.
group_topic_of() {
    case "$1" in
        "$DELIVERY_GROUP"*)      printf '%s\n' "$INTERNAL_TOPIC" ;;
        "$LEGACY_GROUP_PREFIX"*) printf '%s\n' "$INTERNAL_TOPIC" ;;
        bn-replay-*)             printf '%s\n' "$INTERNAL_TOPIC" ;;
        *)                       printf '' ;;
    esac
}
group_lag() {
    group_describe "$1" 2>/dev/null \
        | awk -v t="${2:-$(group_topic_of "$1")}" \
              'NR>1 && $6 ~ /^[0-9]+$/ && (t=="" || $2==t) {s+=$6} END {print s+0}'
}
group_committed() {
    group_describe "$1" 2>/dev/null \
        | awk -v t="${2:-$(group_topic_of "$1")}" \
              'NR>1 && $4 ~ /^[0-9]+$/ && (t=="" || $2==t) {s+=$4} END {print s+0}'
}
# A partition with records but no committed offset is the wedge signature. An
# empty partition is not: one destination is one delivery key is one
# partition, so the partitions no destination hashes to stay empty forever.
group_unknown() {
    group_describe "$1" 2>/dev/null \
        | awk -v t="${2:-$(group_topic_of "$1")}" \
              'NR>1 && $4=="-" && $5+0>0 && (t=="" || $2==t) {n++} END {print n+0}'
}

legacy_group() { printf '%s-%s\n' "$LEGACY_GROUP_PREFIX" "$1"; }

dump_topic() {
    local t="$1" start="$2" out="$3" part="${4:-0}" tmo="${5:-20000}" h n
    h=$(head_p "$t" "$part")
    if [ -z "$h" ]; then say "topic $t absent"; : > "$out"; return 0; fi
    n=$((h - start))
    if [ "$n" -le 0 ]; then say "$t p$part: no new records (head=$h start=$start)"; : > "$out"; return 0; fi
    kbin kafka-console-consumer.sh --bootstrap-server "$(BS)" --topic "$t" \
        --partition "$part" --offset "$start" --max-messages "$n" --timeout-ms "$tmo" \
        --property print.key=true --property print.timestamp=true \
        --property print.partition=true --property print.offset=true \
        > "$out" 2>/dev/null
    say "$t p$part: dumped $(wc -l < "$out" | tr -d ' ') lines (offsets $start..$((h-1))) -> $out"
}

ensure_topic() {
    if topic_exists "$1"; then say "topic $1 exists"; return 0; fi
    kbin kafka-topics.sh --bootstrap-server "$(BS)" --create --topic "$1" \
        --partitions "$2" --replication-factor 1 2>&1 | tail -1
    local ok=0 i=0
    while [ $ok -lt 3 ] && [ $i -lt 60 ]; do
        i=$((i+1))
        if [ "$(kbin kafka-topics.sh --bootstrap-server "$(BS)" --describe --topic "$1" 2>/dev/null | grep -c 'Leader: ')" -ge "$2" ]; then
            ok=$((ok+1)); else ok=0; fi
        zzz 1
    done
    say "topic $1 P=$2 ready (leader confirmations $ok)"
}

# ---------------------------------------------------------- probes ---------
worker_probe_port() { printf '%s\n' "$((WORKER_PROBE_BASE + $1))"; }
worker_metrics()    { curl -s --max-time 3 "http://localhost:$(worker_probe_port "$1")/metrics"; }
worker_live()       { curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://localhost:$(worker_probe_port "$1")/_/live"; }

worker_delivered() {
    local m; m=$(worker_metrics "$1")
    if [ -n "${2:-}" ]; then
        printf '%s\n' "$m" | grep '^s3_notification_delivery_worker_delivered_total' \
            | grep "target=\"$2\"" | awk '{s+=$2} END {printf "%d\n", s+0}'
    else
        printf '%s\n' "$m" | grep '^s3_notification_delivery_worker_delivered_total' \
            | awk '{s+=$2} END {printf "%d\n", s+0}'
    fi
}

worker_dropped() {
    worker_metrics "$1" | grep '^s3_notification_delivery_worker_dropped_total' \
        | sed -E 's/^s3_notification_delivery_worker_dropped_total\{//; s/\} /  /'
}

# ---------------------------------------------------------- processes ------
pids_populator()   { pgrep -f "$DEMO_MARK.*bin/queuePopulator.js" 2>/dev/null | tr '\n' ' '; }
pids_processor()   { pgrep -f "$DEMO_MARK.*queueProcessor/task.js ${1:-}" 2>/dev/null | tr '\n' ' '; }
pids_worker()      { pgrep -f "$DEMO_MARK.*deliveryWorker/task.js" 2>/dev/null | tr '\n' ' '; }
pids_cloudserver() { pgrep -f "$SHIM_DIR/pidfile-shim.js.*index.js" 2>/dev/null | tr '\n' ' '; }

pidfile() { printf '%s\n' "$DEMO/run/$1.pid"; }
readpid() { cat "$(pidfile "$1")" 2>/dev/null | tr -d '\n '; }
alive()   { local p="$1"; [ -n "$p" ] && ps -p "$p" >/dev/null 2>&1; }

running_workers() {
    local f n
    for f in "$DEMO"/run/worker*.pid; do
        [ -e "$f" ] || continue
        n=$(basename "$f" .pid); n=${n#worker}
        alive "$(readpid "worker$n")" && printf '%s\n' "$n"
    done
}

# The driver's endpoint is on its command line, so a demo driver is never
# confused with one driving another rig.
driver_pids() { pgrep -f "driver-cli.js.*--endpoint $S3" 2>/dev/null | tr '\n' ' '; }
driver_stop() { local p; p=$(driver_pids); [ -n "$p" ] && { kill -TERM $p 2>/dev/null; zzz 3; say "driver stopped (pid $p)"; } || say "no demo driver running"; }

# ------------------------------------------------------------- tmux --------
tmux_session() {
    tmux has-session -t "$SESS" 2>/dev/null || tmux new-session -d -s "$SESS" -n home
}

tmux_win() {
    local i=0
    tmux_session
    tmux list-windows -t "$SESS" -F '#{window_name}' 2>/dev/null | grep -qx "$1" \
        || tmux new-window -d -t "$SESS" -n "$1"
    # a send-keys immediately after new-window has been seen to fail with
    # "can't find window", so confirm it is really there before returning
    while [ $i -lt 15 ]; do
        tmux list-windows -t "$SESS" -F '#{window_name}' 2>/dev/null | grep -qx "$1" && return 0
        i=$((i + 1)); zzz 0.2
        tmux new-window -d -t "$SESS" -n "$1" 2>/dev/null || true
    done
    warn "tmux window $1 could not be created in session $SESS"
    return 1
}

tmux_send() {
    local w="$1"; shift
    tmux_win "$w"
    tmux send-keys -t "$SESS:$w" C-c
    zzz 1
    tmux send-keys -t "$SESS:$w" "$*" Enter
}

tmux_interrupt() { tmux_win "$1"; tmux send-keys -t "$SESS:$1" C-c; }

# --------------------------------------------------------- drain gate ------
# wait_drain <group> <label> [timeout_s] [probe_port]
#
# Gate on lag 0 AND on progress. A wedged consumer holds its partitions with
# a lag that simply stops falling while its liveness probe still answers 200.
# exit 0 = drained, 2 = stalled (wedge suspected), 3 = timed out.
wait_drain() {
    local g="$1" label="$2" tmo="${3:-300}" probe="${4:-}"
    local i=0 lag prev_c prev_d c d stalled=0 topic
    topic=$(group_topic_of "$g")
    prev_c=$(group_committed "$g"); prev_d=0
    [ -n "$probe" ] && prev_d=$(curl -s --max-time 3 "http://localhost:$probe/metrics" | grep '^s3_notification_delivery_worker_delivered_total' | awk '{s+=$2} END {printf "%d\n", s+0}')
    while [ $i -lt "$tmo" ]; do
        lag=$(group_lag "$g")
        c=$(group_committed "$g")
        d=$prev_d
        [ -n "$probe" ] && d=$(curl -s --max-time 3 "http://localhost:$probe/metrics" | grep '^s3_notification_delivery_worker_delivered_total' | awk '{s+=$2} END {printf "%d\n", s+0}')
        if [ -n "$topic" ] && [ "$(head_total "$topic")" = "0" ] && [ "$lag" = "0" ]; then
            say "$label: $topic is still empty, nothing to drain"
            return 0
        fi
        if [ "$lag" = "0" ] && [ "$(group_unknown "$g")" = "0" ] && [ $i -gt 4 ]; then
            say "$label: lag 0 after ${i}s"
            return 0
        fi
        if [ "$c" = "$prev_c" ] && [ "$d" = "$prev_d" ]; then
            stalled=$((stalled + 5))
        else
            stalled=0
        fi
        if [ $stalled -ge 60 ]; then
            warn "$label: WEDGE SUSPECTED. lag $lag has not fallen and no progress for ${stalled}s."
            warn "  signature: partitions held, CURRENT-OFFSET may show '-', liveness answers 200, nothing delivered."
            warn "  cure: restart that one consumer. Expect up to 45s while the wedged member's group session expires."
            return 2
        fi
        prev_c=$c; prev_d=$d
        printf '  %s t+%ss lag=%s committed=%s delivered=%s\n' "$label" "$i" "$lag" "$c" "$d"
        zzz 5; i=$((i + 5))
    done
    warn "$label: still not drained after ${tmo}s (lag $(group_lag "$g"))"
    return 3
}

# ----------------------------------------------------------- checks --------
need_stack() {
    kafka_container >/dev/null 2>&1 || die "no demo kafka container running. Bring the stack up: demo/bin/stack-up.sh"
    say "kafka container: $(kafka_container) (broker port $KAFKA_PORT)"
}

need_conf() {
    [ -f "$GENERATED/backbeat-legacy.json" ] && [ -f "$SHIM_DIR/oplog-h-shim.js" ] && return 0
    say "rendering demo configs (first run)"
    bash "$DEMO/bin/conf-render.sh" >/dev/null || die "conf-render.sh failed"
}

need_node() {
    [ -x "$NODE" ] || die "node 22 not found at $NODE_BIN (set NODE_BIN in demo/.env)"
    export PATH="$NODE_BIN:$PATH"
}

need_backbeat() {
    [ -n "$BACKBEAT_DIR" ] && [ -f "$BACKBEAT_DIR/package.json" ] \
        || die "cannot find the backbeat repository. Set BACKBEAT_DIR in demo/.env"
}

mkdir -p "$DEMO/run" "$DEMO_LOG_DIR" 2>/dev/null
