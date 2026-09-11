#!/bin/bash
# Delivery-pool workers. Each worker is one process in one tmux window
# (worker<n>), started under a supervisor loop in that window, because an
# ordinary rebalance can kill it: BackbeatConsumer's commit path throws
# "Local: Erroneous state" (code -172) uncaught, and a standalone worker
# process exits rc=1. Systemd does this on S3C; the supervisor does it here,
# and every exit is timestamped into run/worker<n>.crashes so restarts are
# counted rather than hidden.
#
# Usage:
#   worker.sh start <n> [--workgroup <id>] [--conf <file>]
#   worker.sh stop <n>            stop the worker and its supervisor
#   worker.sh status
#   worker.sh hold <n>            keep it deliberately dead
#   worker.sh release <n>         let the supervisor bring it back
#   worker.sh signal <n> <SIG>    e.g. KILL, STOP, CONT, TERM
#   worker.sh crashes <n>         how many process exits the supervisor saw
#
# Probe port of worker n is WORKER_PROBE_BASE + n, exported as
# DELIVERY_POOL_PROBE_PORT, which the worker honours over the config port.
set -u
. "$(dirname "$0")/lib.sh"

CMD=${1:-status}
N=${2:-}
ARG3=${3:-}

WG=""
CONFFILE=""
shift 2 2>/dev/null || true
while [ $# -gt 0 ]; do
    case "$1" in
        --workgroup) WG=${2:-}; shift 2 ;;
        --conf) CONFFILE=${2:-}; shift 2 ;;
        *) shift ;;
    esac
done

pf() { printf '%s\n' "$DEMO/run/worker$1.pid"; }
hf() { printf '%s\n' "$DEMO/run/worker$1.hold"; }
cf() { printf '%s\n' "$DEMO/run/worker$1.crashes"; }
lf() { printf '%s\n' "$DEMO_LOG_DIR/worker$1.log"; }
wpid() { cat "$(pf "$1")" 2>/dev/null | tr -d '\n '; }

status_one() {
    local n="$1" p port
    p=$(wpid "$n"); port=$(worker_probe_port "$n")
    if alive "$p"; then
        say "worker$n RUNNING pid $p probe :$port live=$(worker_live "$n") delivered=$(worker_delivered "$n") exits=$(cnt "$(cf "$n")" ' EXIT rc=')"
        say "  per destination: $(worker_metrics "$n" | grep '^s3_notification_delivery_worker_delivered_total' | sed -E 's/.*target="([^"]*)".*\} /\1=/' | tr '\n' ' ')"
        say "  assign/revoke:   $(cnt "$(lf "$n")" 'rdkafka.assign')/$(cnt "$(lf "$n")" 'rdkafka.revoke')"
    else
        say "worker$n STOPPED$([ -f "$(hf "$n")" ] && printf ' (held dead)') exits=$(cnt "$(cf "$n")" ' EXIT rc=')"
    fi
}

case "$CMD" in
_supervise)
    # runs inside the tmux window; not for direct use
    CONFFILE=${CONFFILE:-$GENERATED/backbeat-pool.json}
    export PATH=$NODE_BIN:$PATH
    cd "$BACKBEAT_DIR" || exit 1
    while true; do
        while [ -f "$(hf "$N")" ]; do sleep 1; done
        rm -f "$(pf "$N")"
        echo "[demo-supervisor] worker$N starting $(now) conf=$(basename "$CONFFILE") workgroup=${WG:-none}" | tee -a "$(cf "$N")"
        env RIG_PIDFILE="$(pf "$N")" \
            DELIVERY_POOL_PROBE_PORT="$(worker_probe_port "$N")" \
            ${WG:+DELIVERY_POOL_WORKGROUP_ID=$WG} \
            BACKBEAT_CONFIG_FILE="$CONFFILE" \
            node $SHIMS extensions/notification/deliveryWorker/task.js 2>&1 | tee -a "$(lf "$N")"
        RC=${PIPESTATUS[0]}
        echo "[demo-supervisor] worker$N EXIT rc=$RC $(now) epoch_ms=$(nowms)" | tee -a "$(cf "$N")"
        sleep 2
    done
    ;;
start)
    [ -n "$N" ] || die "usage: worker.sh start <n> [--workgroup <id>]"
    need_stack; need_conf; need_node
    if [ -n "$WG" ]; then
        CONFFILE=${CONFFILE:-$GENERATED/backbeat-pool-wg.json}
    else
        CONFFILE=${CONFFILE:-$GENERATED/backbeat-pool.json}
    fi
    [ -f "$CONFFILE" ] || die "no such config: $CONFFILE"
    if alive "$(wpid "$N")"; then say "worker$N already running (pid $(wpid "$N"))"; status_one "$N"; exit 0; fi
    rm -f "$(hf "$N")"
    step "starting worker$N"
    say "config:     $CONFFILE"
    say "topic:      $INTERNAL_TOPIC   group: ${WG:+${DELIVERY_GROUP}-$WG-gen<G>}${WG:-$DELIVERY_GROUP}"
    say "probe:      http://localhost:$(worker_probe_port "$N")/metrics  (/_/live, /_/ready)"
    say "log:        $(lf "$N")"
    say "exits:      $(cf "$N")   (the supervisor restarts it and counts every exit)"
    if [ -n "$WG" ]; then
        say "workgroup $WG: the workgroups document must already exist in zookeeper at"
        say "  $ZK_WORKGROUPS_PATH, written by bin/notificationWorkgroupCutover.js."
        say "  Without it the worker cannot build itself and will exit."
    fi
    tmux_send "worker$N" "cd $DEMO && DEMO_LOG_DIR=$DEMO_LOG_DIR BACKBEAT_DIR=$BACKBEAT_DIR bash bin/worker.sh _supervise $N ${WG:+--workgroup $WG} --conf $CONFFILE"
    i=0
    while ! alive "$(wpid "$N")" && [ $i -lt 60 ]; do zzz 1; i=$((i + 1)); done
    alive "$(wpid "$N")" || { warn "worker$N did not come up in ${i}s, see $(lf "$N")"; exit 1; }
    say "worker$N up as pid $(wpid "$N") after ${i}s"
    say "check it is actually DELIVERING, not just assigned: worker.sh status"
    ;;
stop)
    [ -n "$N" ] || die "usage: worker.sh stop <n>"
    step "stopping worker$N and its supervisor"
    touch "$(hf "$N")"
    p=$(wpid "$N")
    [ -n "$p" ] && kill -TERM "$p" 2>/dev/null
    zzz 3
    p=$(wpid "$N")
    if alive "$p"; then
        # measured on the rig: SIGTERM does not reliably stop a delivery worker,
        # BackbeatConsumer.close() waits for a revoke callback with no deadline
        warn "worker$N ignored SIGTERM (measured behaviour), sending KILL to $p"
        kill -KILL "$p" 2>/dev/null
    fi
    tmux_interrupt "worker$N"
    zzz 1
    # the hold file deliberately stays: if the interrupt did not reach the
    # supervisor loop, the hold is what stops it restarting the worker behind
    # your back. `worker.sh start <n>` clears it.
    rm -f "$(pf "$N")"
    say "worker$N stopped (hold left in place; start clears it)"
    ;;
hold)    [ -n "$N" ] || die "usage: worker.sh hold <n>";    touch "$(hf "$N")"; say "worker$N held dead (the supervisor will not restart it)" ;;
release) [ -n "$N" ] || die "usage: worker.sh release <n>"; rm -f "$(pf "$N")" "$(hf "$N")"; say "worker$N hold released, the supervisor will bring it back" ;;
signal)
    [ -n "$N" ] || die "usage: worker.sh signal <n> <SIG>"
    SIG=$ARG3
    [ -z "$SIG" ] && SIG=TERM
    p=$(wpid "$N"); [ -n "$p" ] || die "worker$N has no pidfile"
    kill -"$SIG" "$p" && say "worker$N pid $p signalled $SIG"
    ;;
crashes) [ -n "$N" ] || die "usage: worker.sh crashes <n>"; cnt "$(cf "$N")" ' EXIT rc=' ;;
status)
    if [ -n "$N" ]; then status_one "$N"; exit 0; fi
    known=$(for f in "$DEMO"/run/worker*.pid "$DEMO"/run/worker*.crashes; do
        [ -e "$f" ] || continue
        n=$(basename "$f"); n=${n#worker}; printf '%s\n' "${n%%.*}"
    done | sort -un)
    if [ -z "$known" ]; then
        say "no worker has ever been started from this demo directory"
    else
        for n in $known; do status_one "$n"; done
    fi
    say "delivery group $DELIVERY_GROUP:"
    group_describe "$DELIVERY_GROUP" 2>/dev/null | sed 's/^/  /' | head -10
    ;;
*) die "usage: worker.sh start <n> [--workgroup <id>] | stop <n> | status | hold <n> | release <n> | signal <n> <SIG> | crashes <n>" ;;
esac
