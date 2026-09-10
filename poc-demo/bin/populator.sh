#!/bin/bash
# The backbeat queue populator, on either notification path. The path is
# chosen purely by the config file: "legacy" has no deliveryPool block, "pool"
# has one. Runs in tmux window "populator" of session bnaas-demo.
#
# Usage: populator.sh start legacy|pool [--conf <file>] | stop | status
#
# stop sends SIGINT, which lets the populator finish its batch. That is what
# keeps the cutover duplicate window shut (rig M2: zero duplicates because the
# populator stopped on a batch boundary).
set -u
. "$(dirname "$0")/lib.sh"

CMD=${1:-status}
MODE=${2:-}
CONFFILE=""
[ "${3:-}" = "--conf" ] && CONFFILE=${4:-}

status() {
    local p; p=$(pids_populator)
    if [ -n "$p" ]; then
        say "populator RUNNING pid $p on the $(cat "$DEMO/run/populator.mode" 2>/dev/null || echo unknown) path"
        ps -o command= -p $p 2>/dev/null | sed 's/^/  /' | cut -c1-200
    else
        say "populator STOPPED"
    fi
    say "internal topic $INTERNAL_TOPIC head: $(head_total "$INTERNAL_TOPIC" 2>/dev/null || echo '?')"
    say "delivery topic $DELIVERY_TOPIC head: $(head_total "$DELIVERY_TOPIC" 2>/dev/null || echo '?')"
    say "zookeeper log offset: $(zkget "$ZK_POPULATOR_PATH/logState/mongo_s3-recordlog/logOffset" 2>/dev/null | head -1)"
    say "log: $DEMO_LOG_DIR/populator-*.log"
}

case "$CMD" in
start)
    need_stack; need_conf; need_node
    case "$MODE" in
    legacy) CONFFILE=${CONFFILE:-$GENERATED/backbeat-legacy.json} ;;
    pool)   CONFFILE=${CONFFILE:-$GENERATED/backbeat-pool.json} ;;
    *) die "usage: populator.sh start legacy|pool [--conf <file>]" ;;
    esac
    [ -f "$CONFFILE" ] || die "no such config: $CONFFILE"
    LOG=$DEMO_LOG_DIR/populator-$MODE.log
    if [ -n "$(pids_populator)" ]; then
        say "a populator is already running (pid $(pids_populator)); stopping it first so the path really changes"
        tmux_interrupt populator
        zzz 10
    fi
    step "starting the populator on the $MODE path"
    say "config:    $CONFFILE"
    say "worktree:  $BACKBEAT_DIR"
    say "publishes: $([ "$MODE" = pool ] && echo "$DELIVERY_TOPIC (addressed records)" || echo "$INTERNAL_TOPIC (legacy records)")"
    say "log:       $LOG"
    tmux_send populator "export PATH=$NODE_BIN:\$PATH; cd $BACKBEAT_DIR && RIG_PIDFILE=$DEMO/run/populator.pid BACKBEAT_CONFIG_FILE=$CONFFILE BACKBEAT_QUEUEPOPULATOR_EXTENSIONS=notification node $SHIMS bin/queuePopulator.js 2>&1 | tee -a $LOG"
    printf '%s\n' "$MODE" > "$DEMO/run/populator.mode"
    i=0
    while [ -z "$(pids_populator)" ] && [ $i -lt 60 ]; do zzz 1; i=$((i + 1)); done
    [ -z "$(pids_populator)" ] && { warn "populator did not come up in ${i}s, see $LOG"; exit 1; }
    say "populator up as pid $(pids_populator) after ${i}s"
    say "all three shim lines must appear in the log; check with: grep 'shim installed' $LOG"
    ;;
stop)
    step "stopping the populator with SIGINT (it finishes its batch)"
    tmux_interrupt populator
    i=0
    while [ -n "$(pids_populator)" ] && [ $i -lt 30 ]; do zzz 1; i=$((i + 1)); done
    p=$(pids_populator)
    [ -n "$p" ] && { warn "still alive after ${i}s, sending TERM to $p"; kill -TERM $p 2>/dev/null; zzz 3; }
    say "stopped after ${i}s"
    status
    ;;
status) status ;;
*) die "usage: populator.sh start legacy|pool [--conf <file>] | stop | status" ;;
esac
