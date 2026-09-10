#!/bin/bash
# One legacy notification queue processor per destination: today's shipped
# path, and the design's "single-destination worker, generation v0". One tmux
# window per destination, named processor-<dest>.
#
# Usage: legacy-processor.sh start <dest> [--conf <file>] | stop <dest> | status
#
# Its consumer group is <LEGACY_GROUP_PREFIX>-<dest>, built by
# QueueProcessor.js from the configured groupId plus the destination id.
set -u
. "$(dirname "$0")/lib.sh"

CMD=${1:-status}
DEST=${2:-}
CONFFILE=$GENERATED/backbeat-legacy.json
[ "${3:-}" = "--conf" ] && CONFFILE=${4:-$CONFFILE}

status_one() {
    local d="$1" g p
    g=$(legacy_group "$d")
    p=$(pids_processor "$d")
    if [ -n "$p" ]; then say "processor $d RUNNING pid $p"; else say "processor $d STOPPED"; fi
    say "  group $g"
    group_describe "$g" 2>/dev/null | sed 's/^/    /' | head -8
}

case "$CMD" in
start)
    [ -n "$DEST" ] || die "usage: legacy-processor.sh start <dest>"
    need_stack; need_conf; need_node
    LOG=$DEMO_LOG_DIR/processor-$DEST.log
    if [ -n "$(pids_processor "$DEST")" ]; then
        say "processor $DEST already running (pid $(pids_processor "$DEST"))"; status_one "$DEST"; exit 0
    fi
    step "starting the legacy queue processor for $DEST"
    say "config: $CONFFILE"
    say "group:  $(legacy_group "$DEST")"
    say "log:    $LOG"
    say "healthy looks like ONE rdkafka.assign and no rdkafka.revoke. A stream of"
    say "assign/revoke pairs is the design/06 wedge: restart it, and expect up to"
    say "45s while the previous member's group session expires."
    tmux_send "processor-$DEST" "export PATH=$NODE_BIN:\$PATH; cd $BACKBEAT_DIR && RIG_PIDFILE=$DEMO/run/processor-$DEST.pid BACKBEAT_CONFIG_FILE=$CONFFILE node $SHIMS extensions/notification/queueProcessor/task.js $DEST 2>&1 | tee -a $LOG"
    i=0
    while [ -z "$(pids_processor "$DEST")" ] && [ $i -lt 45 ]; do zzz 1; i=$((i + 1)); done
    if [ -z "$(pids_processor "$DEST")" ]; then
        warn "processor $DEST did not come up in ${i}s. If its destination is unreachable"
        warn "this is the measured behaviour: 'error setting up kafka notif destination',"
        warn "'Client is disconnected', then exit. See $LOG"
        exit 1
    fi
    say "processor $DEST up as pid $(pids_processor "$DEST") after ${i}s"
    ;;
stop)
    [ -n "$DEST" ] || die "usage: legacy-processor.sh stop <dest>"
    step "stopping the legacy queue processor for $DEST"
    tmux_interrupt "processor-$DEST"
    i=0
    while [ -n "$(pids_processor "$DEST")" ] && [ $i -lt 20 ]; do zzz 1; i=$((i + 1)); done
    p=$(pids_processor "$DEST")
    [ -n "$p" ] && { warn "still alive, sending TERM to $p"; kill -TERM $p 2>/dev/null; zzz 2; }
    status_one "$DEST"
    ;;
status)
    if [ -n "$DEST" ]; then status_one "$DEST"; exit 0; fi
    found=0
    for g in $(group_list | grep "^$LEGACY_GROUP_PREFIX-" || true); do
        found=1; status_one "${g#"$LEGACY_GROUP_PREFIX"-}"
    done
    [ "$found" = 0 ] && say "no legacy consumer group exists yet (prefix $LEGACY_GROUP_PREFIX-)"
    ;;
*) die "usage: legacy-processor.sh start <dest> | stop <dest> | status [dest]" ;;
esac
