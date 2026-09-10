#!/bin/bash
# CloudServer 9.3 from source, on MongoDB metadata, in tmux window
# "cloudserver" of session bnaas-demo.
#
# Usage: cloudserver.sh start|stop|status
set -u
. "$(dirname "$0")/lib.sh"

LOG=$DEMO_LOG_DIR/cloudserver.log
CMD=${1:-status}

status() {
    local p; p=$(pids_cloudserver)
    if [ -n "$p" ]; then
        say "cloudserver RUNNING pid $p on http://localhost:$CLOUDSERVER_PORT"
        curl -s -o /dev/null -w '  GET / -> HTTP %{http_code}\n' --max-time 5 "$S3/" || true
    else
        say "cloudserver STOPPED (expected on http://localhost:$CLOUDSERVER_PORT)"
    fi
    say "config: $GENERATED/cloudserver-config.json"
    say "log:    $LOG"
}

case "$CMD" in
start)
    need_conf; need_node
    if [ -n "$(pids_cloudserver)" ]; then say "cloudserver already running (pid $(pids_cloudserver))"; status; exit 0; fi
    [ -d "$CLOUDSERVER_DIR" ] || die "no cloudserver checkout at $CLOUDSERVER_DIR"
    step "starting cloudserver on port $CLOUDSERVER_PORT"
    say "checkout: $CLOUDSERVER_DIR ($(git -C "$CLOUDSERVER_DIR" rev-parse --abbrev-ref HEAD))"
    say "metadata: mongodb://localhost:$MONGO_PORT (S3DATA=mem, S3VAULT=mem)"
    say "log:      $LOG"
    # the pidfile shim is preloaded for two reasons: it publishes the pid past
    # the tee pipeline, and it puts the demo conf path on the command line so
    # this process can never be confused with the live rig's cloudserver
    tmux_send cloudserver "export PATH=$NODE_BIN:\$PATH; cd $CLOUDSERVER_DIR && RIG_PIDFILE=$DEMO/run/cloudserver.pid S3METADATA=mongodb S3DATA=mem S3VAULT=mem REMOTE_MANAGEMENT_DISABLE=1 S3_CONFIG_FILE=$GENERATED/cloudserver-config.json node --require $SHIM_DIR/pidfile-shim.js index.js 2>&1 | tee -a $LOG"
    i=0
    while [ $i -lt 90 ]; do
        code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$S3/" || true)
        [ "$code" != "000" ] && [ -n "$code" ] && break
        zzz 2; i=$((i + 2))
    done
    if [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$S3/" || true)" = "000" ]; then
        warn "cloudserver did not answer within ${i}s: check the tmux window and $LOG"
        exit 1
    fi
    say "cloudserver answering after ${i}s"
    status
    ;;
stop)
    step "stopping cloudserver"
    tmux_interrupt cloudserver
    zzz 3
    p=$(pids_cloudserver)
    [ -n "$p" ] && { warn "still alive, sending TERM to $p"; kill -TERM $p 2>/dev/null; zzz 2; }
    status
    ;;
status) status ;;
*) die "usage: cloudserver.sh start|stop|status" ;;
esac
