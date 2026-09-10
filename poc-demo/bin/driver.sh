#!/bin/bash
# The workload driver, for driving load by hand. The suite runs the same
# code (tests/functional/demo/lib/driver-cli.js). It carries a monotonic sequence number in the object SIZE
# (size = 1000 + seq) and appends every completed operation to a log in
# completion order, which is what lets the checker reconstruct the expected
# per-key sequence from the delivered events alone.
#
# Usage:
#   driver.sh [--bg] [driver options...]   or  driver.sh stop|status
#
# Adds what the demo knows and the driver cannot: the S3 endpoint from
# demo/.env, and a default log under demo/logs. Everything else is passed
# straight through.
#
#   --bucket <b> | --buckets b1,b2,b3   where to write
#   --prefix <p>       object key prefix, also the checker's --key-prefix
#   --rate <n>         operations per second (one in flight at a time)
#   --duration <s> | --count <n>
#   --straddle <N> --straddle-every <K>
#       every Kth operation is a PUT-then-DELETE on one of N fixed keys, so
#       those keys have operations on both sides of any boundary in a
#       procedure. Straddle keys are the only ones that CAN show a per-key
#       inversion, so a scenario without them cannot measure ordering.
#   --seq-start <n>    continue a sequence
#   --log <file>
#
# --bg runs it in tmux window "driver" instead of in the foreground.
set -u
. "$(dirname "$0")/lib.sh"
need_node

if [ "${1:-}" = "stop" ]; then driver_stop; exit 0; fi
if [ "${1:-}" = "status" ]; then
    p=$(driver_pids)
    [ -n "$p" ] && say "driver RUNNING pid $p" || say "driver STOPPED"
    exit 0
fi

BG=0
if [ "${1:-}" = "--bg" ]; then BG=1; shift; fi

ARGS=("$@")
HAS_LOG=0; HAS_ENDPOINT=0
for a in "${ARGS[@]:-}"; do
    [ "$a" = "--log" ] && HAS_LOG=1
    [ "$a" = "--endpoint" ] && HAS_ENDPOINT=1
done
[ "$HAS_ENDPOINT" = 0 ] && ARGS+=(--endpoint "$S3")
[ "$HAS_LOG" = 0 ] && ARGS+=(--log "$DEMO_LOG_DIR/driver.log")

say "driver: $SUITE/lib/driver-cli.js ${ARGS[*]}"
say "S3 endpoint $S3 (keys $S3_ACCESS_KEY / $S3_SECRET_KEY)"

if [ "$BG" = 1 ]; then
    tmux_send driver "export PATH=$NODE_BIN:\$PATH; cd $DEMO && node $SUITE/lib/driver-cli.js ${ARGS[*]}"
    say "running in tmux window 'driver' of session $SESS"
else
    exec "$NODE" "$SUITE/lib/driver-cli.js" "${ARGS[@]}"
fi
