#!/bin/sh
# The dog: run watch_flock.py every 120 seconds and print its lines, one
# event per line, for the Claude Code Monitor tool. It never acts.
#   watch_flock.sh [--fresh] [--login <login>]
#                                      watch (refuses when a dog runs);
#                                      --fresh, for the first dog of a
#                                      session only, says every stray again
#   watch_flock.sh --running           exit 0 when a dog runs on this machine
# The state survives a re-arm, so a new dog does not replay old events.
DIR="${BNAAS_FLOCK_DIR:-$HOME/.bnaas/flock}"
PID="$DIR/dog.pid"
HERE=$(cd "$(dirname "$0")" && pwd)
# A dog that died without its trap (SIGKILL, a crash) leaves its pid, which
# the system can give to another process later: check that it is a dog.
running() {
  [ -s "$PID" ] && kill -0 "$(cat "$PID")" 2>/dev/null &&
    ps -p "$(cat "$PID")" -o command= 2>/dev/null | grep -q watch_flock
}
if [ "$1" = --running ]; then running; exit $?; fi
fresh=
if [ "$1" = --fresh ]; then fresh=--fresh; shift; fi
if running; then echo "flock: a dog already runs (pid $(cat "$PID"))"; exit 0; fi
mkdir -p "$DIR" && echo $$ > "$PID"
trap 'rm -f "$PID"' EXIT
# Run each step in the background and wait, so that a stop takes effect
# at once instead of after the pass or the sleep.
trap 'kill $! 2>/dev/null; exit 0' INT TERM HUP
# A failing pass is said once, when the exit status changes.
last=0
while :; do
  python3 -I "$HERE/watch_flock.py" $fresh "$@" 2>>"$DIR/errors.log" &
  fresh=
  wait $!
  rc=$?
  if [ "$rc" -ne "$last" ] && [ "$rc" -ne 0 ]; then
    echo "flock error: watch_flock.py exited $rc, see $DIR/errors.log"
  fi
  last=$rc
  sleep 120 &
  wait $!
done
