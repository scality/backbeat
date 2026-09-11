#!/usr/bin/env bash
# Stop every process that the demo suite or the rig scripts spawned on this
# host: populators, legacy processors, delivery workers, drivers. Use it after
# a run was interrupted in a way that Ctrl+C could not clean up (a closed
# terminal, a killed tmux window). The containers are not touched: use
# stack-down.sh for those.
#
#   stop-procs.sh            stop them, SIGTERM then KILL after 8 s
#   stop-procs.sh --list     only list what would be stopped
set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# Every demo process is started through the shims in poc-demo/conf/shims (the
# suite passes them with --require), or through a rig script from poc-demo/bin.
pattern='poc-demo/conf/shims/|poc-demo/bin/(populator|legacy-processor|worker|driver|consumer)\.sh|tests/functional/demo/lib/driver-cli'
pids=$(pgrep -f -- "$pattern" | grep -v "^$$\$" || true)
if [ -z "$pids" ]; then
    say "no demo process is running"
    exit 0
fi
if [ "${1:-}" = "--list" ]; then
    ps -o pid=,etime=,command= -p $(echo "$pids" | tr '\n' ',' | sed 's/,$//') | cut -c1-140
    exit 0
fi
say "stopping $(echo "$pids" | wc -l | tr -d ' ') demo process(es)"
kill $pids 2>/dev/null || true
for _ in $(seq 1 16); do
    left=$(pgrep -f -- "$pattern" || true)
    [ -z "$left" ] && break
    sleep 0.5
done
left=$(pgrep -f -- "$pattern" || true)
if [ -n "$left" ]; then
    warn "$(echo "$left" | wc -l | tr -d ' ') ignored SIGTERM, sending KILL"
    kill -9 $left 2>/dev/null || true
fi
rm -f "$DEMO"/run/*.pid 2>/dev/null
say "done. The single-run lock, if any, is taken over by the next run."
