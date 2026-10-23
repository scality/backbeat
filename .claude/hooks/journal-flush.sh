#!/bin/sh
# The D1 journal Stop and SubagentStop hook: posts the logged facts.
# It never blocks the session: errors go to errors.log, and it exits 0.
dir=${BNAAS_JOURNAL_DIR:-$HOME/.bnaas/journal}
mkdir -p "$dir" 2>/dev/null
python3 -I "$(dirname "$0")/journal" hook-flush 2>>"$dir/errors.log"
exit 0
