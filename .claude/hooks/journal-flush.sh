#!/bin/sh
# The D1 journal Stop hook: puts the logged facts in a batch and asks the
# session to start the bnaas-journal agent on it in the background.
# Errors go to errors.log, and it always exits 0.
dir=${BNAAS_JOURNAL_DIR:-$HOME/.bnaas/journal}
mkdir -p "$dir" 2>/dev/null
python3 -I "$(dirname "$0")/journal" hook-flush 2>>"$dir/errors.log"
exit 0
