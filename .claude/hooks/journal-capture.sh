#!/bin/sh
# The D1 journal PostToolUse hook: logs the facts of a tool call.
# It never blocks the session: it exits 0.
# journal finds the data folder (BNAAS_HOME); without it, it does nothing.
# A crash before its own error handling still lands in errors.log.
case $BNAAS_HOME in /*) err=$BNAAS_HOME/journal/errors.log; mkdir -p "${err%/*}" 2>/dev/null ;; *) err=/dev/null ;; esac
python3 -I "$(dirname "$0")/journal" capture 2>>"$err"
exit 0
