#!/bin/sh
# The D1 journal Stop hook: puts the logged facts in a batch and asks the
# session to start the bnaas-journal agent on it in the background.
# It always exits 0.
# journal finds the data folder (BNAAS_HOME); without it, it does nothing.
# A crash before its own error handling still lands in errors.log.
case $BNAAS_HOME in /*) err=$BNAAS_HOME/journal/errors.log; mkdir -p "${err%/*}" 2>/dev/null ;; *) err=/dev/null ;; esac
python3 -I "$(dirname "$0")/journal" hook-flush 2>>"$err"
exit 0
