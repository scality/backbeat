#!/bin/bash
# The delivery checker, for looking at evidence by hand. The suite runs the
# same code (tests/functional/demo/lib/check.js), which was verified against
# the rig's scripts/check.py on the rig's own evidence.
#
# Usage:
#   check.sh <label> --events <dump> --driver <log> [--key-prefix p] [--bucket b]
#   check.sh <label> --dump <topic> <start-offset> [outfile] [partition]
#
# With no options it reads demo/evidence/<label>/{events.jsonl,driver.log}.
#
#   gaps        expected operations that never arrived, which is LOSS
#   dup_extras  extra copies of an operation
#   inversions  pairs of operations on one key delivered out of driver order
#   unexpected  delivered operations no driver log accounts for
set -u
. "$(dirname "$0")/lib.sh"
need_node

SC=${1:-}
[ -n "$SC" ] || die "usage: check.sh <label> [options], see the header"
shift
EV=$(evdir "$SC")

if [ "${1:-}" = "--dump" ]; then
    need_stack
    dump_topic "$2" "$3" "${4:-$EV/events.jsonl}" "${5:-0}" 30000
    exit 0
fi

if [ $# -eq 0 ]; then
    [ -f "$EV/events.jsonl" ] || die "$EV/events.jsonl not found. Dump it first: check.sh $SC --dump <topic> <start-offset>"
    set -- --events "$EV/events.jsonl" --driver "$EV/driver.log"
fi

step "checking $SC"
exec "$NODE" "$SUITE/lib/check-cli.js" --label "$SC" --out "$EV/checker.json" "$@"
