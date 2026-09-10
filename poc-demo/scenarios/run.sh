#!/bin/bash
# Run the demo, or part of it. A thin wrapper around the test suite, which is
# the demo: `yarn ft_test:demo` from the backbeat repository.
#
#   run.sh                     every act, in order
#   run.sh 02 04               only those acts
#   run.sh legacy-baseline     an act by name
#   run.sh --list              what the acts are
#   DEMO_PACE=slow run.sh 06   slower waits, for recording
#
# The infrastructure has to be up first: demo/bin/stack-up.sh
set -u
. "$(dirname "$0")/../bin/lib.sh"
need_node
need_backbeat

ACTS_LIST="01 code-and-tests        commits, the file map, the unit suite and the linter
02 legacy-baseline       today's path end to end, 25 events, the event shape
03 dead-destination      the stall today, the counted drop on the pool
04 switch-and-drain      the cutover, worker first, and its mirror rollback
05 crashes               populator kill -9 twice, then a worker kill -9
06 workgroups            two workgroups, a death, a pin, a live reshard
07 kerberos              two Kerberos principals in one process
08 semantics             mixed window, detach, overlapping rules, collision"

if [ "${1:-}" = "--list" ]; then
    printf '%s\n' "$ACTS_LIST"
    exit 0
fi

WANT=""
for a in "$@"; do
    case "$a" in
        [0-9][0-9]) WANT="$WANT,$a" ;;
        *)
            id=$(printf '%s\n' "$ACTS_LIST" | awk -v n="$a" '$2==n {print $1}')
            [ -z "$id" ] && die "no act called $a. Try: run.sh --list"
            WANT="$WANT,$id"
            ;;
    esac
done
WANT=${WANT#,}

step "running the demo suite"
say "backbeat  $BACKBEAT_DIR"
say "acts      ${WANT:-all}"
say "pace      ${DEMO_PACE:-normal}"
say "watch     grafana http://localhost:$GRAFANA_PORT, kafka ui http://localhost:$KAFKA_UI_PORT"
say "evidence  $EVROOT/<act>/"

cd "$BACKBEAT_DIR" || die "cannot enter $BACKBEAT_DIR"
export PATH="$NODE_BIN:$PATH"
[ -n "$WANT" ] && export DEMO_ACTS="$WANT"
exec node_modules/.bin/mocha "$SUITE/demo.js" --timeout 1800000 --exit
