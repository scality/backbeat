#!/usr/bin/env bash
# Delete the per-run topics and consumer groups the functional suites leave
# behind, so Kafka UI shows the demo's own topics and nothing else.
#
# tests/functional/deliverypool/* names every topic and group it creates
# after the run that created it (ftint-<what>-<epoch ms>, poc-bn-<what>-<epoch
# ms>), which is what keeps two runs from colliding. Nothing removes them
# afterwards, so a machine that has run the suite a few times shows a
# hundred topics next to the four that matter, and the first question in the
# room becomes "so how many topics does this design need?". One.
#
# Only prefixes the suites own are touched. Everything the demo itself uses
# is refused by name even if a prefix were mistyped, because this script
# deletes.
#
# Usage: clean-suite-topics.sh [--dry-run] [--prefix <p>]...
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
load_env

# what the functional suites name their per-run topics and groups
PREFIXES=(ftint- poc-bn-)
DRY=0

while [ $# -gt 0 ]; do
    case "$1" in
    --dry-run) DRY=1; shift ;;
    --prefix) [ $# -ge 2 ] || die "--prefix needs a value"; PREFIXES+=("$2"); shift 2 ;;
    -h|--help) sed -n '2,16p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument: $1" ;;
    esac
done

# never deleted, whatever a prefix matches. The demo's own topics, kafka's
# own, and the sanity-check topic backbeat creates for itself.
# every variable here is local: this is called from inside a loop over the
# topic list, and a helper that writes the caller's loop variable would have
# it delete whatever name it left behind instead of the one it was given
protected() {
    local name="$1" keep
    case "$name" in
    __consumer_offsets|backbeat-metrics|backbeat-sanitycheck) return 0 ;;
    "$LEGACY_TOPIC"|"$FAILED_TOPIC") return 0 ;;
    customer-topic-*) return 0 ;;
    esac
    for keep in $CUSTOMER_TOPICS; do
        [ "$name" = "$keep" ] && return 0
    done
    return 1
}

matches_prefix() {
    local name="$1" p
    for p in "${PREFIXES[@]}"; do
        case "$name" in "$p"*) return 0 ;; esac
    done
    return 1
}

wait_for "kafka broker" 60 kafka_cli kafka-topics.sh --list

say "cleaning suite leftovers on localhost:${KAFKA_PORT} (project ${PROJECT})"
info "prefixes: ${PREFIXES[*]}"
[ "$DRY" = 1 ] && info "dry run: nothing is deleted"

# groups first: a group is deleted only when it has no live member, and
# deleting its topic underneath it would leave it behind for good
groups=()
while read -r g; do
    [ -n "$g" ] || continue
    matches_prefix "$g" || continue
    groups+=("$g")
done < <(kafka_cli kafka-consumer-groups.sh --list 2>/dev/null | tr -d '\r' | sort)

deleted_groups=0
skipped_groups=0
for g in "${groups[@]:-}"; do
    [ -n "${g:-}" ] || continue
    if [ "$DRY" = 1 ]; then
        info "would delete group $g"
        continue
    fi
    if kafka_cli kafka-consumer-groups.sh --delete --group "$g" >/dev/null 2>&1; then
        deleted_groups=$(( deleted_groups + 1 ))
    else
        # almost always "the group is not empty", i.e. a suite is running
        warn "could not delete group $g (a consumer of it may still be live)"
        skipped_groups=$(( skipped_groups + 1 ))
    fi
done

topics=()
while read -r t; do
    [ -n "$t" ] || continue
    matches_prefix "$t" || continue
    if protected "$t"; then
        warn "refusing to delete $t: it is one of the demo's own topics"
        continue
    fi
    topics+=("$t")
done < <(kafka_cli kafka-topics.sh --list 2>/dev/null | tr -d '\r' | sort)

deleted_topics=0
for t in "${topics[@]:-}"; do
    [ -n "${t:-}" ] || continue
    if [ "$DRY" = 1 ]; then
        info "would delete topic $t"
        continue
    fi
    if kafka_cli kafka-topics.sh --delete --topic "$t" >/dev/null 2>&1; then
        deleted_topics=$(( deleted_topics + 1 ))
    else
        warn "could not delete topic $t"
    fi
done

if [ "$DRY" = 1 ]; then
    info "${#groups[@]} group(s) and ${#topics[@]} topic(s) match"
else
    info "deleted $deleted_groups group(s), $skipped_groups left alone"
    info "deleted $deleted_topics topic(s)"
fi

say "topics now on the broker"
kafka_cli kafka-topics.sh --list 2>/dev/null | tr -d '\r' | sort | sed 's/^/    /'
