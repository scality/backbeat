#!/usr/bin/env bash
# Block until the infrastructure the test suite needs is actually answering,
# then exit 0. Meant for a mocha before-hook, so it is quiet on success and
# says exactly what is missing on failure.
#
#   wait-ready.sh [--timeout SECONDS] [--quiet]
#
# Checks, in the order a suite depends on them: the kafka broker answers a
# metadata request, mongo reports itself PRIMARY, CloudServer answers on /,
# and grafana answers its health endpoint. Being up is not the same as
# being ready: the broker check lists topics rather than opening a socket,
# and the mongo check asks for the replica set state rather than pinging.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

TIMEOUT=180
QUIET=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --quiet|-q) QUIET=1; shift ;;
        -h|--help) awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "$0"; exit 0 ;;
        *) die "unknown argument: $1" ;;
    esac
done

load_env >/dev/null

note() { [ "$QUIET" -eq 1 ] || info "$@"; }

check_kafka() { kafka_cli kafka-topics.sh --list >/dev/null 2>&1; }

check_mongo() {
    docker exec -i "$(mongo_ctr)" mongo --port "$MONGO_PORT" --quiet \
        --eval 'rs.status().myState' 2>/dev/null | grep -q '^1$'
}

# CloudServer answers GET / with 403 for an unsigned request, which is a
# healthy answer, so `curl -f` would call a working server broken. Assert on
# the S3 response header: it proves the request was parsed and answered by
# CloudServer, not by whatever else might hold the port.
check_cloudserver() {
    curl -sS -o /dev/null -D - --max-time 5 \
        "http://localhost:${CLOUDSERVER_PORT}/" 2>/dev/null \
        | grep -qi '^x-amz-request-id'
}

check_grafana() {
    curl -fsS -o /dev/null --max-time 5 "http://localhost:${GRAFANA_PORT}/api/health" 2>/dev/null
}

deadline=$(( $(date +%s) + TIMEOUT ))
failed=""

for pair in "kafka broker on localhost:${KAFKA_PORT}:check_kafka" \
            "mongo PRIMARY on localhost:${MONGO_PORT}:check_mongo" \
            "cloudserver on localhost:${CLOUDSERVER_PORT}:check_cloudserver" \
            "grafana on localhost:${GRAFANA_PORT}:check_grafana"; do
    what="${pair%:*}"
    fn="${pair##*:}"
    while ! "$fn"; do
        if [ "$(date +%s)" -ge "$deadline" ]; then
            failed="$what"
            break
        fi
        sleep 2
    done
    if [ -n "$failed" ]; then
        warn "not ready after ${TIMEOUT}s: $failed"
        warn "yarn demo:status shows what is up"
        exit 1
    fi
    note "$what ready"
done

[ "$QUIET" -eq 1 ] || say "stack ready"
