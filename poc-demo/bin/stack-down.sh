#!/usr/bin/env bash
# Stop the BNaaS demo stack. Without --volumes the named volumes survive,
# so topics, offsets, consumer-group commits, mongo metadata and the
# prometheus history are all still there next time.
#
#   stack-down.sh [--volumes]
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

WITH_VOLUMES=0
for arg in "$@"; do
    case "$arg" in
        --volumes|-v) WITH_VOLUMES=1 ;;
        -h|--help) awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "$0"; exit 0 ;;
        *) die "unknown argument: $arg" ;;
    esac
done

load_env

say "stopping project ${PROJECT} (krb profile included, whether or not it is running)"
if [ "$WITH_VOLUMES" -eq 1 ]; then
    warn "removing named volumes: topics, consumer offsets, mongo metadata and prometheus history all go"
    dck down --volumes --remove-orphans
else
    dck down --remove-orphans
    info "named volumes kept. Use --volumes to wipe them."
fi

say "remaining containers for this project"
if docker ps -a --filter "label=com.docker.compose.project=${PROJECT}" --format '{{.Names}}' | grep -q .; then
    docker ps -a --filter "label=com.docker.compose.project=${PROJECT}" \
        --format '    {{.Names}}  {{.Status}}'
else
    info "none"
fi
say "volumes for this project"
if docker volume ls --filter "label=com.docker.compose.project=${PROJECT}" -q | grep -q .; then
    docker volume ls --filter "label=com.docker.compose.project=${PROJECT}" --format '    {{.Name}}'
else
    info "none"
fi
