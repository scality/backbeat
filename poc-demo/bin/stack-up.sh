#!/usr/bin/env bash
# Bring up the BNaaS demo stack: kafka, zookeeper, redis, a mongo replica
# set for CloudServer metadata, prometheus, grafana, kafka-ui and
# kafka-exporter. With --krb, also the Kerberos KDC and the SASL_GSSAPI
# destination broker.
#
# Idempotent: safe to re-run. It refuses to start if a port it wants is
# held by a container that is not ours, and names the owner.
#
#   stack-up.sh [--krb] [--no-topics]
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

WITH_KRB=0
DO_TOPICS=1
for arg in "$@"; do
    case "$arg" in
        --krb) WITH_KRB=1 ;;
        --no-topics) DO_TOPICS=0 ;;
        -h|--help) awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "$0"; exit 0 ;;
        *) die "unknown argument: $arg" ;;
    esac
done

load_env

say "BNaaS demo stack, project ${PROJECT}, PORT_OFFSET ${PORT_OFFSET}"

# ---- build local-only images ------------------------------------------
# poc-ft-kafka and ci-mongodb are built from .github/dockerfiles, not pulled
# from any registry, so a fresh machine has neither. Build the ones the
# daemon does not already have, so the whole stack comes up with one
# `yarn demo:up`. An existing image is left alone: this is not a rebuild.
for pair in "kafka:${KAFKA_IMAGE:-poc-ft-kafka:latest}" \
            "mongo:${MONGO_IMAGE:-ci-mongodb:latest}"; do
    svc="${pair%%:*}"
    img="${pair#*:}"
    if docker image inspect "$img" >/dev/null 2>&1; then
        info "$img present"
    else
        say "building $img from .github/dockerfiles ($svc): one-time, needs network"
        dck build "$svc" || die "could not build $img. See the build output above."
    fi
done

# The Kerberos functional test image (act 07) is built at setup time, not
# demo time, so it is behind --krb and does not slow an ordinary run.
if [ "$WITH_KRB" -eq 1 ] && [ -x "$DEMO_DIR/bin/krb-test-image.sh" ]; then
    "$DEMO_DIR/bin/krb-test-image.sh" || warn "the kerberos test image could not be built; act 07 will skip"
fi

# ---- port preflight ----------------------------------------------------
CHECK=(ZK_PORT KAFKA_PORT REDIS_PORT MONGO_PORT PROMETHEUS_PORT GRAFANA_PORT \
       KAFKA_UI_PORT KAFKA_EXPORTER_PORT ZOONAV_PORT CLOUDSERVER_PORT)
[ "$WITH_KRB" -eq 1 ] && CHECK+=(KRB_KDC_PORT KRB_BROKER_PORT KRB_VERIFY_PORT)

conflict=0
for name in "${CHECK[@]}"; do
    port="${!name}"
    if port_busy "$port"; then
        owner="$(port_owner "$port")"
        if [ -n "$owner" ] && [[ "$owner" == "${PROJECT}"* ]]; then
            info "$name $port already held by our own $owner"
        elif [ -n "$owner" ]; then
            warn "$name $port is held by container $owner, which is not ours"
            conflict=1
        else
            warn "$name $port is held by a process on the host, not a container"
            conflict=1
        fi
    fi
done
if [ "$conflict" -eq 1 ]; then
    die "port conflict. Raise PORT_OFFSET in $ENV_FILE and re-run, or stop the owner."
fi

# ---- cloudserver config ------------------------------------------------
# Rendered rather than static: the mongo address CloudServer uses carries
# the offset, because the replica set advertises its member on that port and
# a client dials the advertised address.
say "rendering cloudserver/config.json"
sed -e "s/__MONGO_PORT__/${MONGO_PORT}/g" \
    -e "s/__KAFKA_INTERNAL_PORT__/${KAFKA_INTERNAL_PORT}/g" \
    -e "s/__KRB_BROKER_PORT__/${KRB_BROKER_PORT}/g" \
    "$DEMO_DIR/cloudserver/config.json.tmpl" > "$DEMO_DIR/cloudserver/config.json"
python3 -c 'import json,sys; json.load(open(sys.argv[1]))' \
    "$DEMO_DIR/cloudserver/config.json" \
    || die "rendered cloudserver/config.json is not valid JSON"
info "mongo 127.0.0.1:${MONGO_PORT} through the socat alias, destinations poc-dest-1..3 plus krb-dest-a/b"

# ---- krb prep ----------------------------------------------------------
if [ "$WITH_KRB" -eq 1 ]; then
    mkdir -p "$DEMO_DIR/krb/keytabs"
    sed "s/__KRB_KDC_PORT__/${KRB_KDC_PORT}/" \
        "$DEMO_DIR/krb/krb5.conf.host.tmpl" > "$DEMO_DIR/krb/krb5.conf.host"
    info "rendered krb/krb5.conf.host with kdc 127.0.0.1:${KRB_KDC_PORT}"
fi

# ---- up ----------------------------------------------------------------
say "docker compose up"
if [ "$WITH_KRB" -eq 1 ]; then
    dck up -d
else
    dc up -d
fi


# ---- kafka stale-broker-znode recovery -------------------------------
# Topic metadata lives in zookeeper on this broker, so the zookeeper data
# directory is a named volume. That makes one failure mode likely: if
# zookeeper is restarted while the broker holds its ephemeral
# /brokers/ids/1 znode, zookeeper restores that znode from its snapshot and
# the broker cannot re-register, exiting with
# `NodeExistsException ... registerBroker`. The znode goes away by itself
# when the old session expires, about a minute. Restarting kafka alone,
# never zookeeper underneath it, avoids the whole thing.
zk_broker_ids() {
    docker exec -i "${PROJECT}-zookeeper-1" zkCli.sh -server localhost:2181 \
        ls /brokers/ids 2>/dev/null | grep -oE '^\[[^]]*\]$' | tail -1
}

kafka_recover_if_wedged() {
    local st ids deadline
    st="$(docker inspect -f '{{.State.Status}}' "$(kafka_ctr)" 2>/dev/null || echo missing)"
    [ "$st" = "running" ] && return 0
    if ! docker logs "$(kafka_ctr)" 2>&1 | tail -60 | grep -q 'registerBroker'; then
        return 0
    fi
    warn "kafka exited on NodeExistsException at registerBroker: zookeeper restored a stale ephemeral /brokers/ids entry from its snapshot"
    info "waiting for that zookeeper session to expire, then starting kafka again"
    deadline=$(( $(date +%s) + 180 ))
    while :; do
        ids="$(zk_broker_ids)"
        [ "$ids" = "[]" ] && break
        [ "$(date +%s)" -ge "$deadline" ] && die "/brokers/ids is still ${ids:-unknown} after 180s. Wait a minute and re-run stack-up.sh."
        info "/brokers/ids is ${ids:-unreadable}, waiting"
        sleep 5
    done
    info "/brokers/ids is [], starting kafka"
    dc up -d kafka
}

# ---- wait and initialise ----------------------------------------------
say "waiting for the broker"
if ! wait_for "kafka on localhost:${KAFKA_PORT}" 90 kafka_cli kafka-topics.sh --list; then
    kafka_recover_if_wedged
    wait_for "kafka on localhost:${KAFKA_PORT}" 180 kafka_cli kafka-topics.sh --list \
        || die "kafka is not answering. docker logs $(kafka_ctr)"
fi

say "mongo replica set"
"$DEMO_DIR/bin/mongo-init.sh"

if [ "$DO_TOPICS" -eq 1 ]; then
    say "topics"
    "$DEMO_DIR/bin/topics-create.sh"
else
    warn "skipping topic creation (--no-topics). Create them BEFORE starting any consumer."
fi

say "observability"
wait_for "prometheus" 90 bash -c \
    "curl -fsS http://localhost:${PROMETHEUS_PORT}/-/ready >/dev/null"
wait_for "grafana" 120 bash -c \
    "curl -fsS http://localhost:${GRAFANA_PORT}/api/health >/dev/null"
if curl -fsS "http://localhost:${GRAFANA_PORT}/api/search?query=BNaaS%20delivery%20pool" \
    | grep -q 'bnaas-delivery-pool'; then
    info 'dashboard "BNaaS delivery pool" provisioned'
else
    warn 'dashboard "BNaaS delivery pool" not visible yet; the file provider polls every 10s'
fi
wait_for "kafka-ui" 180 bash -c \
    "curl -fsS http://localhost:${KAFKA_UI_PORT}/actuator/health >/dev/null || curl -fsS http://localhost:${KAFKA_UI_PORT}/ >/dev/null"
wait_for "zoonavigator" 240 bash -c \
    "curl -fsS -o /dev/null http://localhost:${ZOONAV_PORT}/"

say "cloudserver"
# amd64 under emulation, so it is slower to start than everything else
# 403 to an unsigned GET / is a healthy answer, so assert on the S3
# response header rather than on the status code
if wait_for "cloudserver on localhost:${CLOUDSERVER_PORT}" 300 bash -c \
    "curl -sS -o /dev/null -D - --max-time 5 "http://localhost:${CLOUDSERVER_PORT}/" 2>/dev/null | grep -qi '^x-amz-request-id'"; then
    info "s3 endpoint http://localhost:${CLOUDSERVER_PORT}, accessKey1 / verySecretKey1"
else
    warn "cloudserver did not answer. docker logs $(cs_ctr)"
fi

if [ "$WITH_KRB" -eq 1 ]; then
    say "kerberos destination broker"
    wait_for "krb broker on localhost:${KRB_VERIFY_PORT}" 180 bash -c \
        "docker exec -i ${PROJECT}-krb-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:${KRB_VERIFY_PORT} --list >/dev/null"
    docker exec -i -e KRB_VERIFY_PORT="${KRB_VERIFY_PORT}" \
        "${PROJECT}-krb-kafka-1" /demo/setup-topics-acls.sh 2>&1 | sed 's/^/    /'
    info "keytabs in $DEMO_DIR/krb/keytabs, host krb5.conf in $DEMO_DIR/krb/krb5.conf.host"
    info "host producers: KRB5_CONFIG=$DEMO_DIR/krb/krb5.conf.host KRB5_CLIENT_KTNAME=$DEMO_DIR/krb/keytabs/merged.keytab KRB5CCNAME=DIR:<empty dir>"
    info "kerberos broker: localhost:${KRB_BROKER_PORT} (SASL_GSSAPI), localhost:${KRB_VERIFY_PORT} (plaintext readback)"
fi

say "up. URLs and endpoints:"
urls
say "next: yarn demo:wait, then the demo: DEMO_ACTS=02,03,04,05,06,08 yarn ft_test:demo (yarn demo:status shows what is up)"
info "prometheus scrapes the delivery workers at host.docker.internal:${DELIVERY_PROBE_PORT_BASE}..$(( DELIVERY_PROBE_PORT_BASE + DELIVERY_PROBE_PORT_COUNT - 1 ))"
info 'their probeServer bindAddress must be 0.0.0.0, not localhost, or the scrape cannot reach them'
