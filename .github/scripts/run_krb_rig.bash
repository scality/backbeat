#!/bin/bash
# Kerberos rig for the delivery pool Kerberos suite, on a Linux runner.
#
# Three plain containers on the host network: ZooKeeper, a MIT KDC (realm
# SCALITY.TEST, broker principal kafka/localhost, client principals notifa and
# notifb, keytabs written to $KRB_DIR/keytabs), and a Kafka 3.4 broker with a
# SASL_PLAINTEXT/GSSAPI listener on 19095, a plaintext listener on 19096 and
# ACLs binding each principal to its topic. Host networking keeps every
# address at localhost, which is what the broker principal and the advertised
# listeners name, for the containers and for the test process alike.
#
#   run_krb_rig.bash up      build the images, start the rig, write CONF_DIR
#   run_krb_rig.bash probe   kinit and kvno from the runner, with KRB5_TRACE
#   run_krb_rig.bash logs    copy the container logs under $KRB_DIR/logs
#   run_krb_rig.bash down    remove the containers
#
# The images and their entrypoints are the demo's, under poc-demo/krb.
set -eu -o pipefail

KRB_DIR="${KRB_DIR:-${RUNNER_TEMP:-/tmp}/krb}"
KRB_SRC="${KRB_SRC:-poc-demo/krb}"
KRB_BROKER_PORT="${KRB_BROKER_PORT:-19095}"
KRB_VERIFY_PORT="${KRB_VERIFY_PORT:-19096}"
ZK_IMAGE="${ZK_IMAGE:-zookeeper:3.9.4}"

wait_for_port() {
    local port=$1 what=$2 tries=${3:-90}
    for _ in $(seq 1 "$tries"); do
        if nc -z localhost "$port" 2>/dev/null; then
            echo "$what is listening on $port"
            return 0
        fi
        sleep 1
    done
    echo "timed out waiting for $what on port $port" >&2
    return 1
}

up() {
    mkdir -p "$KRB_DIR/keytabs" "$KRB_DIR/ssl" "$KRB_DIR/logs"
    chmod 0777 "$KRB_DIR/keytabs"

    docker build -t krb-kdc -f "$KRB_SRC/Dockerfile.kdc" "$KRB_SRC"
    docker build -t krb-kafka "$KRB_SRC/kafka"

    docker run -d --name krb-zk --network host \
        -e ZOOKEEPER_CLIENT_PORT=2181 -e ZOOKEEPER_TICK_TIME=2000 \
        "$ZK_IMAGE"
    wait_for_port 2181 zookeeper

    docker run -d --name krb-kdc --network host \
        -v "$KRB_DIR/keytabs:/keytabs" \
        krb-kdc
    for _ in $(seq 1 60); do
        [ -s "$KRB_DIR/keytabs/merged.keytab" ] && break
        sleep 1
    done
    [ -s "$KRB_DIR/keytabs/merged.keytab" ] || {
        echo "the KDC did not write its keytabs" >&2
        docker logs krb-kdc >&2 || true
        exit 1
    }
    wait_for_port 1088 kdc

    docker run -d --name krb-kafka --network host \
        -e KAFKA_OPTS=-Djava.security.krb5.conf=/etc/krb5.conf \
        -e KRB_BROKER_PORT="$KRB_BROKER_PORT" \
        -e KRB_VERIFY_PORT="$KRB_VERIFY_PORT" \
        -v "$KRB_DIR/keytabs:/keytabs:ro" \
        -v "$PWD/$KRB_SRC/krb5.conf:/etc/krb5.conf:ro" \
        -v "$PWD/$KRB_SRC/server-kerberos.properties.tmpl:/demo/server-kerberos.properties.tmpl:ro" \
        -v "$PWD/$KRB_SRC/broker-entrypoint.sh:/demo/broker-entrypoint.sh:ro" \
        -v "$PWD/$KRB_SRC/setup-topics-acls.sh:/demo/setup-topics-acls.sh:ro" \
        krb-kafka /demo/broker-entrypoint.sh
    wait_for_port "$KRB_VERIFY_PORT" "kerberos broker (plaintext listener)" 120
    wait_for_port "$KRB_BROKER_PORT" "kerberos broker (GSSAPI listener)" 60
    # the admin client on the plaintext listener is ANONYMOUS, a super user
    docker exec -e KRB_VERIFY_PORT="$KRB_VERIFY_PORT" krb-kafka /demo/setup-topics-acls.sh

    # the suite reads CONF_DIR/ssl/<principal>.keytab
    cp "$KRB_DIR"/keytabs/*.keytab "$KRB_DIR/ssl/"
    klist -kte "$KRB_DIR/ssl/notifa.keytab"
    write_krb5_conf
    echo "rig up: CONF_DIR=$KRB_DIR KRB5_CONFIG=$KRB_DIR/krb5.conf brokers localhost:$KRB_BROKER_PORT verify localhost:$KRB_VERIFY_PORT"
}

# The runner's krb5.conf is the rig's, plus one line. MIT krb5 1.18 and later
# qualify a single-label hostname with the machine's DNS domain before mapping
# it to a realm (qualify_shortname), so on a cloud runner "localhost" became
# "localhost.<runner domain>", the realm became the uppercased runner domain,
# and every client asked the KDC for a cross-realm ticket it does not have
# ("Server krbtgt/<RUNNER DOMAIN>@SCALITY.TEST not found"). An empty
# qualify_shortname (written as "" since a bare empty value does not parse)
# keeps "localhost" as it is, which is what the broker
# principal kafka/localhost and the domain_realm section name. Containers
# never hit this because they have no search domain.
write_krb5_conf() {
    awk '
        { print }
        /^\[libdefaults\]/ { print "    qualify_shortname = \"\"" }
    ' "$KRB_SRC/krb5.conf" > "$KRB_DIR/krb5.conf"
    echo "--- $KRB_DIR/krb5.conf"; cat "$KRB_DIR/krb5.conf"
}

# Fail fast, with the library's own trace, before the suite runs: a ticket for
# notifa from its keytab, then a service ticket for the broker principal.
probe() {
    export KRB5_CONFIG="$KRB_DIR/krb5.conf"
    export KRB5CCNAME="FILE:$KRB_DIR/probe.cc"
    echo "hostname -f: $(hostname -f 2>/dev/null || hostname)"
    echo "getent hosts localhost: $(getent hosts localhost | tr '\n' ' ')"
    KRB5_TRACE=/dev/stderr kinit -kt "$KRB_DIR/ssl/notifa.keytab" notifa@SCALITY.TEST
    KRB5_TRACE=/dev/stderr kvno -S kafka localhost
    klist
    kdestroy || true
}

logs() {
    mkdir -p "$KRB_DIR/logs"
    for c in krb-zk krb-kdc krb-kafka; do
        docker logs "$c" > "$KRB_DIR/logs/$c.log" 2>&1 || true
    done
    docker ps -a > "$KRB_DIR/logs/docker-ps.txt" 2>&1 || true
}

down() {
    docker rm -f krb-kafka krb-kdc krb-zk >/dev/null 2>&1 || true
}

case "${1:-}" in
    up) up ;;
    probe) probe ;;
    logs) logs ;;
    down) down ;;
    *) echo "usage: $0 up|probe|logs|down" >&2; exit 2 ;;
esac
