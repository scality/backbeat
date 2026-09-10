#!/usr/bin/env bash
# Initiate the single-member replica set CloudServer's metadata backend
# needs, with the member host pinned to the port mongod actually runs on.
#
# Why not the ci-mongodb image's own initReplicaSet.js: it pins the member
# to 127.0.0.1:27018, and the address in the replica-set config is what the
# driver handshake hands back to clients, which then use it. Host port
# 27018 belongs to another workstream's container on this machine, so a
# remapped port would silently point CloudServer and backbeat at the wrong
# database. Running mongod on ${MONGO_PORT} with a matching member host is
# what makes the handshake resolve correctly.
#
# Idempotent: re-running against an initiated set only reports its state.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
load_env

CTR="$(mongo_ctr)"
say "mongo replica set rs0 on 127.0.0.1:${MONGO_PORT} (container ${CTR})"

mongo_eval() {
    docker exec -i "$CTR" mongo --port "$MONGO_PORT" --quiet --eval "$1"
}

wait_for "mongod on ${MONGO_PORT}" 120 \
    docker exec -i "$CTR" mongo --port "$MONGO_PORT" --quiet --eval 'db.adminCommand({ping:1}).ok'

state="$(mongo_eval 'try { rs.status().myState } catch (e) { print(-1) }' | tr -d '[:space:]')"
if [ "$state" = "1" ]; then
    info "already PRIMARY, nothing to do"
else
    info "initiating rs0 with member 127.0.0.1:${MONGO_PORT}"
    mongo_eval "rs.initiate({_id:'rs0',members:[{_id:0,host:'127.0.0.1:${MONGO_PORT}'}]})" | sed 's/^/    /'
    wait_for "rs0 PRIMARY" 60 bash -c \
        "docker exec -i '$CTR' mongo --port '$MONGO_PORT' --quiet --eval 'rs.status().myState' | grep -q '^1$'"
fi

say "replica set state"
mongo_eval 'JSON.stringify({myState: rs.status().myState, members: rs.status().members.map(function (m) { return m.name + " " + m.stateStr; })})' \
    | sed 's/^/    /'
info "connection string: mongodb://localhost:${MONGO_PORT}/?replicaSet=rs0  (database metadata)"
