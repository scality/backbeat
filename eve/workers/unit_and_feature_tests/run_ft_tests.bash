#!/bin/bash

set -x
set -eu -o pipefail

# port for cloudserver
PORT=8000

if [ ! -d "node_modules/@zenko/cloudserver" ]; then
    echo "cloudserver module was not found!"
    exit 1
fi

trap killandsleep EXIT

killandsleep () {
  kill -9 $(lsof -t -i:$PORT) || true
  sleep 10
}

cd node_modules/@zenko/cloudserver && yarn run mem_backend & bash tests/utils/wait_for_local_port.bash $PORT 40
yarn run $1
