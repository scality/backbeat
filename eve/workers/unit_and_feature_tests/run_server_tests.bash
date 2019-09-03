#!/bin/bash

set -x
set -eu -o pipefail

# port for backbeat server
PORT=8900

trap killandsleep EXIT

killandsleep () {
  kill -9 $(lsof -t -i:$PORT) || true
  sleep 10
}

yarn start & bash tests/utils/wait_for_local_port.bash $PORT 40
yarn run $1
