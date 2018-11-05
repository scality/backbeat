#!/bin/bash

set -x
set -eu -o pipefail

# port for cloudserver
PORT=8900

trap killandsleep EXIT

killandsleep () {
  kill -9 $(lsof -t -i:$PORT) || true
  sleep 10
}

npm start & bash tests/utils/wait_for_local_port.bash $PORT 40
npm run $1
