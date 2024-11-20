#!/bin/bash

set -x
set -eu -o pipefail

NODE_PATH=${NODE_PATH:-node_modules}
# port for cloudserver
PORT=8000

trap killandsleep EXIT

killandsleep () {
  kill -9 $(lsof -t -i:$PORT) || true
  sleep 10
}

docker-compose -f .github/dockerfiles/cloudserver/docker-compose.yml up & bash tests/utils/wait_for_local_port.bash $PORT 40
./node_modules/.bin/nyc --clean --silent yarn run $1
./node_modules/.bin/nyc report --report-dir "./coverage/$1" --reporter=lcov
