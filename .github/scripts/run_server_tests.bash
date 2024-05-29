#!/bin/bash

set -x
set -eu -o pipefail

# port for backbeat server
PORT=8900

killandsleep () {
  kill -9 $(lsof -t -i:$PORT) || true
  sleep 10
}

./node_modules/.bin/nyc --clean --silent yarn start &
bash tests/utils/wait_for_local_port.bash $PORT 40
yarn run $1
killandsleep
./node_modules/.bin/nyc report --report-dir "./coverage/$1" --reporter=lcov
