#!/bin/bash

set -x
set -e

killandsleep () {
  kill -9 $(lsof -t -i:$1) || true
  sleep 10
}

# run backbeat server
npm start & bash tests/utils/wait_for_local_port.bash 8900 40 && npm run $1

killandsleep 8900

exit $?
