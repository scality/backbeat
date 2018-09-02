#!/bin/bash

set -x
set -e

killandsleep () {
  kill -9 $(lsof -t -i:$1) || true
  sleep 10
}

npm start & bash tests/utils/wait_for_local_port.bash 8900 40 && npm run ft_server_test

killandsleep 8900
