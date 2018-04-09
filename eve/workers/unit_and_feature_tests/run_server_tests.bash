#!/bin/bash

set -x
set -e

killandsleep () {
  kill -9 $(lsof -t -i:$1) || true
  sleep 10
}

# run s3 mem_backend
cd node_modules/s3 && npm run mem_backend & bash tests/utils/wait_for_local_port.bash 8000 40 && TEST_SWITCH=1 npm run ft_test

killandsleep 8000

# run backbeat server
TEST_SWITCH=1 npm start & bash tests/utils/wait_for_local_port.bash 8900 40 && npm run ft_server_test

killandsleep 8900

exit $?
