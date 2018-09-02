#!/bin/bash

set -x
set -e

killandsleep () {
  kill -9 $(lsof -t -i:$1) || true
  sleep 10
}

if [ ! -d "node_modules/@zenko/cloudserver" ]; then
    echo "cloudserver module was not found!"
    exit 1
fi

# run s3 mem_backend
cd node_modules/@zenko/cloudserver && npm run mem_backend & bash tests/utils/wait_for_local_port.bash 8000 40 && npm run ft_test

killandsleep 8000

# run backbeat server
npm start & bash tests/utils/wait_for_local_port.bash 8900 40 && npm run ft_server_test

killandsleep 8900

exit $?
