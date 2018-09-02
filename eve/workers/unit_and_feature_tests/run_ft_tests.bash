#!/bin/bash

set -x
set -e

if [ ! -d "node_modules/@zenko/cloudserver" ]; then
    echo "cloudserver module was not found!"
    exit 1
fi


killandsleep () {
  kill -9 $(lsof -t -i:$1) || true
  sleep 10
}

cd node_modules/@zenko/cloudserver && npm run mem_backend & bash tests/utils/wait_for_local_port.bash 8000 40 && npm run ft_test

killandsleep 8000
