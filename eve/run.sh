#!/bin/bash

SCRIPT_FULL_PATH=$(readlink -f "$0")
SCRIPT_DIR=$(dirname "${SCRIPT_FULL_PATH}")
REPO_DIR="${SCRIPT_DIR}/.."

ARGS="$@"


# Build dependencies
docker build -t backbeat-worker:latest --file "${SCRIPT_DIR}/workers/unit_and_feature_tests/Dockerfile" ${REPO_DIR}
docker build -t backbeat-kafka:latest "${SCRIPT_DIR}/workers/kafka"

# Start ecosystem
docker run -d --rm --network=host --name backbeat-mongo scality/ci-mongo:3.6.8
docker run -d --rm --network=host --name backbeat-kafka backbeat-kafka
docker run -d --rm --network=host --name backbeat-redis redis:alpine

# Health Check kafka
bash ${REPO_DIR}/tests/utils/wait_for_local_port.bash 9092 60
# Health Check redis
bash ${REPO_DIR}/tests/utils/wait_for_local_port.bash 6379 60
# Health Check mongo
bash ${REPO_DIR}/tests/utils/wait_for_local_port.bash 27018 60

# Run tests
docker run -it --rm \
    -v ${REPO_DIR}:/home/eve/workspace \
    --network=host \
    --env CI="true" \
    --env BACKBEAT_CONFIG_FILE="tests/config.json" \
    backbeat-worker ${ARGS}

EXIT=$?

# Get ecosystem logs
docker logs backbeat-kafka >> ${SCRIPT_DIR}/.backbeat-kafka.log 2>&1
docker logs backbeat-redis >> ${SCRIPT_DIR}/.backbeat-redis.log 2>&1
docker logs backbeat-mongo >> ${SCRIPT_DIR}/.backbeat-mongo.log 2>&1

docker kill backbeat-mongo backbeat-kafka backbeat-redis

exit ${EXIT}
