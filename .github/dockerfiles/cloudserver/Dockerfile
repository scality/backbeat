ARG CLOUDSERVER_IMAGE=ghcr.io/scality/cloudserver:7.70.47

FROM ${CLOUDSERVER_IMAGE}

ADD ./config.json /conf/config.json

RUN apt update \
    && apt install -y curl \
    && apt clean
