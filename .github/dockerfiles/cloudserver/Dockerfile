ARG CLOUDSERVER_IMAGE=ghcr.io/scality/cloudserver:7.70.47

FROM ${CLOUDSERVER_IMAGE}

ADD ./config.json /conf/config.json

# Update sources.list to use archive repositories for Buster
RUN sed -i 's/deb.debian.org/archive.debian.org/g' /etc/apt/sources.list \
    && sed -i 's/security.debian.org/archive.debian.org/g' /etc/apt/sources.list \
    && sed -i '/buster-updates/d' /etc/apt/sources.list \
    && apt update \
    && apt install -y curl \
    && apt clean
