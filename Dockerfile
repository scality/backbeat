ARG NODE_VERSION=22.14.0-bookworm-slim

FROM node:${NODE_VERSION} AS builder

WORKDIR /usr/src/app

# libsasl2-dev is required at build time for node-rdkafka to compile
# librdkafka with SASL GSSAPI (Kerberos) support.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        build-essential \
        wget \
        bash \
        python3 \
        git \
        jq \
        zlib1g-dev \
        libncurses5-dev \
        libgdbm-dev \
        libnss3-dev \
        libssl-dev \
        libreadline-dev \
        libffi-dev \
        libzstd-dev \
        libsasl2-dev

ENV DOCKERIZE_VERSION=v0.6.1

RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

COPY package.json yarn.lock /usr/src/app/
RUN yarn install --ignore-engines --frozen-lockfile --production --network-concurrency 1 \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/yarn-*

################################################################################
FROM node:${NODE_VERSION}

# Kerberos runtime for Kafka destinations: kinit (krb5-user), libsasl2, and
# the Cyrus SASL GSSAPI plugin, loaded dynamically by librdkafka.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        jq \
        krb5-user \
        libsasl2-2 \
        libsasl2-modules-gssapi-mit \
        openssl \
        tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Keep the .git directory in order to properly report version
COPY . /usr/src/app
COPY --from=builder /usr/src/app/node_modules ./node_modules/
COPY --from=builder /usr/local/bin/dockerize /usr/local/bin/

ENV AWS_SDK_JS_SUPPRESS_MAINTENANCE_MODE_MESSAGE=1

ENTRYPOINT ["tini", "-g", "--", "/usr/src/app/docker-entrypoint.sh"]

EXPOSE 8900
