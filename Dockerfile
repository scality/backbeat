ARG NODE_VERSION=24.21.0-bookworm-slim

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
RUN yarn install --frozen-lockfile --production --network-concurrency 1 \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/yarn-*

################################################################################
FROM builder AS compiler

# Install scripts are skipped: the compiler reads sources, it never loads any
# of them.
RUN yarn install --frozen-lockfile --ignore-scripts --network-concurrency 1

COPY . /usr/src/app/

RUN yarn build

# The IAM policy documents are used by ensureServiceUser, never imported, so the
# compiler leaves them behind.
RUN find policies extensions -name '*.json' -exec install -D {} dist/{} \;

################################################################################
FROM node:${NODE_VERSION}

# Kerberos runtime for Kafka destinations: kinit (krb5-user), libsasl2, and
# the Cyrus SASL GSSAPI plugin, loaded dynamically by librdkafka.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        krb5-user \
        libsasl2-2 \
        libsasl2-modules-gssapi-mit \
        openssl \
        tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY conf/ ./conf/
COPY --from=compiler /usr/src/app/dist/ ./
COPY --from=builder /usr/src/app/node_modules ./node_modules/
COPY --from=builder /usr/local/bin/dockerize /usr/local/bin/

# Expose the script without extension for backwards compatibility
RUN mv bin/ensureServiceUser.js bin/ensureServiceUser \
    && chmod +x bin/ensureServiceUser

ENV AWS_SDK_JS_SUPPRESS_MAINTENANCE_MODE_MESSAGE=1

ENTRYPOINT ["tini", "-g", "--"]

EXPOSE 8900
