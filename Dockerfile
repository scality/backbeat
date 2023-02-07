ARG NODE_VERSION=16.19-bullseye-slim

FROM node:${NODE_VERSION} as builder

WORKDIR /usr/src/app

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
        libzstd-dev

ENV DOCKERIZE_VERSION v0.6.1

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

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        jq \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Keep the .git directory in order to properly report version
COPY . /usr/src/app
COPY --from=builder /usr/src/app/node_modules ./node_modules/
COPY --from=builder /usr/local/bin/dockerize /usr/local/bin/

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]

EXPOSE 8900
