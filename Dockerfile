FROM node:10-slim

WORKDIR /usr/src/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    build-essential \
    libssl-dev \
    wget \
    bash \
    python \
    git \
    jq

ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

COPY package.json yarn.lock /usr/src/app/
RUN yarn install --frozen-lockfile --production \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/yarn-* \
    && mkdir /conf

COPY . /usr/src/app/

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]

EXPOSE 8900
