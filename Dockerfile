FROM node:8-alpine

WORKDIR /usr/src/app

RUN apk --no-cache add \
    bash \
    g++ \
    ca-certificates \
    lz4-dev \
    musl-dev \
    cyrus-sasl-dev \
    openssl-dev \
    make \
    python \
    jq

RUN apk add --no-cache --virtual .build-deps gcc zlib-dev libc-dev bsd-compat-headers py-setuptools bash git

ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz

# Keep the .git directory in order to properly report version
COPY . /usr/src/app

RUN npm install --production \
    && rm -rf /var/lib/apt/lists/* \
    && npm cache clear --force \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/npm-* \
    && apk del .build-deps

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]

EXPOSE 8900
