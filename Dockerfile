FROM node:8

ENV LD_LIBRARY_PATH "/usr/src/app/node_modules/node-rdkafka/build/Release ${LD_LIBRARY_PATH}"

WORKDIR /usr/src/app

RUN apt-get update \
    && apt-get install -y jq wget --no-install-recommends



ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

COPY package.json /usr/src/app
RUN npm install --production \
    && rm -rf /var/lib/apt/lists/* \
    && npm cache clear --force \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/npm-*


# Keep the .git directory in order to properly report version
COPY . /usr/src/app

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]

EXPOSE 8900
