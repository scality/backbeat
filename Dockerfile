FROM node:8

WORKDIR /usr/src/app

RUN apt-get update \
    && apt-get install -y jq --no-install-recommends

# Keep the .git directory in order to properly report version
COPY . /usr/src/app

RUN npm install --production \
    && rm -rf /var/lib/apt/lists/* \
    && npm cache clear --force \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/npm-*

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]

EXPOSE 8900
