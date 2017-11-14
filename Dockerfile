FROM node:6-slim

WORKDIR /usr/src/app

# Keep the .git directory in order to properly report version
COPY . /usr/src/app

RUN apt-get update \
    && apt-get install -y jq python git build-essential --no-install-recommends \
    && npm install --production \
    && apt-get autoremove --purge -y python git build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && npm cache clear \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/npm-*

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]

EXPOSE 8000
