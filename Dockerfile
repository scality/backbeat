FROM node:6-slim

COPY . /opt/backbeat
WORKDIR /opt/backbeat

RUN apt-get update \
    && apt-get install -y jq python git build-essential --no-install-recommends \
    && npm install --production \
    && apt-get autoremove --purge -y python git build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && npm cache clear \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/npm-*

ENTRYPOINT ["./docker-entrypoint.sh"]
CMD [ "npm", "start" ]
