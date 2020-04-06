FROM buildpack-deps:xenial-curl

#
# Install apt packages needed by backbeat and buildbot_worker
#
ENV LANG C.UTF-8
ENV PATH=$PATH:/backbeat/node_modules/.bin
ENV NODE_PATH=/backbeat/node_modules

COPY eve/workers/unit_and_feature_tests/backbeat_packages.list \
    eve/workers/unit_and_feature_tests/buildbot_worker_packages.list /tmp/

WORKDIR /backbeat

RUN curl -sS http://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - \
    && echo "deb http://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && cat /tmp/*packages.list | xargs apt-get install -y \
    && git clone https://github.com/tj/n.git \
    && make -C ./n \
    && n 10 latest \
    && pip install pip==9.0.1 \
    && rm -rf ./n \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /tmp/*packages.list

#
# Install yarn dependencies
#

COPY package.json yarn.lock /backbeat/

RUN yarn cache clean \
    && yarn install --frozen-lockfile \
    && yarn cache clean

#
# Add user eve
#
RUN adduser -u 1042 --home /home/eve --disabled-password --gecos "" eve \
    && adduser eve sudo \
    && sed -ri 's/(%sudo.*)ALL$/\1NOPASSWD:ALL/' /etc/sudoers

#
# Run buildbot-worker on startup through supervisor
#
ARG BUILDBOT_VERSION=0.9.1

RUN pip install buildbot-worker==$BUILDBOT_VERSION
ADD eve/workers/unit_and_feature_tests/supervisor/buildbot_worker.conf /etc/supervisor/conf.d/

CMD ["/bin/bash", "-l", "-c", "buildbot-worker create-worker . $BUILDMASTER:$BUILDMASTER_PORT $WORKERNAME $WORKERPASS   && buildbot-worker start --nodaemon"]
