FROM buildpack-deps:xenial-curl

#
# Install apt packages needed by backbeat and buildbot_worker
#
COPY ./backbeat_packages.list ./buildbot_worker_packages.list /tmp/

RUN curl -sL https://deb.nodesource.com/setup_10.x | bash - \
    && cat /tmp/*packages.list | xargs apt-get install -y \
    && pip install pip==9.0.1 \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /tmp/*packages.list

#
# Add user eve
#
RUN adduser -u 1042 --home /home/eve --disabled-password --gecos "" eve \
    && adduser eve sudo \
    && sed -ri 's/(%sudo.*)ALL$/\1NOPASSWD:ALL/' /etc/sudoers

#
# Run buildbot-worker on startup through supervisor
#
ARG BUILDBOT_VERSION=0.9.7

RUN pip install buildbot-worker==$BUILDBOT_VERSION
ADD supervisor/buildbot_worker.conf /etc/supervisor/conf.d/
