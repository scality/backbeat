FROM spotify/kafka

#
# Install apt packages needed by backbeat and buildbot_worker
#
COPY ./backbeat_packages.list ./buildbot_worker_packages.list /tmp/

# Workaround since Jessie release has been archived recently
# (https://lists.debian.org/debian-devel-announce/2019/03/msg00006.html)
RUN rm -f /etc/apt/sources.list.d/jessie-backports.list

RUN curl -sL https://deb.nodesource.com/setup_8.x | bash - \
    && curl -fsSL https://get.docker.com | bash - \
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
ARG BUILDBOT_VERSION=0.9.1

RUN pip install buildbot-worker==$BUILDBOT_VERSION
ADD supervisor/buildbot_worker.conf /etc/supervisor/conf.d/

RUN echo "message.max.bytes=5000020" \
    >> /opt/kafka_2.11-0.10.1.0/config/server.properties
