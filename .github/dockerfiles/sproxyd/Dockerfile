ARG SPROXYD_VERSION=7.10.11.0

FROM ghcr.io/scality/federation/sproxyd:${SPROXYD_VERSION}

ADD ./sproxyd.conf /conf/sproxyd.conf

RUN chown root:root /conf/sproxyd.conf

RUN mkdir /var/run/scality/

RUN touch /var/run/scality/scality-sproxyd-adminapi
