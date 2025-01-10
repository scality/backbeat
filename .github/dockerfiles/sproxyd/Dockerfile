ARG SPROXYD_VERSION=7.10.6.8

FROM ghcr.io/scality/federation/sproxyd:${SPROXYD_VERSION}

ADD ./conf /conf/

RUN chown root:root /conf/sproxyd.conf
