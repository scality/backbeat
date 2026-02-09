ARG BACKBEAT_VERSION=latest
FROM ghcr.io/scality/backbeat:${BACKBEAT_VERSION} AS builder

####################################################################################################
FROM ghcr.io/scality/backbeat:${BACKBEAT_VERSION} AS supervisord

ARG SUPERVISORD_VERSION=0.7.3
ADD https://github.com/ochinchina/supervisord/releases/download/v${SUPERVISORD_VERSION}/supervisord_${SUPERVISORD_VERSION}_Linux_64-bit.tar.gz /tmp/supervisord.tar.gz
RUN tar -xzvf /tmp/supervisord.tar.gz -C /usr/local/bin --strip-components 1 --wildcards '*/supervisord'

####################################################################################################
FROM ghcr.io/scality/backbeat:${BACKBEAT_VERSION} AS ballot

ARG BALLOT_VERSION=1.0.4
ADD https://github.com/scality/ballot/releases/download/v${BALLOT_VERSION}/ballot-v${BALLOT_VERSION}-linux-amd64 /usr/src/app/ballot
RUN chmod +x /usr/src/app/ballot

####################################################################################################
FROM ghcr.io/scality/backbeat:${BACKBEAT_VERSION}

# Install external dependencies
COPY --from=ballot /usr/src/app/ballot /usr/local/bin/
COPY --from=supervisord /usr/local/bin/supervisord /usr/local/bin/

# Prepare runtime environment
ENV USER="scality"
ENV HOME_DIR="/home/${USER}" \
    LOG_DIR="/logs" \
    CONF_DIR="/conf" \
    DATA_DIR="/data" \
    SUP_RUN_DIR="/var/run/supervisor"

RUN mv /home/node ${HOME_DIR} && \
    useradd -ms /bin/bash ${USER} && \
    usermod -aG node ${USER} && \
    mkdir ${LOG_DIR} && chown ${USER} ${LOG_DIR} && \
    mkdir ${CONF_DIR} && chown ${USER} ${CONF_DIR} && \
    mkdir ${DATA_DIR} && chown ${USER} ${DATA_DIR} && \
    mkdir -m 777 ${SUP_RUN_DIR} && chown ${USER} ${SUP_RUN_DIR}

USER ${USER}
WORKDIR ${HOME_DIR}/backbeat

# Keep same output as chown command without group (use group 0)
COPY --chown=${USER}:0 --from=builder /usr/src/app ${HOME_DIR}/backbeat

# Run supervisord with the provided configuration (and remove entrypoint from parent image)
CMD [ "/bin/bash", "-c", "ulimit -c 0 && source ${CONF_DIR}/env && export && exec supervisord -c ${CONF_DIR}/${SUPERVISORD_CONF}" ]
ENTRYPOINT []
