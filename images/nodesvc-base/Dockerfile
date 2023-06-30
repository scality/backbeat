FROM registry.scality.com/federation/nodesvc-base:7.10.6.8

WORKDIR ${HOME_DIR}/backbeat

COPY ./package.json ./yarn.lock ${HOME_DIR}/backbeat

# Remove when gitcache is sorted out
RUN rm /root/.gitconfig

RUN  \
    apt-get update && \
    apt-get install -yq libssl-dev && \
    yarn install --production --frozen-lockfile --network-concurrency 1 && \
    yarn cache clean

COPY . ${HOME_DIR}/backbeat

RUN chown -R ${USER} ${HOME_DIR}/backbeat

USER ${USER}

CMD bash -c "source ${CONF_DIR}/env && export && supervisord -c ${CONF_DIR}/${SUPERVISORD_CONF}"
