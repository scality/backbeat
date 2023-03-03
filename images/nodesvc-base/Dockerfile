FROM registry.scality.com/federation/nodesvc-base:7.10.5.0

WORKDIR ${HOME_DIR}/backbeat

COPY ./package.json ./yarn.lock ${HOME_DIR}/backbeat

# Remove when gitcache is sorted out
RUN rm /root/.gitconfig

RUN yarn install --production --frozen-lockfile --network-concurrency 1
RUN yarn cache clean --all

COPY . ${HOME_DIR}/backbeat

RUN chown -R ${USER} ${HOME_DIR}/backbeat

USER ${USER}

CMD bash -c "source ${CONF_DIR}/env && export && supervisord -c ${CONF_DIR}/${SUPERVISORD_CONF}"
