FROM registry.scality.com/federation/nodesvc-base:latest

WORKDIR ${HOME_DIR}/backbeat

COPY ./package.json ./yarn.lock ${HOME_DIR}/backbeat

RUN yarn install --production --frozen-lockfile --network-concurrency 1

COPY . ${HOME_DIR}/backbeat

RUN chown -R ${USER} ${HOME_DIR}/backbeat

USER ${USER}

CMD bash -c "source ${CONF_DIR}/env && export && supervisord -c ${CONF_DIR}/${SUPERVISORD_CONF}"
