FROM registry.scality.com/federation/nodesvc-base:7.10.7.0 as builder

WORKDIR /tmp/build

COPY ./package.json ./yarn.lock /tmp/build

RUN yarn cache clean \
    && yarn install --production --frozen-lockfile --ignore-optional --ignore-engines --network-concurrency 1 \
    && yarn cache clean

################################################################################
FROM registry.scality.com/federation/nodesvc-base:7.10.7.0

WORKDIR ${HOME_DIR}/backbeat

COPY . ${HOME_DIR}/backbeat
COPY --from=builder /tmp/build/node_modules ./node_modules/

RUN chown -R ${USER} ${HOME_DIR}/backbeat

USER ${USER}

CMD bash -c "source ${CONF_DIR}/env && export && supervisord -c ${CONF_DIR}/${SUPERVISORD_CONF}"
