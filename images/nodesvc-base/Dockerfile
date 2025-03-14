FROM ghcr.io/scality/federation/nodesvc-base:7.10.7.0 as builder

WORKDIR /tmp/build

COPY ./package.json ./yarn.lock /tmp/build

RUN yarn cache clean \
    && yarn install --production --frozen-lockfile --ignore-optional --ignore-engines --network-concurrency 1 \
    && yarn cache clean

# Somehow chown in another layer will replace some node_modules built symlink with copy of file
# To keep the same output as previously
RUN chown -R ${USER} /tmp/build/node_modules

################################################################################
FROM ghcr.io/scality/federation/nodesvc-base:7.10.7.0

WORKDIR ${HOME_DIR}/backbeat
RUN chown -R ${USER} ${HOME_DIR}/backbeat

# Keep same output as chown command without group (use group 0)
COPY --chown=${USER}:0 . ${HOME_DIR}/backbeat
COPY --from=builder --chown=${USER}:0 /tmp/build/node_modules ./node_modules/

# All copied files are already chowned scality(1000):root(0)
# This removes a 763MB layer. Image goes from 2.34GB to 1.55GB
# Besides S3C Federation will chown scality(1001):scality(1001) at poststart
# TODO S3C-9479: Images will be reworked for size optimization
# RUN chown -R ${USER} ${HOME_DIR}/backbeat

USER ${USER}

CMD bash -c "ulimit -c 0 && source ${CONF_DIR}/env && export && supervisord -c ${CONF_DIR}/${SUPERVISORD_CONF}"
