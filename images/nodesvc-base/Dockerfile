FROM registry.scality.com/federation/nodesvc-base:latest

COPY . ${HOME_DIR}/backbeat
RUN chown -R ${USER} ${HOME_DIR}/backbeat
USER ${USER}
WORKDIR ${HOME_DIR}/backbeat

CMD bash -c "source ${CONF_DIR}/env && export && supervisord -c ${CONF_DIR}/${SUPERVISORD_CONF}"
