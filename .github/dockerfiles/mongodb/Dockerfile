FROM mongo:8.0.32

# Workaround for SERVER-121912, drop when bumping to mongo >= 8.0.35
ENV GLIBC_TUNABLES=

# Set up directories and permissions
RUN mkdir -p /data/db /data/configdb && chown -R mongodb:mongodb /data/db /data/configdb

# copy the mongo config file
COPY /conf/mongod.conf /conf/mongod.conf
COPY /conf/mongo-run.sh /conf/mongo-run.sh
COPY /conf/initReplicaSet.js /conf/initReplicaSet.js

EXPOSE 27017/tcp
EXPOSE 27018

# Set up CMD
ENTRYPOINT ["bash", "/conf/mongo-run.sh"]
CMD ["bash", "/conf/mongo-run.sh"]
