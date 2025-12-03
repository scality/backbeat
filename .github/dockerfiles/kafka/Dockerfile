FROM eclipse-temurin:17-jre

# Install required tools
RUN apt-get update && \
    apt-get install -y wget netcat-traditional && \
    rm -rf /var/lib/apt/lists/*

# Download and install Kafka 3.9.0
ENV KAFKA_VERSION=3.9.0
ENV SCALA_VERSION=2.13
ENV KAFKA_HOME=/opt/kafka

RUN wget -q "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" && \
    tar -xzf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" && \
    mv "kafka_${SCALA_VERSION}-${KAFKA_VERSION}" ${KAFKA_HOME} && \
    rm "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"

# Add start script
COPY --chmod=755 start-kafka.sh /usr/local/bin/start-kafka.sh

RUN echo "message.max.bytes=5000020" >> ${KAFKA_HOME}/config/server.properties

WORKDIR ${KAFKA_HOME}

# Expose Kafka broker port
EXPOSE 9092

CMD ["/usr/local/bin/start-kafka.sh"]
