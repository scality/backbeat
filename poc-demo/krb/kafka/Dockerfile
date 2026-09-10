# Kafka 3.4.0 on Alpine with a JRE, for the Kerberos destination broker.
# Same image as bnaas-poc/krb-spike/rig/kafka/Dockerfile. On this machine
# the already-built bnaaskrb-kafka:latest is identical: name it as
# KRB_KAFKA_IMAGE in demo/.env to skip the build and the download.
FROM alpine:latest

RUN apk add --no-cache \
    openjdk17-jre \
    bash

RUN wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz \
    && tar -xzf kafka_2.13-3.4.0.tgz \
    && mv kafka_2.13-3.4.0 /opt/kafka \
    && rm kafka_2.13-3.4.0.tgz

ENV PATH="/opt/kafka/bin:$PATH"
WORKDIR /opt/kafka
