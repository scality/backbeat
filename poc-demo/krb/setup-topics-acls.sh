#!/bin/bash
# Topics and ACLs on the Kerberos destination broker, applied over the
# plaintext VERIFY listener where the admin client is ANONYMOUS, a super
# user. Run inside the krb-kafka container:
#   docker exec -e KRB_VERIFY_PORT=$KRB_VERIFY_PORT bnaasdemo-krb-kafka-1 \
#     /demo/setup-topics-acls.sh
set -ex
B="localhost:${KRB_VERIFY_PORT:?KRB_VERIFY_PORT is required}"
K=/opt/kafka/bin
for t in topic-a topic-b; do
  $K/kafka-topics.sh --create --if-not-exists --bootstrap-server "$B" \
      --partitions 1 --replication-factor 1 --topic "$t"
done
$K/kafka-acls.sh --bootstrap-server "$B" --add --allow-principal User:notifa \
    --operation Write --operation Describe --topic topic-a
$K/kafka-acls.sh --bootstrap-server "$B" --add --allow-principal User:notifb \
    --operation Write --operation Describe --topic topic-b
$K/kafka-acls.sh --bootstrap-server "$B" --list
$K/kafka-topics.sh --bootstrap-server "$B" --list
