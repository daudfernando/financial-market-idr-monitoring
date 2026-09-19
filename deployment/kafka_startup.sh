#!/bin/bash
# Container-Optimized OS startup script. Kafka binds its private NIC only.
set -euo pipefail
PRIVATE_IP=$(curl --fail --silent --show-error -H 'Metadata-Flavor: Google' \
  http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip)
DATA_DIR=/var/lib/daud-kafka
mkdir -p "$DATA_DIR"
chown 1000:1000 "$DATA_DIR"
if ! docker container inspect daud-kafka >/dev/null 2>&1; then
  docker run -d --name daud-kafka --restart unless-stopped --network host \
    -v "$DATA_DIR:/tmp/kraft-combined-logs" \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e "KAFKA_LISTENERS=INTERNAL://${PRIVATE_IP}:9092,CONTROLLER://127.0.0.1:9093" \
    -e "KAFKA_ADVERTISED_LISTENERS=INTERNAL://${PRIVATE_IP}:9092" \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
    -e KAFKA_LOG_RETENTION_HOURS=24 -e KAFKA_LOG_DIRS=/tmp/kraft-combined-logs \
    -e 'KAFKA_HEAP_OPTS=-Xms256m -Xmx512m' apache/kafka:3.9.1
else
  docker start daud-kafka
fi
ready=false
for attempt in $(seq 1 36); do
  if docker exec daud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$PRIVATE_IP:9092" --list; then
    ready=true
    break
  fi
  sleep 5
done
"$ready" || { echo 'Kafka did not become ready'; exit 1; }
for topic in market.prices.replay.v1 market.prices.live.v1; do
  docker exec daud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$PRIVATE_IP:9092" \
    --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
done
