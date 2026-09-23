#!/usr/bin/env bash
# =============================================================================
# scripts/create-kafka-topics.sh
# Run inside container to create topics on both Kafka clusters
# =============================================================================

set -euo pipefail

export KAFKA_OPTS="-Djava.security.krb5.conf=/var/lib/secret/krb5.conf"

echo "=================================================="
echo "1. Creating topics on KAFKA-PLAIN (Port 29092)"
echo "=================================================="

PLAIN_TOPICS=(
  "rule_definitions:6"
  "data_mappers:6"
  "debezium_heartbeat:1"
  "GNOTIFY_SAVE_MESSAGE_HBASE:6"
  "history_service_insert_hbase_product:6"
  "HISTORY_SERVICE_INSERT_HBASE_OBJECT:6"
  "cdcn_log_central_prod:6"
  "ADS-THIRD-PARTY-GIFT-DATA-RESULT-CMD:6"
  "core-recharge-history:6"
  "result:6"
  "dlq:6"
)

for item in "${PLAIN_TOPICS[@]}"; do
  TOPIC="${item%%:*}"
  PARTITIONS="${item##*:}"
  echo "Creating topic '$TOPIC' ($PARTITIONS partitions)..."
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-plain:29092 \
    --command-config /etc/kafka/secrets/plain/client.properties \
    --topic "$TOPIC" \
    --create \
    --if-not-exists \
    --partitions "$PARTITIONS" \
    --replication-factor 1
done

echo ""
echo "=================================================="
echo "2. Creating topics on KAFKA-GSSAPI (Port 29094)"
echo "=================================================="

GSSAPI_TOPICS=(
  "schema_registry:6"
  "V1-UPDATE-TRANS-DAILY-HIS:6"
  "P1-EVENT-TRACKING:6"
  "V1-UPDATE-TRANS-DAILY-HIS:6"
  "V1-INSERT-TRANS-DAILY-HIS:6"
  "debezium_heartbeat:1"
)

for item in "${GSSAPI_TOPICS[@]}"; do
  TOPIC="${item%%:*}"
  PARTITIONS="${item##*:}"
  echo "Creating topic '$TOPIC' ($PARTITIONS partitions)..."
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-gssapi:29094 \
    --command-config /etc/kafka/secrets/gssapi/client.properties \
    --topic "$TOPIC" \
    --create \
    --if-not-exists \
    --partitions "$PARTITIONS" \
    --replication-factor 1
done

echo ""
echo "All topics successfully initialized on both clusters!"