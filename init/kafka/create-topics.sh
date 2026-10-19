#!/usr/bin/env bash
set -euo pipefail

BROKER="${KAFKA_BROKER:-kafka:29092}"
TOPICS=(equities.prices crypto.trades forex.rates macro.indicators)

until kafka-topics --bootstrap-server "$BROKER" --list >/dev/null 2>&1; do
  echo "Waiting for Kafka at $BROKER..."
  sleep 2
done

for topic in "${TOPICS[@]}"; do
  kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1
  echo "Topic $topic ready"
done
