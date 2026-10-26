"""Shared configuration for the Kafka -> ClickHouse consumer."""

import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
GROUP_ID = os.getenv("CONSUMER_GROUP", "clickhouse-writer")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "analytics")

FLUSH_INTERVAL_SECONDS = float(os.getenv("FLUSH_INTERVAL_SECONDS", "5"))
FLUSH_MAX_ROWS = int(os.getenv("FLUSH_MAX_ROWS", "1000"))
