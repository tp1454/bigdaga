"""Shared Kafka configuration: brokers and topic names."""

import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_EQUITIES = os.getenv("TOPIC_EQUITIES", "equities.prices")
