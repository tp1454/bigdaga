"""Shared Kafka configuration: brokers and topic names."""

import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_EQUITIES = os.getenv("TOPIC_EQUITIES", "equities.prices")
TOPIC_CRYPTO = os.getenv("TOPIC_CRYPTO", "crypto.trades")
TOPIC_CRYPTO_MARKET = os.getenv("TOPIC_CRYPTO_MARKET", "crypto.market")
TOPIC_FOREX = os.getenv("TOPIC_FOREX", "forex.rates")
TOPIC_MACRO = os.getenv("TOPIC_MACRO", "macro.indicators")
TOPIC_FUNDAMENTALS = os.getenv("TOPIC_FUNDAMENTALS", "fundamentals.filings")