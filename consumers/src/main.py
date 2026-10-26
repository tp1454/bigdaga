"""Consume Kafka topics and write rows into ClickHouse."""

import json
import logging
import time
from datetime import date, datetime, timezone

from confluent_kafka import Consumer

from . import config
from .clickhouse_writer import ClickHouseWriter

log = logging.getLogger(__name__)


def _ms_to_dt(ms):
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


def _iso_to_dt(s):
    return datetime.fromisoformat(str(s).replace("Z", "+00:00"))


def _to_date(s):
    if not s:
        return None
    return date.fromisoformat(str(s)[:10])


def _sec_rows(d):
    """Flatten one SEC submission into one row per recent filing."""
    recent = d.get("filings") or {}
    accs = recent.get("accessionNumber") or []
    dates = recent.get("filingDate") or []
    forms = recent.get("form") or []
    docs = recent.get("primaryDocument") or []
    return [
        [
            d.get("cik"), d.get("entity_name"), d.get("sic"), acc,
            _to_date(dates[i] if i < len(dates) else None),
            forms[i] if i < len(forms) else None,
            docs[i] if i < len(docs) else None,
            d.get("source"),
        ]
        for i, acc in enumerate(accs)
    ]


TOPICS = {
    "equities.prices": (
        "equities_prices",
        ["symbol", "ts", "open", "high", "low", "close", "volume"],
        lambda d: [[d["symbol"], _iso_to_dt(d["ts"]), float(d["open"]), float(d["high"]),
                    float(d["low"]), float(d["close"]), int(d["volume"])]],
    ),
    "crypto.trades": (
        "crypto_trades",
        ["symbol", "timestamp", "price", "quantity", "trade_id", "source"],
        lambda d: [[d["symbol"], _ms_to_dt(d["timestamp"]), float(d["price"]),
                    float(d["quantity"]), int(d["trade_id"]), d["source"]]],
    ),
    "crypto.market": (
        "crypto_market_data",
        ["symbol", "timestamp", "price", "market_cap", "volume_24h", "source"],
        lambda d: [[d["symbol"], _ms_to_dt(d["timestamp"]), float(d["price"]),
                    d.get("market_cap"), d.get("volume_24h"), d["source"]]],
    ),
    "forex.rates": (
        "forex_rates",
        ["pair", "timestamp", "rate", "source"],
        lambda d: [[d["pair"], _ms_to_dt(d["timestamp"]), float(d["rate"]), d["source"]]],
    ),
    "macro.indicators": (
        "macro_indicators",
        ["indicator", "timestamp", "value", "source"],
        lambda d: [[d["indicator"], _to_date(d["timestamp"]), float(d["value"]), d["source"]]],
    ),
    "fundamentals.filings": (
        "company_filings",
        ["cik", "entity_name", "sic", "accession_number", "filing_date", "form",
         "primary_document", "source"],
        _sec_rows,
    ),
}


def main():
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BROKER,
        "group.id": config.GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(list(TOPICS.keys()))
    writer = ClickHouseWriter()
    buffers = {table: [] for table, _, _ in TOPICS.values()}
    columns = {table: cols for table, cols, _ in TOPICS.values()}
    last_flush = time.time()

    def flush():
        for table, rows in buffers.items():
            if rows:
                writer.insert(table, columns[table], rows)
                rows.clear()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.time() - last_flush >= config.FLUSH_INTERVAL_SECONDS:
                    flush()
                    last_flush = time.time()
                continue
            if msg.error():
                log.error("consumer error: %s", msg.error())
                continue
            table, _, fn = TOPICS.get(msg.topic(), (None, None, None))
            if table is None:
                continue
            try:
                record = json.loads(msg.value())
                rows = fn(record)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as e:
                log.warning("skip bad record on %s: %s", msg.topic(), e)
                continue
            if not rows:
                continue
            buffers[table].extend(rows)
            if time.time() - last_flush >= config.FLUSH_INTERVAL_SECONDS \
                    or len(buffers[table]) >= config.FLUSH_MAX_ROWS:
                flush()
                last_flush = time.time()
    finally:
        flush()
        consumer.close()
        writer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
    main()
