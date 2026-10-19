"""Poll Yahoo Finance 1m bars into the equities topic."""

import logging
import time

import yfinance as yf

from .. import config
from ..base_producer import BaseProducer

log = logging.getLogger(__name__)

SYMBOLS = ["AAPL", "MSFT", "GOOG"]
INTERVAL_SECONDS = 15


class YahooProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_EQUITIES)

    def run(self):
        while True:
            for symbol in SYMBOLS:
                history = yf.Ticker(symbol).history(period="1d", interval="1m")
                for ts, row in history.iterrows():
                    # numpy scalars aren't JSON-serializable
                    self.publish(symbol, {
                        "symbol": symbol,
                        "ts": ts.isoformat(),
                        "open": float(row["Open"]),
                        "high": float(row["High"]),
                        "low": float(row["Low"]),
                        "close": float(row["Close"]),
                        "volume": int(row["Volume"]),
                    })
            self.flush()
            time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    YahooProducer().run()
