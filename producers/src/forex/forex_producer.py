"""Poll Frankfurter (ECB) for live FX rates into the forex topic."""

import logging
import time

import requests

from .. import config
from ..base_producer import BaseProducer

log = logging.getLogger(__name__)

PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD"]
INTERVAL_SECONDS = 60


class ForexProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_FOREX)

    def run(self):
        while True:
            try:
                for pair in PAIRS:
                    resp = requests.get(
                        "https://api.frankfurter.dev/v1/latest",
                        params={"from": pair[:3], "to": pair[3:]},
                        timeout=10,
                    )
                    resp.raise_for_status()
                    self.publish(pair, {
                        "pair": pair,
                        "timestamp": int(time.time() * 1000),
                        "rate": resp.json()["rates"][pair[3:]],
                        "source": "frankfurter",
                    })
                self.flush()
            except Exception as e:
                log.error("Forex fetch failed: %s", e)
            time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ForexProducer().run()