"""Poll FreeForexAPI for live FX rates into the forex topic."""

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
                url = "https://www.freeforexapi.com/api/live"
                params = {"pairs": ",".join(PAIRS)}
                resp = requests.get(url, params=params, timeout=10)
                resp.raise_for_status()

                for pair, data in resp.json()["rates"].items():
                    self.publish(pair, {
                        "pair": pair,
                        "timestamp": int(time.time() * 1000),
                        "rate": data["rate"],
                        "source": "freeforexapi",
                    })
                self.flush()
            except Exception as e:
                log.error("Forex fetch failed: %s", e)
            time.sleep(INTERVAL_SECONDS)