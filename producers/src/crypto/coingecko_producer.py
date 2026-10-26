"""Poll CoinGecko for crypto market snapshots into the crypto topic."""

import logging
import time

import requests

from .. import config
from ..base_producer import BaseProducer

log = logging.getLogger(__name__)

COIN_IDS = ["bitcoin", "ethereum", "solana"]
INTERVAL_SECONDS = 300  # 5 minutes to respect free-tier limits


class CoinGeckoProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_CRYPTO_MARKET)

    def run(self):
        while True:
            try:
                url = "https://api.coingecko.com/api/v3/simple/price"
                params = {
                    "ids": ",".join(COIN_IDS),
                    "vs_currencies": "usd",
                    "include_market_cap": "true",
                    "include_24hr_vol": "true",
                }
                resp = requests.get(url, params=params, timeout=10)
                resp.raise_for_status()

                for coin_id, data in resp.json().items():
                    self.publish(coin_id, {
                        "symbol": coin_id,
                        "timestamp": int(time.time() * 1000),
                        "price": data["usd"],
                        "market_cap": data.get("usd_market_cap"),
                        "volume_24h": data.get("usd_24h_vol"),
                        "source": "coingecko",
                    })
                self.flush()
            except Exception as e:
                log.error("CoinGecko fetch failed: %s", e)
            time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    CoinGeckoProducer().run()