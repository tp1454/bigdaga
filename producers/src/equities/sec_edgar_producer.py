"""Fetch SEC EDGAR company filings with proper rate limiting."""

import logging
import time

import requests

from .. import config
from ..base_producer import BaseProducer

log = logging.getLogger(__name__)

CIKS = ["0000320193"]  # Apple
USER_EMAIL = "you@example.com"
MIN_INTERVAL = 0.15  # ~6.6 req/s, safely under the 10 req/s limit


class SECEdgarProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_FUNDAMENTALS)
        self.headers = {"User-Agent": f"FinancialApp/1.0 ({USER_EMAIL})"}
        self._last_request = 0.0

    def _rate_limited_get(self, url):
        """Enforce a minimum gap between requests."""
        elapsed = time.time() - self._last_request
        if elapsed < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - elapsed)
        resp = requests.get(url, headers=self.headers, timeout=15)
        self._last_request = time.time()
        if resp.status_code == 429:
            log.warning("SEC rate limit hit, backing off 10s")
            time.sleep(10)
            return self._rate_limited_get(url)
        resp.raise_for_status()
        return resp.json()

    def run(self):
        while True:
            for cik in CIKS:
                try:
                    url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
                    data = self._rate_limited_get(url)
                    self.publish(cik, {
                        "cik": cik,
                        "entity_name": data.get("name"),
                        "sic": data.get("sic"),
                        "filings": data.get("filings", {}).get("recent", {}),
                        "source": "sec_edgar",
                    })
                except Exception as e:
                    log.error("SEC fetch failed for CIK %s: %s", cik, e)
            self.flush()
            time.sleep(3600)  # re-check hourly


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    SECEdgarProducer().run()