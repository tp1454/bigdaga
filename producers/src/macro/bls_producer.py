"""Fetch BLS economic indicators (CPI, unemployment) into the macro topic."""

import json
import logging
import time

import requests

from .. import config
from ..base_producer import BaseProducer

log = logging.getLogger(__name__)

SERIES = {
    "CUUR0000SA0": "CPI",
    "LNS14000000": "UNEMPLOYMENT",
}
INTERVAL_SECONDS = 86400  # daily


class BLSProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_MACRO)

    def run(self):
        while True:
            try:
                url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
                payload = {
                    "seriesid": list(SERIES.keys()),
                    "startyear": str(time.gmtime().tm_year - 1),
                    "endyear": str(time.gmtime().tm_year),
                }
                resp = requests.post(url, json=payload, timeout=15)
                resp.raise_for_status()

                for series in resp.json()["Results"]["series"]:
                    series_id = series["seriesID"]
                    for point in series["data"]:
                        self.publish(series_id, {
                            "indicator": SERIES.get(series_id, series_id),
                            "timestamp": f"{point['year']}-{point['period'][1:]}-01",
                            "value": float(point["value"]),
                            "source": "bls",
                        })
                self.flush()
            except Exception as e:
                log.error("BLS fetch failed: %s", e)
            time.sleep(INTERVAL_SECONDS)