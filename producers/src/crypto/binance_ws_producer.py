"""Stream Binance trade events into the crypto topic."""

import logging

from binance import ThreadedWebsocketManager

from .. import config
from ..base_producer import BaseProducer

log = logging.getLogger(__name__)

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


class BinanceProducer(BaseProducer):
    def __init__(self):
        super().__init__(config.TOPIC_CRYPTO)
        self.bm = ThreadedWebsocketManager()
        self.bm.start()

    def _handle_trade(self, msg):
        """Callback fired on every trade event from Binance."""
        if msg.get("e") != "trade":
            return
        data = msg["data"]
        self.publish(data["s"], {
            "symbol": data["s"],
            "timestamp": data["T"],
            "price": float(data["p"]),
            "quantity": float(data["q"]),
            "trade_id": data["t"],
            "source": "binance",
        })

    def run(self):
        for symbol in SYMBOLS:
            self.bm.start_trade_socket(symbol, self._handle_trade)
        self.bm.join()  # blocks forever, keeps the stream alive