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
        self.publish(msg["s"], {
            "symbol": msg["s"],
            "timestamp": msg["T"],
            "price": float(msg["p"]),
            "quantity": float(msg["q"]),
            "trade_id": msg["t"],
            "source": "binance",
        })

    def run(self):
        for symbol in SYMBOLS:
            self.bm.start_trade_socket(symbol=symbol, callback=self._handle_trade)
        self.bm.join()  # blocks until sockets close; the library auto-reconnects

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
    BinanceProducer().run()