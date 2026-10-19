"""Abstract producer: shared Kafka publish/delivery-report logic."""

import json
import logging
from abc import ABC, abstractmethod

from confluent_kafka import Producer

from . import config

log = logging.getLogger(__name__)


class BaseProducer(ABC):
    def __init__(self, topic):
        self.topic = topic
        self.producer = Producer({"bootstrap.servers": config.KAFKA_BROKER})

    def publish(self, key, value):
        self.producer.produce(
            self.topic,
            key=str(key),
            value=json.dumps(value),
            callback=self._delivery_report,
        )
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()

    @staticmethod
    def _delivery_report(err, msg):
        if err is not None:
            log.error("Delivery failed for %s: %s", msg.key(), err)
        else:
            log.debug("Delivered to %s[%d]", msg.topic(), msg.partition())

    @abstractmethod
    def run(self):
        """Produce records until interrupted."""
