"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from producers.models.producer import Producer
from producers.models.turnstile_hardware import TurnstileHardware

logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
                .replace("/", "_and_")
                .replace(" ", "_")
                .replace("-", "_")
                .replace("'", "")
        )

        super().__init__(
            "org.chicago.cta.turnstile.v1",  # following the pattern defined in server.py
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=1,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.info(f" sending notification about {num_entries} in station {self.station.name}")
        for _ in range(num_entries):
            self.producer.produce(
                topic=self.topic_name,
                key_schema=Turnstile.key_schema,
                value_schema=Turnstile.value_schema,
                key={"timestamp": self.time_millis()},
                value={
                    # TODO: Configure this
                    'station_id': self.station.station_id,
                    'station_name': self.station.name,
                    'line': self.station.color.name,
                },
            )
