"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(self, topic_name, key_schema, value_schema=None, num_partitions=1, num_replicas=1):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094",
            "schema.registry.url": "http://localhost:8081/"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(self.broker_properties)

    def create_topic(self):

        client = AdminClient(self.broker_properties)
        topics_list = client.list_topics(timeout=5)
        if set(t.topic for t in iter(topics_list.topics.values())) is False:
            creation = client.create_topics([
                NewTopic(topic=self.topic_name, num_partitions=self.num_partitions,
                         replication_factor=self.num_replicas,
                         config={"cleanup.policy": "delete",
                                 "compression.type": "lz4",
                                 "delete.retention.ms": "2000",
                                 "file.delete.delay.ms": "2000",
                                 })
            ])
            for topic, future in creation.items():
                try:
                    future.result()
                    logger.info("topic creation completed")
                except Exception as e:
                    logger.error(f"topic creation failed for topic {self.topic_name}: {e}")


    def close(self):
        logger.info("waiting producer to finish pooling messages")
        self.producer.flush()
        logger.info("producer ready to close")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))