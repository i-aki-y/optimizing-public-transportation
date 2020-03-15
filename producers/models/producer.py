"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient
import confluent_kafka

logger = logging.getLogger(__name__)


ADMIN_CLIENT_TIMEOUT_SEC = 5
TOPIC_REPLICATION_FACTOR = 1
TOPIC_CONFIG = {
    "cleanup.policy": "delete",
    "compression.type": "lz4",
    "delete.retention.ms": "20000",
    "file.delete.delay.ms": "20000",
}

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=3,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "BROKER_URL": "PLAINTEXT://localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
        }

        if len(Producer.existing_topics) == 0:
            self.update_existing_topics()

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
        else:
            logger.info(f"topic {self.topic_name} already exists")

        # TODO: Configure the AvroProducer
        schema_registry = CachedSchemaRegistryClient({"url": self.broker_properties["SCHEMA_REGISTRY_URL"]})
        self.producer = AvroProducer(
            {
                "bootstrap.servers": self.broker_properties["BROKER_URL"],
                #"debug": "topic, queue"
            },
            schema_registry=schema_registry
        )


    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        logger.info(f"start topic creation {self.topic_name}")
        client = self.get_broker()
        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config=TOPIC_CONFIG,
                )
            ]
        )
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"topic {self.topic_name} is created")
            except Exception as e:
                logger.warning(f"fail to create {self.topic_name}: {e}")

    def update_existing_topics(self):
        """Get topic list from broker and update existing topics variable"""
        client = self.get_broker()
        topic_metadata = client.list_topics(timeout=ADMIN_CLIENT_TIMEOUT_SEC)
        Producer.existing_topics = set(t.topic for t in iter(topic_metadata.topics.values()))

    def get_broker(self):
        """Get AdminClient"""
        return AdminClient({
            "bootstrap.servers": self.broker_properties["BROKER_URL"],
            # "debug": "broker,admin"
        })


    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        res = self.producer.flush(0.2)
        logger.info(f"{self.topic_name} producer closed. flush() returns {res}.")


    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
