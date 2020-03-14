import unittest
from enum import IntEnum
from dataclasses import asdict, dataclass

from confluent_kafka.admin import AdminClient
from models.producer import Producer
from models import Station, Train, Weather
from connector import configure_connector, delete_connector

BROKER_URL = "PLAINTEXT://localhost:9092"

import logging

logging.basicConfig(level=logging.DEBUG)


def get_topic_list(client):
    """Checks if the given topic exists"""
    topic_metadata = client.list_topics(timeout=5)
    return [t.topic for t in iter(topic_metadata.topics.values())]



class TestProducer(unittest.TestCase):

    def test_topic(self):
        client = AdminClient({"bootstrap.servers": BROKER_URL})
        for topic in get_topic_list(client):
            print(topic)

    def test_producer(self):
        Producer("topic_test", "schema")

    def test_station(self):
        colors = IntEnum("colors", "blue green red", start=0)
        s = Station(40020, "TestStation", colors.red)
        print(s)

        t = Train("BLUEL999", Train.status.in_service)
        s.run(t, "a", 90000, "a")


    def test_weather(self):
        w = Weather(5)
        w.run(5)


    def test_connect(self):
        configure_connector()
        delete_connector()


if __name__ == "__main__":
    unittest.main()
