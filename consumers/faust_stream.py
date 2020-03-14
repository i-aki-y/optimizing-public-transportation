"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)

TOPIC_NAME_COMMON = "org.chicago.cta"

# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool

    def transform(self):
        return TransformedStation(
            station_id=self.station_id,
            station_name=self.station_name,
            order=self.order,
            line=self.get_color())

    def get_color(self):
        if self.red:
            return "red"
        if self.blue:
            return "blue"
        if self.gree:
            return "green"
        else:
            return ""


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic(f"{TOPIC_NAME_COMMON}.connect-stations", value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic(f"{TOPIC_NAME_COMMON}.stations-stream", partitions=1)
# TODO: Define a Faust Table
table = app.Table(
    # "TODO",
    # default=TODO,
    partitions=1,
    changelog_topic=out_topic,
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#

@app.agent(topic)
async def station(stations):
    stations.add_processor(transform_station)
    async for station in stations:
        await out_topic.send(key=station.stop_id, value=station)


def transform_station(s: Station) -> TransformedStation:
    return s.transform()


if __name__ == "__main__":
    app.main()
