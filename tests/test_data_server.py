import strict_rfc3339
import time
import asyncio
import re
from typing import Any, List

import pytest
from aioinflux import InfluxDBClient

from toad_influx_data.handlers.handler_abc import IHandler, InfluxPoint
from toad_influx_data.mqtt import MQTT
from toad_influx_data.server import DataServer
from toad_influx_data.utils.config import MQTT_BROKER_HOST
from toad_influx_data.utils import protocol as prot


class TestInfluxHandler(IHandler):
    LISTEN_TOPIC = "test_handler"
    POINT = {
        "time": strict_rfc3339.now_to_rfc3339_utcoffset(),
        "measurement": "test_measurement",
        "tags": {"tag1": "value1"},
        "fields": {"value": 0},
    }

    def get_topic(self) -> str:
        return TestInfluxHandler.LISTEN_TOPIC

    def can_handle(self, topic: str) -> bool:
        can_handle = True if re.match(TestInfluxHandler.LISTEN_TOPIC, topic) else False
        return can_handle

    def get_influx_database(self, data: Any) -> str:
        return TestInfluxHandler.LISTEN_TOPIC

    def get_influx_points(self, data: Any) -> List[InfluxPoint]:
        return [TestInfluxHandler.POINT]


@pytest.fixture
async def data_server_fixture():
    data_server = DataServer()

    await data_server.start(MQTT_BROKER_HOST)
    yield data_server
    await data_server.stop()


@pytest.fixture
async def mqtt_client_fixture():
    mqtt_client = MQTT("mqtt-test-client")

    async def message_handler(self, topic, payload, properties):
        pass

    await mqtt_client.start(MQTT_BROKER_HOST, message_handler, [])
    yield mqtt_client
    await mqtt_client.stop()


@pytest.fixture
async def influx_database():
    async def create_database(database):
        async with InfluxDBClient(db=database) as client:
            await client.create_database(db=database)

    async def delete_database(database):
        async with InfluxDBClient(db=database) as client:
            await client.drop_database(db=database)

    handler = TestInfluxHandler()
    database = handler.get_influx_database("")

    await create_database(database)
    yield
    await delete_database(database)


@pytest.mark.asyncio
async def test_start_stop_server():
    data_server = DataServer()
    await data_server.start()

    await asyncio.sleep(1)

    await data_server.stop()


@pytest.mark.asyncio
async def test_data_to_influx(
    data_server_fixture, mqtt_client_fixture, influx_database
):
    data_server, mqtt_client = data_server_fixture, mqtt_client_fixture
    handler = TestInfluxHandler()
    data_server.add_handler(handler)

    DATA = ""
    PUBLISHED_DATA = {prot.PAYLOAD_DATA_FIELD: DATA}
    mqtt_client.publish(handler.LISTEN_TOPIC, PUBLISHED_DATA)

    await asyncio.sleep(2)
    async with InfluxDBClient(db=handler.get_influx_database(DATA)) as client:
        point = handler.get_influx_points("")[0]
        measurement = point["measurement"]
        resp = await client.query(f"SELECT * FROM {measurement}", epoch="s")

        result = resp["results"][0]["series"][0]
        expected_columns = {"time", *point["tags"].keys(), *point["fields"].keys()}
        expected_values = {
            point["time"],
            *point["tags"].values(),
            *point["fields"].values(),
        }
        expected_name = point["measurement"]
        assert result["name"] == expected_name
        result["values"][0][0] = time_to_rfc3339(
            result["values"][0][0]
        )  # time to RFC3339
        assert set(result["columns"]) == expected_columns
        assert set(result["values"][0]) == expected_values


def time_to_rfc3339(timestamp):
    return strict_rfc3339.timestamp_to_rfc3339_utcoffset(timestamp)
