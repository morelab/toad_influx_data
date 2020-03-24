import asyncio
import re
from typing import Any, List

import pytest
from aioinflux import InfluxDBClient

from toad_influx_data.handlers.handler_abc import IHandler, InfluxPoint
from toad_influx_data.mqtt import MQTT
from toad_influx_data.server import DataServer
from toad_influx_data.utils.config import MQTT_BROKER_HOST


class TestInfluxHandler(IHandler):
    LISTEN_TOPIC = "test-handler"
    POINT = {
        "time": "2000-01-01T01:00:00Z",
        "measurement": "test-measurement",
        "tags": {"tag1": "value1"},
        "fields": {"value": 0},
    }

    def get_topic(self) -> str:
        return TestInfluxHandler.LISTEN_TOPIC

    def can_handle(self, topic: str) -> bool:
        return True if re.match(TestInfluxHandler.LISTEN_TOPIC, topic) else False

    def get_influx_database(self, data: Any) -> str:
        return TestInfluxHandler.LISTEN_TOPIC

    def get_influx_points(self, data: Any) -> List[InfluxPoint]:
        return [TestInfluxHandler.POINT]


@pytest.fixture
def data_server_fixture(event_loop: asyncio.AbstractEventLoop):
    data_server = DataServer()

    yield data_server

    event_loop.run_until_complete(data_server.stop())


@pytest.fixture
def mqtt_client_fixture(event_loop: asyncio.AbstractEventLoop):
    mqtt_client = MQTT("mqtt-test-client")

    async def message_handler(self, topic, payload, properties):
        pass

    event_loop.run_until_complete(
        mqtt_client.start(MQTT_BROKER_HOST, message_handler, [])
    )

    yield mqtt_client

    if mqtt_client.running:
        event_loop.run_until_complete(mqtt_client.stop())


@pytest.fixture
def influx_database(event_loop: asyncio.AbstractEventLoop):
    async def create_database(database):
        async with InfluxDBClient(db=database) as client:
            await client.create_database(db=database)

    async def delete_database(database):
        async with InfluxDBClient(db=database) as client:
            await client.drop_database(db=database)

    handler = TestInfluxHandler()
    database = handler.get_influx_database("")

    event_loop.run_until_complete(create_database(database))
    yield
    event_loop.run_until_complete(delete_database(database))


@pytest.mark.asyncio
async def test_start_stop_server():
    data_server = DataServer()
    await data_server.start()

    await asyncio.sleep(1)

    await data_server.stop()


async def test_data_to_influx(
    data_server_fixture, mqtt_client_fixture, influx_database
):
    data_server, mqtt_client = data_server_fixture, mqtt_client_fixture
    handler = TestInfluxHandler()
    await data_server.add_handler(handler)

    PUBLISHED_DATA = ""
    mqtt_client.publish(handler.LISTEN_TOPIC, PUBLISHED_DATA)
    await asyncio.sleep(1)
    async with InfluxDBClient(db=handler.get_influx_database(PUBLISHED_DATA)) as client:
        point = handler.get_influx_points("")[0]
        measurement = point["measurement"]
        value = point["fields"]["value"]
        resp = await client.query(f"SELECT value FROM {measurement}")
        assert resp == value
