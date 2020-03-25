import pytest

from toad_influx_data.mqtt import MQTT
from toad_influx_data.server import DataServer
from toad_influx_data.utils.config import MQTT_BROKER_HOST


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
