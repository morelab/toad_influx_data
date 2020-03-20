import asyncio
import uuid
from typing import Dict, Any

from aioinflux import InfluxDBClient

from toad_influx_data import config
from toad_influx_data.mqtt import MQTT, MQTTTopic, MQTTProperties
from toad_influx_data.protocol import REST_PAYLOAD_FIELD, REST_SUBTOPICS_FIELD
from toad_influx_data.protocol import SP_DATA_SOURCE_TOPIC, INFLUX_SP_DATABASE


class DataServer:
    """
    Runs the server and handles the requests.

    :ivar events: events that are being waited. Mostly is used for MQTT responses.
    :ivar events_results: dict where events results are stored.
    :ivar mqtt_client: ~`toad_influx_data.mqtt.MQTT` mqtt client.
    :ivar app: aiohttp ~`aiohttp.web.Application` of the running server.
    :ivar running: boolean that represents if the server is running.
    """

    events: Dict[str, asyncio.Event]
    events_results: Dict[str, bytes]
    mqtt_client: MQTT
    running: bool

    def __init__(self):
        self.server_id = uuid.uuid4().hex
        self.data_source_topics = [SP_DATA_SOURCE_TOPIC + "/#"]
        self.events = {}
        self.events_results = {}
        self.mqtt_client = MQTT(self.__class__.__name__ + "/" + self.server_id)
        self.running = False

    async def start(
        self, mqtt_broker=config.MQTT_BROKER_IP, mqtt_token=None,
    ):
        """
        Runs the server.

        :param mqtt_broker: MQTT broker IP.
        :param mqtt_token: MQTT credential token.
        :return:
        """
        if self.running:
            raise RuntimeError("Server already running")
        await self.mqtt_client.run(
            mqtt_broker,
            self._mqtt_response_handler,
            self.data_source_topics,
            mqtt_token,
        )
        self.running = True

    async def stop(self):
        """
        Stops the server.

        :return:
        """
        if self.running:
            await self.mqtt_client.stop()
            # todo: stop aiothpp app?
            self.running = False

    async def _mqtt_response_handler(
        self, topic: MQTTTopic, payload: bytes, properties: MQTTProperties
    ):
        """
        Handles MQTT messages; it stores the message payload in.

        ~`DataServer.events_results`, and it sets the Event in
        ~`DataServer.events`

        :param topic: MQTT topic the message was received in.
        :param payload: MQTT message payload
        :param properties: MQTT message properties
        :return:
        """

        # extract response_id
        response_id = topic.replace(self.data_source_topics, "")
        response_id = response_id.replace("/", "")
        # store event result
        self.events_results[response_id] = payload
        # set event
        self.events[response_id].set()

    async def write_to_influx(self, data: Any):
        point = {
            "time": "2009-11-10T23:00:00Z",
            "measurement": "cpu_load_short",
            "tags": {"host": "server01", "region": "us-west"},
            "fields": {"value": 0.64},
        }

        async with InfluxDBClient(db=INFLUX_SP_DATABASE) as client:
            await client.create_database(db=INFLUX_SP_DATABASE)
            await client.write(point)


def check_request_body(data_json: Dict):
    """
    Parses POST /api/in requests body.

    :param data_json: JSON dictionary containin
    :return: JSON dictionary
    """
    if 2 < len(data_json):
        raise ValueError("Invalid data JSON")
    if REST_PAYLOAD_FIELD not in data_json:
        raise ValueError("Invalid data JSON")
    if len(data_json) == 2 and REST_SUBTOPICS_FIELD not in data_json:
        raise ValueError("Invalid data JSON")
