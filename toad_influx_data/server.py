import asyncio
import json
import uuid
from typing import List, Any

from aioinflux import InfluxDBClient

import toad_influx_data.utils.protocol as prot
from toad_influx_data.handlers import HANDLERS
from toad_influx_data.handlers.handler_abc import IHandler
from toad_influx_data.mqtt import MQTT, MQTTTopic, MQTTProperties
from toad_influx_data.utils import config


class DataServer:
    """
    Runs the server and handles the requests.

    :ivar events: events that are being waited. Mostly is used for MQTT responses.
    :ivar events_results: dict where events results are stored.
    :ivar mqtt_client: ~`toad_influx_data.mqtt.MQTT` mqtt client.
    :ivar app: aiohttp ~`aiohttp.web.Application` of the running server.
    :ivar running: boolean that represents if the server is running.
    """

    server_id: str
    mqtt_client: MQTT
    running: bool
    handlers: List[IHandler]
    listen_topics: List[str]

    def __init__(self, handlers=None):
        self.server_id = uuid.uuid4().hex
        self.handlers = handlers or HANDLERS
        self.listen_topics = list({parser.get_topic() for parser in self.handlers})
        self.mqtt_client = MQTT(self.__class__.__name__ + "/" + self.server_id)
        self.running = False

    async def start(
        self, mqtt_broker=config.MQTT_BROKER_HOST, mqtt_token=None,
    ):
        """
        Runs the server.

        :param mqtt_broker: MQTT broker IP.
        :param mqtt_token: MQTT credential token.
        :return:
        """
        if self.running:
            raise RuntimeError("Server already running")
        await self.mqtt_client.start(
            mqtt_broker, self._mqtt_response_handler, self.listen_topics, mqtt_token,
        )
        self.running = True

    async def stop(self):
        """
        Stops the server.

        :return:
        """
        if self.running:
            await self.mqtt_client.stop()
            self.running = False

    async def add_handler(self, handler: IHandler):
        await self.mqtt_client.subscribe(handler.get_topic())
        self.handlers.append(handler)

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
        payload_json = json.loads(payload.decode())
        data = payload_json[prot.PAYLOAD_DATA_FIELD]
        for parser in self.handlers:
            if not parser.can_handle(topic):
                continue
            data_points = parser.get_influx_points(data)
            database = parser.get_influx_database(data)
            time_precision = parser.get_time_precision()
            for point in data_points:
                asyncio.create_task(
                    self._write_to_influx(database, point, time_precision)
                )

    async def _write_to_influx(self, database: str, point_data: Any, time_precicion):
        async with InfluxDBClient(db=database) as client:
            await client.write(point_data, precision=time_precicion)
