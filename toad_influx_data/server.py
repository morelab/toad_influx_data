import asyncio
import json
import uuid
from typing import List, Any, Optional

from aioinflux import InfluxDBClient

import toad_influx_data.utils.protocol as prot
from toad_influx_data.handlers import HANDLERS
from toad_influx_data.handlers.handler_abc import IHandler
from toad_influx_data.mqtt import MQTT, MQTTTopic, MQTTProperties
from toad_influx_data.utils import config
from toad_influx_data.utils import logger


class DataServer:
    """
    Runs the server and handles the requests.

    :ivar server_id: random ID that identifies the Server.
    :ivar mqtt_client: ~`toad_influx_data.mqtt.MQTT` mqtt client.
    :ivar running: boolean that represents if the server is running.
    :ivar handlers: list containing all the handlers that implement ~`toad_influx_data.handlers.handler_abc.IHandler`
    :ivar listen_topics: topics list to which the Server listens.
    """

    server_id: str
    mqtt_client: MQTT
    running: bool
    handlers: List[IHandler]
    listen_topics: List[str]

    def __init__(self, handlers=None):
        """
        DataServer initializer

        :param handlers: list of handlers that handle MQTT messages.
        """

        self.server_id = uuid.uuid4().hex
        self.handlers = handlers or HANDLERS
        topics = set()
        for parser in self.handlers:
            topics.update(parser.get_topics())
        self.listen_topics = list(topics)
        self.mqtt_client = MQTT(self.__class__.__name__ + "/" + self.server_id)
        self.running = False

    async def start(
        self, mqtt_host=config.MQTT_BROKER_HOST, mqtt_token=None,
    ):
        """
        Runs the server.

        :param mqtt_host: MQTT broker IP.
        :param mqtt_token: MQTT credential token.
        :return:
        """
        if self.running:
            raise RuntimeError("Server already running")
        await self.mqtt_client.start(
            mqtt_host, self._mqtt_response_handler, self.listen_topics, mqtt_token,
        )
        self.running = True
        logger.log_info(f"toad_influx_data server running...")

    async def stop(self):
        """
        Stops the server.

        :return:
        """
        if self.running:
            await self.mqtt_client.stop()
            self.running = False
            logger.log_info("toad_influx_data server stopped")

    def add_handler(self, handler: IHandler):
        for topic in handler.get_topics():
            self.mqtt_client.subscribe(topic)
            self.listen_topics.append(topic)
        self.handlers.append(handler)

    async def _mqtt_response_handler(
        self, topic: MQTTTopic, payload: bytes, properties: MQTTProperties
    ):
        """
        Handles MQTT messages; it checks what handlers can handle it.
        Every positive handler generates points from the MQTT message,
        and the DataServer stores the points into InfluxDB.

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
            # time_precision = parser.get_time_precision()
            time_precision = None
            for point in data_points:
                asyncio.create_task(
                    self._write_to_influx(database, point, time_precision)
                )

    async def _write_to_influx(
        self, database: str, point_data: Any, time_precision: Optional[str]
    ):
        """
        Method for writing a data point to InfluxDB

        :param database: InfluxDB database to which will write.
        :param point_data: data point that will write.
        :param time_precision: the precision that the time is formatted.
        :return:
        """
        logger.log_info(f"Writing to influx {database}:{point_data}...")
        async with InfluxDBClient(db=database) as client:
            await client.write(point_data, precision=time_precision)
        logger.log_info(f"Written to influx {database}:{point_data}")
