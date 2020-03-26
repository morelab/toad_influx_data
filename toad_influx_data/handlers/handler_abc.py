from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

InfluxPoint = Dict[str, Any]


class IHandler(ABC):
    """
    Interface that the Handlers need to implement.
    Handlers are the Classes that handle MQTT messages,
    so that they transform the messages into InfluxDB data points.
    """

    @abstractmethod
    def get_topics(self) -> List[str]:
        """

        :return: topics that the handler need to listen.
        """
        pass

    @abstractmethod
    def can_handle(self, topic: str) -> bool:
        """

        :param topic: the topic that the message was received from.
        :return: if it is capable of handling a message received in a certain topic.
        """
        pass

    @abstractmethod
    def get_influx_database(self, data: Any) -> str:
        """

        :param data: MQTT message data.
        :return: the database to which the points will be writen to.
        """
        pass

    @abstractmethod
    def get_influx_points(self, data: Any) -> List[InfluxPoint]:
        """

        :param data: MQTT message data.
        :return: list of InfluxDB data points that are generated from the MQTT message data.
        """
        pass

    def get_time_precision(self) -> Optional[str]:
        """

        :return: time precision
        """
        return None
