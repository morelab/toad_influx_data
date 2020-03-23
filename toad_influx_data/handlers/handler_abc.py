from typing import Any, Dict, List
from abc import ABC, abstractmethod

InfluxPoint = Dict[str, Any]


class IHandler(ABC):
    @abstractmethod
    def get_topic(self) -> str:
        pass

    @abstractmethod
    def can_handle(self, topic: str) -> bool:
        pass

    @abstractmethod
    def get_influx_database(self, data: Any) -> str:
        pass

    @abstractmethod
    def get_influx_points(self, data: Any) -> List[InfluxPoint]:
        pass

    def get_time_precision(self):
        return None
