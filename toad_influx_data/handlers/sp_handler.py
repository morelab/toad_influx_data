import re
from typing import Dict, Union, Any
import json
from typing import List
from senml.senml import SenMLDocument, SenMLMeasurement

from toad_influx_data.handlers.handler_abc import IParser, InfluxPoint


class SmartPlugParser(IParser):
    SP_DATA_SOURCE_TOPIC = "data/influx_data/sp_data"

    def get_topic(self) -> str:
        return SmartPlugParser.SP_DATA_SOURCE_TOPIC + "/#"

    def can_handle(self, topic: str) -> bool:
        return True if re.match(SmartPlugParser.SP_DATA_SOURCE_TOPIC, topic) else False

    def get_influx_database(self, data: Any) -> str:
        return "sp"

    def get_influx_points(self, senml_data_points: Any) -> List[InfluxPoint]:
        influx_points = []
        senml_document = SenMLDocument.from_json(senml_data_points)
        for senml_measurement in senml_document.measurements:
            influx_point = {
                "time": self._get_time_from_senml(senml_document, senml_measurement),
                "measurement": self._get_measurement_from_senml(
                    senml_document, senml_measurement
                ),
                "tags": self._get_tags_from_senml(senml_document, senml_measurement),
                "fields": self._get_fields_from_senml(
                    senml_document, senml_measurement
                ),
            }
            influx_points.append(influx_point)
        return influx_points

    def get_time_precision(self):
        return "s"

    def _get_time_from_senml(
        self, document: SenMLDocument, measurement: SenMLMeasurement
    ) -> float:
        time = measurement.time or document.base.time
        if not time:
            raise ValueError("No time specified")
        return time

    def _get_measurement_from_senml(
        self, document: SenMLDocument, measurement: SenMLMeasurement
    ) -> str:
        name = measurement.name or document.base.name
        if not name:
            raise ValueError("No Name specified")
        sp_id, measurement = name.split(
            "/"
        )  # the name is <id>/<measurement>; e.g. w.r1.c1/power
        return measurement

    def _get_tags_from_senml(
        self, document: SenMLDocument, measurement: SenMLMeasurement
    ) -> Dict[str, Union[str, int]]:
        name = measurement.name or document.base.name
        if not name:
            raise ValueError("No Name specified")
        sp_id, measurement = name.split(
            "/"
        )  # the name is <id>/<measurement>; e.g. w.r1.c1/power
        unit = measurement.unit or document.base.unit
        type = sp_id[0]
        tags = {"id": sp_id, "unit": unit, "type": type}
        if type == "w":
            type, row, column = sp_id.split(".")
            tags["row"] = row
            tags["column"] = column
        return tags

    def _get_fields_from_senml(
        self, document: SenMLDocument, measurement: SenMLMeasurement
    ) -> Dict[str, Union[str, int, float]]:
        return {"value": measurement.value}
