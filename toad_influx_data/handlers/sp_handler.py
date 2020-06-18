import re
from typing import Dict, Union, Any, Optional
from typing import List

import strict_rfc3339
from senml.senml import SenMLDocument, SenMLMeasurement

from toad_influx_data.handlers.handler_abc import IHandler, InfluxPoint


class SmartPlugHandler(IHandler):
    LISTEN_TOPIC = "data/+/influx_data/sp"

    def get_topics(self) -> List[str]:
        return [SmartPlugHandler.LISTEN_TOPIC, SmartPlugHandler.LISTEN_TOPIC + "/#"]

    def can_handle(self, topic: str) -> bool:
        regex_topic = SmartPlugHandler.LISTEN_TOPIC.replace("+", "[^/]+").replace(
            "#", ".+"
        )
        return True if re.match(regex_topic, topic) else False

    def get_influx_database(self, topic: str) -> str:
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

    def get_time_precision(self) -> Optional[str]:
        return "s"

    def _get_time_from_senml(
        self, senml_document: SenMLDocument, senml_measurement: SenMLMeasurement
    ) -> str:
        time = senml_measurement.time or senml_document.base.time
        if not time:
            raise ValueError("No time specified")
        return strict_rfc3339.timestamp_to_rfc3339_localoffset(time)

    def _get_measurement_from_senml(
        self, senml_document: SenMLDocument, senml_measurement: SenMLMeasurement
    ) -> str:
        name = self._get_name(senml_document, senml_measurement)
        if not name:
            raise ValueError("No Name specified")
        sp_id, measurement = name.split(
            "/"
        )  # the name is <id>/<measurement>; e.g. sp_w.r1.c1/power
        return measurement

    def _get_tags_from_senml(
        self, senml_document: SenMLDocument, senml_measurement: SenMLMeasurement
    ) -> Dict[str, Union[str, int]]:
        name = self._get_name(senml_document, senml_measurement)
        if not name:
            raise ValueError("No Name specified")
        sp_id, measurement = name.split(
            "/"
        )  # the name is <id>/<measurement>; e.g. sp_w.r1.c1/power
        unit = senml_measurement.unit or senml_document.base.unit
        type = sp_id[3]  # sp_w.r0.c1
        tags = {"id": sp_id, "unit": unit, "type": type}
        if type == "w":
            type, row, column = sp_id.split(".")
            tags["row"] = row[1:]
            tags["column"] = column[1:]
        return tags

    def _get_fields_from_senml(
        self, senml_document: SenMLDocument, senml_measurement: SenMLMeasurement
    ) -> Dict[str, Union[str, int, float]]:
        return {"value": senml_measurement.value}

    def _get_name(
        self, senml_document: SenMLDocument, senml_measurement: SenMLMeasurement
    ) -> str:
        base_name = senml_document.base.name or ""
        name = senml_measurement.name or ""
        return base_name + name
