import asyncio
import time

import pytest
import strict_rfc3339
from aioinflux import InfluxDBClient

from tests.utils import mqtt_client_fixture, data_server_fixture
from toad_influx_data.handlers.sp_handler import SmartPlugHandler
from toad_influx_data.utils import protocol as prot

mqtt_client_fixture = mqtt_client_fixture
data_server_fixture = data_server_fixture


@pytest.fixture
async def influx_database():
    async def create_database(database):
        async with InfluxDBClient(db=database) as client:
            await client.create_database(db=database)

    async def delete_database(database):
        async with InfluxDBClient(db=database) as client:
            await client.drop_database(db=database)

    handler = SmartPlugHandler()
    database = handler.get_influx_database("")

    await create_database(database)
    yield
    await delete_database(database)


sp_data_1 = [
    {"bn": "w.r1.c1/power", "bt": time.time(), "bu": "W", "v": 120.1},
]
sp_points_1 = [
    {
        "time": strict_rfc3339.timestamp_to_rfc3339_utcoffset(sp_data_1[0]["bt"]),
        "measurement": "power",
        "tags": {"id": "w.r1.c1", "row": "1", "column": "1", "type": "w", "unit": "W"},
        "fields": {"value": 120.1},
    },
]

sp_data_2 = [
    {"n": "w.r1.c1/status", "t": time.time(), "bu": "W", "v": 1},
    {"n": "m2/power", "t": time.time(), "bu": "W", "v": 120.1},
]

sp_points_2 = [
    {
        "time": strict_rfc3339.timestamp_to_rfc3339_utcoffset(sp_data_2[0]["t"]),
        "measurement": "status",
        "tags": {"id": "w.r1.c1", "row": "1", "column": "1", "type": "w", "unit": "W"},
        "fields": {"value": 1},
    },
    {
        "time": strict_rfc3339.timestamp_to_rfc3339_utcoffset(sp_data_2[1]["t"]),
        "measurement": "power",
        "tags": {"id": "m2", "type": "m", "unit": "W"},
        "fields": {"value": 120.1},
    },
]

sp_data_points = [(sp_data_1, sp_points_1), (sp_data_2, sp_points_2)]


def test_sp_hanlder():
    handler = SmartPlugHandler()
    for data, expected_points in sp_data_points:
        points = handler.get_influx_points(data)
        for expected_point, point in zip(expected_points, points):
            assert expected_point == point


@pytest.mark.asyncio
@pytest.mark.parametrize("sp_data", map(lambda dp: dp[0], sp_data_points))
async def test_sp_controller(
    data_server_fixture, mqtt_client_fixture, influx_database, sp_data
):
    mqtt_client = mqtt_client_fixture
    handler = SmartPlugHandler()
    PUBLISHED_DATA = {prot.PAYLOAD_DATA_FIELD: sp_data}
    mqtt_client.publish(handler.get_topics()[0], PUBLISHED_DATA)

    await asyncio.sleep(1)
    async with InfluxDBClient(db=handler.get_influx_database(sp_data)) as client:
        for point in handler.get_influx_points(sp_data):
            resp = await client.query(
                f"SELECT * FROM {point['measurement']} WHERE time='{point['time']}'"
            )

            result = resp["results"][0]["series"][0]
            expected_columns = {"time", *point["tags"].keys(), *point["fields"].keys()}
            expected_values = {
                point["time"],
                *point["tags"].values(),
                *point["fields"].values(),
            }
            expected_name = point["measurement"]
            assert result["name"] == expected_name
            result["values"][0][0] = nanos_to_rfc3339(
                result["values"][0][0]
            )  # time to RFC3339
            assert set(result["columns"]) == expected_columns
            assert set(result["values"][0]) == expected_values


def nanos_to_rfc3339(nanos):
    secs = nanos / 1e9
    return strict_rfc3339.timestamp_to_rfc3339_utcoffset(secs)
