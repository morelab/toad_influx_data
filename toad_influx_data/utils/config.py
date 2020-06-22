import configparser
import os
from os import path

DEFAULT_CONFIG_FILE = path.join(
    path.dirname(path.dirname(path.dirname(__file__))), "config", "config.ini"
)


config = configparser.ConfigParser()
config.read(os.environ.get("TOAD_API_CONFIG_FILE", DEFAULT_CONFIG_FILE))

influx_config = config["INFLUXDB"]
mqtt_config = config["MQTT"]
logger_config = config["LOGGER"]

# InfluxDB configuration
INFLUXDB_HOST = influx_config["HOST"]
INFLUXDB_PORT = influx_config["PORT"]
# MQTT client configuration
MQTT_BROKER_HOST = mqtt_config["BROKER_HOST"]
MQTT_RESPONSE_TIMEOUT = int(mqtt_config["RESPONSE_TIMEOUT"])

# Logger configuration
LOGGER_VERBOSE = logger_config.getboolean("VERBOSE")
