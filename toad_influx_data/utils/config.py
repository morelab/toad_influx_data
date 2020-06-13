import configparser
import os
from os import path

DEFAULT_CONFIG_FILE = path.join(
    path.dirname(path.dirname(path.dirname(__file__))), "config", "config.ini"
)

print(DEFAULT_CONFIG_FILE)

config = configparser.ConfigParser()
config.read(os.environ.get("TOAD_API_CONFIG_FILE", DEFAULT_CONFIG_FILE))

mqtt_config = config["MQTT"]
logger_config = config["LOGGER"]

# API server's MQTT client configuration
MQTT_BROKER_HOST = mqtt_config["BROKER_HOST"]
MQTT_RESPONSE_TIMEOUT = int(mqtt_config["RESPONSE_TIMEOUT"])

# Logger configuration
LOGGER_VERBOSE = logger_config.getboolean("VERBOSE")
