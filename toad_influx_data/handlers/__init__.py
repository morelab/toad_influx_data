from typing import List
from toad_influx_data.handlers.generic_handler import GenericHandler
from toad_influx_data.handlers.handler_abc import IHandler

HANDLERS: List[IHandler] = [GenericHandler()]
