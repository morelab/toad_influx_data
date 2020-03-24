from typing import List
from toad_influx_data.handlers.sp_handler import SmartPlugHandler
from toad_influx_data.handlers.handler_abc import IHandler

HANDLERS: List[IHandler] = [SmartPlugHandler()]
