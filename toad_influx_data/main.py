import asyncio
from toad_influx_data.server import DataServer


if __name__ == "__main__":
    data_server = DataServer()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(data_server.start())
    loop.run_forever()
