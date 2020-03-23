from toad_influx_data.server import DataServer


async def toad_influx_data_app():
    data_server = DataServer()
    await data_server.start()
    return data_server
