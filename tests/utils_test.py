from dask_gateway_server.utils import random_port

from aiohttp import web


class aiohttp_server(object):
    def __init__(self, routes, host="localhost", port=None):
        self.app = web.Application()
        self.app.add_routes(routes)
        self.runner = web.AppRunner(self.app)
        self.host = host
        self.port = port or random_port()

    @property
    def address(self):
        return "http://%s:%d" % (self.host, self.port)

    async def __aenter__(self):
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        return self

    async def __aexit__(self, *args):
        await self.runner.cleanup()
