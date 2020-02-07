from aiohttp import web
from traitlets.config import Config

from dask_gateway_server.app import DaskGateway
from dask_gateway_server.utils import random_port


class aiohttp_server(object):
    def __init__(self, routes=None, app=None, host="localhost", port=None):
        self.app = app or web.Application()
        if routes is not None:
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


class temp_gateway(object):
    def __init__(self, **kwargs):
        config = Config()
        config2 = kwargs.pop("config", None)

        options = {
            "gateway_url": "tls://127.0.0.1:%d" % random_port(),
            "private_url": "http://127.0.0.1:%d" % random_port(),
            "public_url": "http://127.0.0.1:%d" % random_port(),
            "authenticator_class": "dask_gateway_server.auth.SimpleAuthenticator",
        }
        options.update(kwargs)
        config["DaskGateway"].update(options)

        if config2:
            config.merge(config2)

        self.config = config

    async def __aenter__(self):
        self.gateway = DaskGateway(config=self.config)
        self.gateway.initialize([])
        await self.gateway.start_async()
        return self.gateway

    async def __aexit__(self, *args):
        await self.gateway.stop_async()
