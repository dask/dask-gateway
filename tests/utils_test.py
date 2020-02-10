from aiohttp import web
from traitlets.config import Config

from dask_gateway_server.app import DaskGateway
from dask_gateway_server.backends.inprocess import InProcessBackend
from dask_gateway_server.utils import random_port
from dask_gateway import Gateway


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
        c = Config()
        c.DaskGateway.backend_class = InProcessBackend

        config2 = kwargs.pop("config", None)

        c.DaskGateway.address = "127.0.0.1:0"
        c.Proxy.address = "127.0.0.1:0"
        c.Proxy.scheduler_address = "127.0.0.1:0"
        c.DaskGateway.authenticator_class = (
            "dask_gateway_server.auth.SimpleAuthenticator"
        )
        c.DaskGateway.update(kwargs)

        if config2:
            c.merge(config2)

        self.config = c

    async def __aenter__(self):
        self.gateway = DaskGateway(config=self.config)
        self.gateway.initialize([])
        await self.gateway.start_async()
        await self.gateway.backend.proxy._proxy_contacted
        self.address = f"http://{self.gateway.backend.proxy.address}"
        self.proxy_address = f"tls://{self.gateway.backend.proxy.scheduler_address}"
        return self

    async def __aexit__(self, *args):
        await self.gateway.stop_async()

    def gateway_client(self, **kwargs):
        defaults = {
            "address": self.address,
            "proxy_address": self.proxy_address,
            "asynchronous": True,
        }
        defaults.update(kwargs)
        return Gateway(**defaults)
