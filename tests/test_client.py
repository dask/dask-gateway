import asyncio

import pytest
import dask
from dask_gateway.auth import get_auth, BasicAuth, KerberosAuth, JupyterHubAuth
from dask_gateway.client import Gateway
from dask_gateway_server.utils import random_port
from tornado import web
from tornado.httpclient import HTTPRequest


def test_get_auth():
    # Pass through existing auth objects
    auth = BasicAuth()
    assert get_auth(auth) is auth

    # Auth by keyword name
    auth = get_auth("basic")
    assert isinstance(auth, BasicAuth)

    auth = get_auth("kerberos")
    assert isinstance(auth, KerberosAuth)

    # Auth from config
    config = {"gateway": {"auth": {"type": "basic", "kwargs": {}}}}
    with dask.config.set(config):
        auth = get_auth()
        assert isinstance(auth, BasicAuth)

    # Auth from config with import path
    config = {
        "gateway": {"auth": {"type": "dask_gateway.auth.BasicAuth", "kwargs": {}}}
    }
    with dask.config.set(config):
        auth = get_auth()
        assert isinstance(auth, BasicAuth)

    # Auth from config with kwargs
    config = {"gateway": {"auth": {"type": "basic", "kwargs": {"username": "bruce"}}}}
    with dask.config.set(config):
        auth = get_auth()
        assert isinstance(auth, BasicAuth)
        assert auth.username == "bruce"

    # Errors
    with pytest.raises(TypeError):
        get_auth(1)

    with pytest.raises(TypeError):
        get_auth(lambda: 1)

    with pytest.raises(ImportError):
        get_auth("dask_gateway.auth.Foo")

    with pytest.raises(ImportError):
        get_auth("not_a_real_module_name_foo_barrr")


def test_jupyterhub_auth(monkeypatch):
    with pytest.raises(ValueError) as exc:
        get_auth("jupyterhub")
    assert "JUPYTERHUB_API_TOKEN" in str(exc.value)

    monkeypatch.setenv("JUPYTERHUB_API_TOKEN", "abcde")
    auth = get_auth("jupyterhub")
    assert auth.api_token == "abcde"
    assert isinstance(auth, JupyterHubAuth)

    # Parameters override environment variable
    assert JupyterHubAuth(api_token="other").api_token == "other"


def test_client_init():
    config = {
        "gateway": {
            "address": "http://127.0.0.1:8888",
            "auth": {"type": "basic", "kwargs": {"username": "bruce"}},
        }
    }

    with dask.config.set(config):
        # Defaults
        gateway = Gateway()
        assert gateway.address == "http://127.0.0.1:8888"
        assert gateway._auth.username == "bruce"

        # Address override
        gateway = Gateway(address="http://127.0.0.1:9999")
        assert gateway.address == "http://127.0.0.1:9999"
        assert gateway._auth.username == "bruce"

        # Auth override
        gateway = Gateway(auth="kerberos")
        assert gateway.address == "http://127.0.0.1:8888"
        assert isinstance(gateway._auth, KerberosAuth)

    config = {"gateway": {"address": None, "auth": {"type": "basic", "kwargs": {}}}}

    with dask.config.set(config):
        # No address provided
        with pytest.raises(ValueError):
            Gateway()


class SlowHandler(web.RequestHandler):
    async def get(self):
        self.waiter = asyncio.ensure_future(asyncio.sleep(30))
        self.settings["task_set"].add(self.waiter)
        try:
            await self.waiter
        except asyncio.CancelledError:
            return
        self.write("Hello world")

    def on_connection_close(self):
        if hasattr(self, "waiter"):
            self.waiter.cancel()


class slow_server(object):
    def __init__(self):
        self.port = random_port()
        self.address = "http://127.0.0.1:%d" % self.port
        self.tasks = set()
        self.app = web.Application([(r"/", SlowHandler)], task_set=self.tasks)

    async def __aenter__(self):
        self.server = self.app.listen(self.port)
        return self

    async def __aexit__(self, *args):
        self.server.stop()
        await asyncio.gather(*self.tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_client_fetch_timeout():
    async with slow_server() as server:
        gateway = Gateway(server.address, auth=BasicAuth("alice"))
        with pytest.raises(TimeoutError):
            await gateway._fetch(HTTPRequest(url=server.address, request_timeout=1))
