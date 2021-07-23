import os
import subprocess
import uuid

import pytest
from traitlets.config import Config

from dask_gateway.auth import BasicAuth, JupyterHubAuth
from dask_gateway_server.utils import random_port

from .utils_test import temp_gateway

try:
    import kerberos

    del kerberos
    skip = not os.environ.get("TEST_DASK_GATEWAY_YARN")
    requires_kerberos = pytest.mark.skipif(skip, reason="No kerberos server running")
except ImportError:
    requires_kerberos = pytest.mark.skipif(True, reason="Cannot import kerberos")

try:
    import jupyterhub.tests.mocking as hub_mocking
except ImportError:
    hub_mocking = None


KEYTAB_PATH = "/home/dask/dask.keytab"


def kinit():
    subprocess.check_call(["kinit", "-kt", KEYTAB_PATH, "dask"])


def kdestroy():
    subprocess.check_call(["kdestroy"])


@pytest.mark.asyncio
async def test_basic_auth():
    async with temp_gateway() as g:
        async with g.gateway_client(auth="basic") as gateway:
            await gateway.list_clusters()


@pytest.mark.asyncio
async def test_basic_auth_password():
    config = Config()
    config.DaskGateway.authenticator_class = (
        "dask_gateway_server.auth.SimpleAuthenticator"
    )
    config.SimpleAuthenticator.password = "mypass"

    async with temp_gateway(config=config) as g:
        auth = BasicAuth()
        async with g.gateway_client(auth=auth) as gateway:
            with pytest.raises(Exception):
                await gateway.list_clusters()

            auth.password = "mypass"

            await gateway.list_clusters()


@pytest.mark.asyncio
@requires_kerberos
async def test_kerberos_auth():
    config = Config()
    config.Proxy.address = "master.example.com:0"
    config.DaskGateway.authenticator_class = (
        "dask_gateway_server.auth.KerberosAuthenticator"
    )
    config.KerberosAuthenticator.keytab = KEYTAB_PATH

    async with temp_gateway(config=config) as g:
        async with g.gateway_client(auth="kerberos") as gateway:
            kdestroy()

            with pytest.raises(Exception):
                await gateway.list_clusters()

            kinit()

            await gateway.list_clusters()

            kdestroy()


class temp_hub(object):
    def __init__(self, hub):
        self.hub = hub

    async def __aenter__(self):
        await self.hub.initialize([])
        await self.hub.start()

        # alembic turns off all logs, reenable them for the tests
        import logging
        from tornado.log import app_log, access_log, gen_log

        logs = [app_log, access_log, gen_log, logging.getLogger("DaskGateway")]
        for log in logs:
            log.disabled = False

        # Disable curl http client for easier testing
        from tornado.httpclient import AsyncHTTPClient

        AsyncHTTPClient.configure("tornado.simple_httpclient.SimpleAsyncHTTPClient")

    async def __aexit__(self, *args):
        if self.hub.http_server:
            self.hub.http_server.stop()
        await self.hub.cleanup()
        type(self.hub).clear_instance()


def configure_dask_gateway(jhub_api_token, jhub_bind_url):
    config = Config()
    config.DaskGateway.authenticator_class = (
        "dask_gateway_server.auth.JupyterHubAuthenticator"
    )
    config.JupyterHubAuthenticator.jupyterhub_api_token = jhub_api_token
    config.JupyterHubAuthenticator.jupyterhub_api_url = jhub_bind_url + "api/"
    return config


@pytest.mark.skipif(not hub_mocking, reason="JupyterHub not installed")
@pytest.mark.asyncio
async def test_jupyterhub_auth_user(monkeypatch):
    from jupyterhub.tests.utils import add_user

    jhub_api_token = uuid.uuid4().hex
    jhub_bind_url = "http://127.0.0.1:%i/@/space%%20word/" % random_port()

    hub_config = Config()
    hub_config.JupyterHub.services = [
        {"name": "dask-gateway", "api_token": jhub_api_token}
    ]
    hub_config.JupyterHub.bind_url = jhub_bind_url

    class MockHub(hub_mocking.MockHub):
        def init_logging(self):
            pass

    hub = MockHub(config=hub_config)

    # Configure gateway
    config = configure_dask_gateway(jhub_api_token, jhub_bind_url)

    async with temp_gateway(config=config) as g:
        async with temp_hub(hub):
            # Create a new jupyterhub user alice, and get the api token
            u = add_user(hub.db, name="alice")
            api_token = u.new_api_token()
            hub.db.commit()

            # Configure auth with incorrect api token
            auth = JupyterHubAuth(api_token=uuid.uuid4().hex)

            async with g.gateway_client(auth=auth) as gateway:
                # Auth fails with bad token
                with pytest.raises(Exception):
                    await gateway.list_clusters()

                # Auth works with correct token
                auth.api_token = api_token
                await gateway.list_clusters()


@pytest.mark.skipif(not hub_mocking, reason="JupyterHub not installed")
@pytest.mark.asyncio
async def test_jupyterhub_auth_service(monkeypatch):
    jhub_api_token = uuid.uuid4().hex
    jhub_service_token = uuid.uuid4().hex
    jhub_bind_url = "http://127.0.0.1:%i/@/space%%20word/" % random_port()

    hub_config = Config()
    hub_config.JupyterHub.services = [
        {"name": "dask-gateway", "api_token": jhub_api_token},
        {"name": "any-service", "api_token": jhub_service_token},
    ]
    hub_config.JupyterHub.bind_url = jhub_bind_url

    class MockHub(hub_mocking.MockHub):
        def init_logging(self):
            pass

    hub = MockHub(config=hub_config)

    # Configure gateway
    config = configure_dask_gateway(jhub_api_token, jhub_bind_url)

    async with temp_gateway(config=config) as g:
        async with temp_hub(hub):
            # Configure auth with incorrect api token
            auth = JupyterHubAuth(api_token=uuid.uuid4().hex)
            async with g.gateway_client(auth=auth) as gateway:
                # Auth fails with bad token
                with pytest.raises(Exception):
                    await gateway.list_clusters()

                # Auth works with service token
                auth.api_token = jhub_api_token
                await gateway.list_clusters()
