import os
import subprocess
import uuid

import pytest
from traitlets.config import Config

from dask_gateway import Gateway
from dask_gateway.auth import BasicAuth, JupyterHubAuth
from dask_gateway_server.utils import random_port

from .utils import temp_gateway

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
async def test_basic_auth(tmpdir):
    async with temp_gateway(temp_dir=str(tmpdir.join("dask-gateway"))) as gateway_proc:

        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True, auth="basic"
        ) as gateway:

            await gateway.list_clusters()


@pytest.mark.asyncio
async def test_basic_auth_password(tmpdir):
    config = Config()
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.DaskGateway.authenticator_class = (
        "dask_gateway_server.auth.DummyAuthenticator"
    )
    config.DummyAuthenticator.password = "mypass"

    async with temp_gateway(config=config) as gateway_proc:
        auth = BasicAuth()

        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True, auth=auth
        ) as gateway:

            with pytest.raises(Exception):
                await gateway.list_clusters()

            auth.password = "mypass"

            await gateway.list_clusters()


@pytest.mark.asyncio
@requires_kerberos
async def test_kerberos_auth(tmpdir):
    config = Config()
    config.DaskGateway.public_url = "http://master.example.com:%d" % random_port()
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.DaskGateway.authenticator_class = (
        "dask_gateway_server.auth.KerberosAuthenticator"
    )
    config.KerberosAuthenticator.keytab = KEYTAB_PATH

    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True, auth="kerberos"
        ) as gateway:

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


@pytest.mark.skipif(not hub_mocking, reason="JupyterHub not installed")
@pytest.mark.asyncio
async def test_jupyterhub_auth(tmpdir, monkeypatch):
    from jupyterhub.tests.utils import add_user

    gateway_address = "http://127.0.0.1:%d" % random_port()
    jhub_api_token = uuid.uuid4().hex
    jhub_bind_url = "http://127.0.0.1:%i/@/space%%20word/" % random_port()

    hub_config = Config()
    hub_config.JupyterHub.services = [
        {"name": "dask-gateway", "url": gateway_address, "api_token": jhub_api_token}
    ]
    hub_config.JupyterHub.bind_url = jhub_bind_url

    class MockHub(hub_mocking.MockHub):
        def init_logging(self):
            pass

    hub = MockHub(config=hub_config)

    # Configure gateway
    config = Config()
    config.DaskGateway.public_url = gateway_address + "/services/dask-gateway/"
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.DaskGateway.authenticator_class = (
        "dask_gateway_server.auth.JupyterHubAuthenticator"
    )
    config.JupyterHubAuthenticator.jupyterhub_api_token = jhub_api_token
    config.JupyterHubAuthenticator.jupyterhub_api_url = jhub_bind_url + "api/"

    async with temp_gateway(config=config) as gateway_proc:
        async with temp_hub(hub):
            # Create a new jupyterhub user alice, and get the api token
            u = add_user(hub.db, name="alice")
            api_token = u.new_api_token()
            hub.db.commit()

            # Configure auth with incorrect api token
            auth = JupyterHubAuth(api_token=uuid.uuid4().hex)

            async with Gateway(
                address=gateway_proc.public_url, asynchronous=True, auth=auth
            ) as gateway:

                # Auth fails with bad token
                with pytest.raises(Exception):
                    await gateway.list_clusters()

                # Auth works with correct token
                auth.api_token = api_token
                await gateway.list_clusters()
