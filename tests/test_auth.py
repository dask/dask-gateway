import logging
import os
import uuid

import pytest
from dask_gateway.auth import BasicAuth, JupyterHubAuth
from dask_gateway_server.utils import random_port
from traitlets.config import Config

from .utils_test import temp_gateway

# Testing the Kerberos authenticator requires a Linux system with MIT Kerberos
# installed together with the pykerberos and k5test packages, so it is opt-in
# via an environment variable. When TEST_DASK_GATEWAY_KERBEROS is set, missing
# dependencies fail the test instead of skipping it, so CI can't silently pass
# without testing the Kerberos authenticator.
requires_kerberos = pytest.mark.skipif(
    not os.environ.get("TEST_DASK_GATEWAY_KERBEROS"),
    reason="TEST_DASK_GATEWAY_KERBEROS not set",
)

try:
    import jupyterhub.tests.mocking as hub_mocking
except ImportError:
    hub_mocking = None
else:
    from tornado.log import access_log, app_log, gen_log


@pytest.fixture
def kerberos_realm(monkeypatch):
    """An ephemeral MIT Kerberos realm with a service principal for the gateway.

    Creates a self-contained KDC with k5test, adds an HTTP service principal
    matching the local hostname, and points the process environment at the
    realm so that both the in-process gateway server and the client use it.
    """
    import k5test

    realm = k5test.K5Realm(get_creds=False, create_host=False)
    try:
        http_princ = f"HTTP/{realm.hostname}@{realm.realm}"
        realm.addprinc(http_princ)
        realm.extract_keytab(http_princ, realm.keytab)
        for key, value in realm.env.items():
            monkeypatch.setenv(key, value)
        yield realm
    finally:
        # Stops the KDC daemon and removes the realm's temporary directory
        # (keytab, credential cache, database).
        realm.stop()


async def test_basic_auth():
    async with temp_gateway() as g:
        async with g.gateway_client(auth="basic") as gateway:
            await gateway.list_clusters()


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


@requires_kerberos
async def test_kerberos_auth(kerberos_realm):
    # The hostname in the gateway URL determines the service principal the
    # client requests a ticket for, so the proxy must listen at the same
    # hostname as the HTTP principal created by the kerberos_realm fixture.
    config = Config()
    config.Proxy.address = f"{kerberos_realm.hostname}:0"
    config.DaskGateway.authenticator_class = (
        "dask_gateway_server.auth.KerberosAuthenticator"
    )
    config.KerberosAuthenticator.keytab = kerberos_realm.keytab

    async with temp_gateway(config=config) as g:
        async with g.gateway_client(auth="kerberos") as gateway:
            # The realm is created without credentials, so requests fail
            with pytest.raises(Exception):
                await gateway.list_clusters()

            # After kinit the full mutual authentication handshake succeeds
            kerberos_realm.kinit(
                kerberos_realm.user_princ,
                password=kerberos_realm.password("user"),
            )

            await gateway.list_clusters()


class temp_hub:
    def __init__(self, hub):
        self.hub = hub

    async def __aenter__(self):
        await self.hub.initialize([])
        await self.hub.start()

        # alembic turns off all logs, reenable them for the tests

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


def configure_dask_gateway(jhub_api_token, jhub_bind_url, service_name=""):
    config = Config()
    config.DaskGateway.authenticator_class = (
        "dask_gateway_server.auth.JupyterHubAuthenticator"
    )
    config.JupyterHubAuthenticator.jupyterhub_api_token = jhub_api_token
    config.JupyterHubAuthenticator.jupyterhub_api_url = jhub_bind_url + "api"
    if service_name:
        config.JupyterHubAuthenticator.jupyterhub_service_name = service_name
    return config


@pytest.mark.skipif(not hub_mocking, reason="JupyterHub not installed")
async def test_jupyterhub_auth_legacy(monkeypatch):
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

    hub = MockHub(log=app_log, config=hub_config)

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
async def test_jupyterhub_auth_user(monkeypatch):
    from jupyterhub.tests.utils import add_user

    jhub_api_token = uuid.uuid4().hex
    jhub_bind_url = "http://127.0.0.1:%i/@/space%%20word/" % random_port()

    hub_config = Config()
    hub_config.JupyterHub.services = [
        {"name": "dask-gateway", "api_token": jhub_api_token}
    ]
    hub_config.JupyterHub.bind_url = jhub_bind_url
    hub_config.JupyterHub.load_roles = [
        {
            "name": "dask-users",
            "scopes": ["access:services!service=dask-gateway"],
            "users": ["alice"],
        }
    ]

    class MockHub(hub_mocking.MockHub):
        def init_logging(self):
            pass

    hub = MockHub(log=app_log, config=hub_config)

    # Configure gateway
    config = configure_dask_gateway(
        jhub_api_token, jhub_bind_url, service_name="dask-gateway"
    )

    async with temp_gateway(config=config) as g:
        async with temp_hub(hub):
            # Create a new jupyterhub user alice, and get the api token
            u = add_user(hub.db, name="alice")
            api_token = u.new_api_token()
            hub.db.commit()

            u2 = add_user(hub.db, name="bob")
            wrong_api_token = u2.new_api_token()
            hub.db.commit()

            # Configure auth with incorrect api token
            auth = JupyterHubAuth(api_token=wrong_api_token)

            async with g.gateway_client(auth=auth) as gateway:
                # Auth fails with bad token
                with pytest.raises(Exception):
                    await gateway.list_clusters()

                # Auth works with correct token
                auth.api_token = api_token
                await gateway.list_clusters()


@pytest.mark.skipif(not hub_mocking, reason="JupyterHub not installed")
async def test_jupyterhub_auth_service(monkeypatch):
    jhub_api_token = uuid.uuid4().hex
    jhub_service_token = uuid.uuid4().hex
    other_service_token = uuid.uuid4().hex
    jhub_bind_url = "http://127.0.0.1:%i/@/space%%20word/" % random_port()

    hub_config = Config()
    hub_config.JupyterHub.services = [
        {"name": "dask-gateway", "api_token": jhub_api_token},
        {"name": "any-service", "api_token": jhub_service_token},
        {"name": "other-service", "api_token": other_service_token},
    ]
    hub_config.JupyterHub.bind_url = jhub_bind_url
    hub_config.JupyterHub.load_roles = [
        {
            "name": "dask-users",
            "scopes": ["access:services!service=dask-gateway"],
            "services": ["any-service"],
        }
    ]

    class MockHub(hub_mocking.MockHub):
        def init_logging(self):
            pass

    hub = MockHub(log=app_log, config=hub_config)

    # Configure gateway
    config = configure_dask_gateway(
        jhub_api_token, jhub_bind_url, service_name="dask-gateway"
    )

    async with temp_gateway(config=config) as g:
        async with temp_hub(hub):
            # Configure auth with incorrect api token
            auth = JupyterHubAuth(api_token=other_service_token)
            async with g.gateway_client(auth=auth) as gateway:
                # Auth fails with bad token
                with pytest.raises(Exception):
                    await gateway.list_clusters()

                # Auth works with service token
                auth.api_token = jhub_service_token
                await gateway.list_clusters()
