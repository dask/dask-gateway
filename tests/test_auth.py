import os
import subprocess

import pytest
from traitlets.config import Config

from dask_gateway import Gateway, BasicAuth
from dask_gateway_server.utils import random_port

from .utils import temp_gateway


try:
    import kerberos

    del kerberos
    skip = not os.environ.get("TEST_DASK_GATEWAY_YARN")
    requires_kerberos = pytest.mark.skipif(skip, reason="No kerberos server running")
except ImportError:
    requires_kerberos = pytest.mark.skipif(True, reason="Cannot import kerberos")


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
