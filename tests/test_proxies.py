import asyncio
import ssl

import pytest
from aiohttp import web, ClientSession
from distributed import Client
from distributed.security import Security
from distributed.deploy.local import LocalCluster

from dask_gateway.client import GatewaySecurity
from dask_gateway_server.proxy import Proxy
from dask_gateway_server.tls import new_keypair
from dask_gateway_server.utils import random_port

from .utils_test import aiohttp_server, with_retries


class temp_proxy(object):
    def __init__(self, **kwargs):
        self._port = random_port()
        self.proxy = Proxy(
            address="127.0.0.1:0",
            prefix="/foobar",
            gateway_address=f"127.0.0.1:{self._port}",
            log_level="debug",
            proxy_status_period=0.5,
            **kwargs,
        )
        self.app = web.Application()
        self.runner = web.AppRunner(self.app)

    async def __aenter__(self):
        await self.proxy.setup(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", self._port)
        await self.site.start()
        await self.proxy._proxy_contacted
        return self.proxy

    async def __aexit__(self, *args):
        await self.runner.cleanup()
        await self.proxy.cleanup()


@pytest.fixture(params=["separate", "shared"])
async def proxy(request):
    kwargs = {"tcp_address": "127.0.0.1:0"} if request.param == "separate" else {}
    async with temp_proxy(**kwargs) as proxy:
        yield proxy


@pytest.fixture
async def cluster_and_security(tmpdir):
    tls_cert, tls_key = new_keypair("temp")
    tls_key_path = str(tmpdir.join("dask.pem"))
    tls_cert_path = str(tmpdir.join("dask.crt"))
    with open(tls_key_path, "wb") as f:
        f.write(tls_key)
    with open(tls_cert_path, "wb") as f:
        f.write(tls_cert)

    security = Security(
        tls_scheduler_key=tls_key_path,
        tls_scheduler_cert=tls_cert_path,
        tls_client_key=tls_key_path,
        tls_client_cert=tls_cert_path,
        tls_worker_key=tls_key_path,
        tls_worker_cert=tls_cert_path,
        tls_ca_file=tls_cert_path,
        require_encryption=True,
    )
    client_security = GatewaySecurity(tls_key.decode(), tls_cert.decode())

    cluster = None
    try:
        cluster = await LocalCluster(
            0,
            scheduler_port=0,
            silence_logs=False,
            dashboard_address=None,
            security=security,
            asynchronous=True,
        )
        yield cluster, client_security
    finally:
        if cluster is not None:
            await cluster.close()


hello_routes = web.RouteTableDef()


@hello_routes.get("/")
async def hello(request):
    return web.Response(text="Hello world")


@pytest.mark.asyncio
async def test_web_proxy(proxy):
    async with ClientSession() as client, aiohttp_server(hello_routes) as server:
        # Add a route
        await proxy.add_route(kind="PATH", path="/hello", target=server.address)

        proxied_addr = f"http://{proxy.address}{proxy.prefix}/hello"

        # Proxy works
        async def test_works():
            resp = await client.get(proxied_addr)
            assert resp.status == 200
            body = await resp.text()
            assert "Hello world" == body

        await with_retries(test_works, 5)

        # Remove the route
        await proxy.remove_route(kind="PATH", path="/hello")
        # Delete idempotent
        await proxy.remove_route(kind="PATH", path="/hello")

        # Route no longer available
        async def test_fails():
            resp = await client.get(proxied_addr)
            assert resp.status == 404

        await with_retries(test_fails, 5)


@pytest.mark.asyncio
async def test_web_proxy_bad_target(proxy):
    async with ClientSession() as client:
        # Add a bad route
        addr = "http://127.0.0.1:%d" % random_port()
        await proxy.add_route(kind="PATH", path="/hello", target=addr)

        proxied_addr = f"http://{proxy.address}{proxy.prefix}/hello"

        # Route not available
        async def test_502():
            resp = await client.get(proxied_addr)
            assert resp.status == 502

        await with_retries(test_502, 5)


@pytest.fixture(params=["separate", "shared"])
async def ca_and_tls_proxy(request, tmpdir_factory):
    trustme = pytest.importorskip("trustme")
    ca = trustme.CA()
    cert = ca.issue_cert("127.0.0.1")

    certdir = tmpdir_factory.mktemp("certs")
    tls_key = str(certdir.join("key.pem"))
    tls_cert = str(certdir.join("cert.pem"))

    cert.private_key_pem.write_to_path(tls_key)
    cert.cert_chain_pems[0].write_to_path(tls_cert)

    kwargs = {"tcp_address": "127.0.0.1:0"} if request.param == "separate" else {}
    async with temp_proxy(tls_key=tls_key, tls_cert=tls_cert, **kwargs) as proxy:
        yield ca, proxy


@pytest.mark.asyncio
async def test_web_proxy_public_tls(ca_and_tls_proxy):
    ca, proxy = ca_and_tls_proxy

    async with ClientSession() as client, aiohttp_server(hello_routes) as server:
        ctx = ssl.create_default_context()
        ca.configure_trust(ctx)

        # Add a route
        await proxy.add_route(kind="PATH", path="/hello", target=server.address)

        proxied_addr = f"https://{proxy.address}{proxy.prefix}/hello"

        # Proxy works
        async def test_works():
            resp = await client.get(proxied_addr, ssl=ctx)
            assert resp.status == 200
            body = await resp.text()
            assert "Hello world" == body

        await with_retries(test_works, 5)

        # Remove the route
        await proxy.remove_route(kind="PATH", path="/hello")
        # Delete idempotent
        await proxy.remove_route(kind="PATH", path="/hello")

        # Route no longer available
        async def test_fails():
            resp = await client.get(proxied_addr, ssl=ctx)
            assert resp.status == 404

        await with_retries(test_fails, 5)


@pytest.mark.asyncio
async def test_scheduler_proxy(proxy, cluster_and_security):
    cluster, security = cluster_and_security

    proxied_addr = f"gateway://{proxy.tcp_address}/temp"

    # Add a route
    await proxy.add_route(kind="SNI", sni="temp", target=cluster.scheduler_address)

    # Proxy works
    async def test_works():
        async with Client(proxied_addr, security=security, asynchronous=True) as client:
            res = await client.run_on_scheduler(lambda x: x + 1, 1)
            assert res == 2

    await with_retries(test_works, 5)

    # Remove the route
    await proxy.remove_route(kind="SNI", sni="temp")
    await proxy.remove_route(kind="SNI", sni="temp")


@pytest.mark.asyncio
async def test_resilient_to_proxy_death(proxy):
    async with ClientSession() as client, aiohttp_server(hello_routes) as server:
        # Add a route
        await proxy.add_route(kind="PATH", path="/hello", target=server.address)

        proxied_addr = f"http://{proxy.address}{proxy.prefix}/hello"

        # Proxy works
        async def test_works():
            resp = await client.get(proxied_addr)
            assert resp.status == 200
            body = await resp.text()
            assert "Hello world" == body

        await with_retries(test_works, 5)

        # Kill the proxy process
        proxy.proxy_process.terminate()

        # Give a bit of time to restart
        await asyncio.sleep(2)

        # Process restarts, fetches routing table, and things work again
        await with_retries(test_works, 5)
