import asyncio
import ssl
from contextlib import contextmanager

import pytest
from distributed import Client
from distributed.security import Security
from distributed.deploy.local import LocalCluster
from tornado import web
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

from dask_gateway.client import GatewaySecurity
from dask_gateway_server.proxy import SchedulerProxy, WebProxy
from dask_gateway_server.tls import new_keypair
from dask_gateway_server.utils import random_port


@pytest.fixture
async def scheduler_proxy():
    proxy = SchedulerProxy(public_url="tls://127.0.0.1:%s" % random_port())
    try:
        await proxy.start()
        yield proxy
    finally:
        proxy.stop()


@pytest.fixture
async def web_proxy():
    proxy = WebProxy(public_url="http://127.0.0.1:%s" % random_port())
    try:
        await proxy.start()
        yield proxy
    finally:
        proxy.stop()


@pytest.fixture
async def cluster_and_security(tmpdir):
    tls_cert, tls_key = new_keypair("temp")
    tls_key_path = tmpdir.join("dask.pem")
    tls_cert_path = tmpdir.join("dask.crt")
    with open(tls_key_path, "wb") as f:
        f.write(tls_key)
    with open(tls_cert_path, "wb") as f:
        f.write(tls_cert)

    security = Security(
        tls_scheduler_key=tls_key_path,
        tls_scheduler_cert=tls_cert_path,
        tls_ca_file=tls_cert_path,
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


class HelloHandler(web.RequestHandler):
    def get(self):
        self.write("Hello world")


@contextmanager
def hello_server():
    port = random_port()
    app = web.Application([(r"/", HelloHandler)])
    try:
        server = app.listen(port)
        yield "http://127.0.0.1:%d" % port
    finally:
        server.stop()


@pytest.mark.asyncio
async def test_web_proxy(web_proxy):
    assert not await web_proxy.get_all_routes()

    client = AsyncHTTPClient()

    with hello_server() as addr:
        # Add a route
        await web_proxy.add_route("/hello", addr)
        routes = await web_proxy.get_all_routes()
        assert routes == {"/hello": addr}

        # Proxy works
        proxied_addr = web_proxy.public_url + "/hello"
        req = HTTPRequest(url=proxied_addr)
        resp = await client.fetch(req)
        assert resp.code == 200
        assert b"Hello world" == resp.body

        # Remove the route
        await web_proxy.delete_route("/hello")
        assert not await web_proxy.get_all_routes()
        # Delete idempotent
        await web_proxy.delete_route("/hello")

        # Route no longer available
        req = HTTPRequest(url=proxied_addr)
        resp = await client.fetch(req, raise_error=False)
        assert resp.code == 404


@pytest.mark.asyncio
async def test_web_proxy_bad_target(web_proxy):
    assert not await web_proxy.get_all_routes()

    client = AsyncHTTPClient()

    addr = "http://127.0.0.1:%d" % random_port()
    proxied_addr = web_proxy.public_url + "/hello"

    await web_proxy.add_route("/hello", addr)
    routes = await web_proxy.get_all_routes()
    assert routes == {"/hello": addr}

    # Route not available
    req = HTTPRequest(url=proxied_addr)
    resp = await client.fetch(req, raise_error=False)
    assert resp.code == 502


@pytest.mark.asyncio
async def test_web_proxy_api_auth(web_proxy):
    assert not await web_proxy.get_all_routes()

    auth_token = web_proxy.auth_token
    web_proxy.auth_token = "abcdefg"

    # Authentication fails
    with pytest.raises(Exception) as exc:
        await web_proxy.add_route("/foo", "http://127.0.0.1:12345")
    assert exc.value.code == 403

    web_proxy.auth_token = auth_token
    # Route not added
    assert not await web_proxy.get_all_routes()


@pytest.fixture
async def ca_and_tls_web_proxy(tmpdir_factory):
    trustme = pytest.importorskip("trustme")
    ca = trustme.CA()
    cert = ca.issue_cert("127.0.0.1")

    certdir = tmpdir_factory.mktemp("certs")
    tls_key = str(certdir.join("key.pem"))
    tls_cert = str(certdir.join("cert.pem"))

    cert.private_key_pem.write_to_path(tls_key)
    cert.cert_chain_pems[0].write_to_path(tls_cert)

    public_url = "https://127.0.0.1:%s" % random_port()

    proxy = WebProxy(public_url=public_url, tls_key=tls_key, tls_cert=tls_cert)
    try:
        await proxy.start()
        yield ca, proxy
    finally:
        proxy.stop()


@pytest.mark.asyncio
async def test_web_proxy_public_tls(ca_and_tls_web_proxy):
    ca, web_proxy = ca_and_tls_web_proxy

    assert not await web_proxy.get_all_routes()

    client = AsyncHTTPClient()

    with hello_server() as addr:
        # Add a route
        await web_proxy.add_route("/hello", addr)
        routes = await web_proxy.get_all_routes()
        assert routes == {"/hello": addr}

        # Proxy works
        proxied_addr = web_proxy.public_url + "/hello"
        ctx = ssl.create_default_context()
        ca.configure_trust(ctx)
        req = HTTPRequest(url=proxied_addr, ssl_options=ctx)
        resp = await client.fetch(req)
        assert resp.code == 200
        assert b"Hello world" == resp.body

        # Remove the route
        await web_proxy.delete_route("/hello")
        assert not await web_proxy.get_all_routes()
        # Delete idempotent
        await web_proxy.delete_route("/hello")

        # Route no longer available
        req = HTTPRequest(url=proxied_addr, ssl_options=ctx)
        resp = await client.fetch(req, raise_error=False)
        assert resp.code == 404


@pytest.fixture
def two_proxies():
    kwargs = {
        "public_url": "http://127.0.0.1:%s" % random_port(),
        "api_url": "http://127.0.0.1:%s" % random_port(),
        "auth_token": "abcdefg",
    }
    try:
        proxy = WebProxy(**kwargs)
        proxy2 = WebProxy(externally_managed=True, **kwargs)
        yield proxy, proxy2
    finally:
        proxy.stop()
        proxy2.stop()


@pytest.mark.asyncio
async def test_proxy_externally_managed(two_proxies):
    proxy, proxy2 = two_proxies

    # Connect times out
    proxy2.connect_timeout = 0.3
    with pytest.raises(RuntimeError):
        await proxy2.start()

    # Start the proxy sometime in the future
    async def start_proxy():
        await asyncio.sleep(0.5)
        await proxy.start()

    start_task = asyncio.ensure_future(start_proxy())

    # Wait for proxy to start
    proxy2.connect_timeout = 2
    await proxy2.start()

    # Connected proxy works
    routes = await proxy2.get_all_routes()
    assert not routes

    # Stop proxy2
    proxy2.stop()

    # Change the auth_token so connection fails
    proxy2.auth_token = "badvalue"
    with pytest.raises(RuntimeError) as exc:
        await proxy2.start()
    assert "authentication" in str(exc.value)
    assert "DASK_GATEWAY_PROXY_TOKEN" in str(exc.value)

    # Wait for task to finish to cleanup resources
    await start_task


@pytest.mark.asyncio
async def test_scheduler_proxy(scheduler_proxy, cluster_and_security):
    cluster, security = cluster_and_security

    assert not await scheduler_proxy.get_all_routes()

    addr = cluster.scheduler_address
    proxied_addr = "gateway://%s/temp" % scheduler_proxy.public_url.replace(
        "tls://", ""
    )

    # Add a route
    await scheduler_proxy.add_route("/temp", addr)
    routes = await scheduler_proxy.get_all_routes()
    assert routes == {"temp": addr.replace("tls://", "")}

    # Proxy works
    async with Client(proxied_addr, security=security, asynchronous=True) as client:
        res = await client.run_on_scheduler(lambda x: x + 1, 1)
        assert res == 2

    # Remove the route
    await scheduler_proxy.delete_route("/temp")
    assert not await scheduler_proxy.get_all_routes()
    # Delete idempotent
    await scheduler_proxy.delete_route("/temp")
