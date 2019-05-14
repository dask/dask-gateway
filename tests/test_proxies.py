from contextlib import contextmanager

import pytest
from tornado import web
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from dask_gateway_server.proxy import SchedulerProxy, WebProxy
from dask_gateway_server.utils import random_port


@pytest.fixture
async def scheduler_proxy(loop):
    proxy = SchedulerProxy()
    try:
        await proxy.start()
        yield proxy
    finally:
        proxy.stop()


@pytest.fixture
async def web_proxy(loop):
    proxy = WebProxy(public_url="http://127.0.0.1:%s" % random_port())
    try:
        await proxy.start()
        yield proxy
    finally:
        proxy.stop()


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
async def test_web_proxy(web_proxy, loop):
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
async def test_web_proxy_bad_target(web_proxy, loop):
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
async def test_web_proxy_api_auth(web_proxy, loop):
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
