import asyncio
import socket

import pytest
from traitlets import HasTraits, TraitError

from dask_gateway_server.utils import (
    Type,
    timeout,
    format_bytes,
    classname,
    ServerUrls,
    nullcontext,
)


def test_Type_traitlet():
    class Foo(HasTraits):
        typ = Type(klass="dask_gateway_server.managers.ClusterManager")

    with pytest.raises(TraitError) as exc:
        Foo(typ="dask_gateway_server.managers.not_a_real_path")
    assert "Failed to import" in str(exc.value)

    Foo(typ="dask_gateway_server.managers.local.LocalClusterManager")


def test_ServerUrls():
    hostname = socket.gethostname()

    urls = ServerUrls("http://:8000/foo/bar/")
    assert urls.bind.scheme == "http"
    assert urls.bind.path == "/foo/bar"
    assert urls.bind_host == ""
    assert urls.bind_port == 8000
    assert urls.bind_url == "http://:8000/foo/bar"
    assert urls.connect_url == f"http://{hostname}:8000/foo/bar"
    assert urls._to_log == [urls.connect_url, "http://127.0.0.1:8000/foo/bar"]

    urls = ServerUrls("tls://127.0.0.1:0")
    assert urls.bind.scheme == "tls"
    assert urls.bind.port != 0
    assert urls.bind_port == urls.bind.port
    assert urls.connect_url == urls.bind_url
    assert urls._to_log == [urls.connect_url]

    urls = ServerUrls("http://127.0.0.1:8000", "http://foo.com/fizz/buzz/")
    assert urls.bind_url == "http://127.0.0.1:8000"
    assert urls.connect_url == "http://foo.com/fizz/buzz"

    # bind port chosen based on scheme
    assert ServerUrls("http://foo.com").bind_port == 80
    assert ServerUrls("https://foo.com").bind_port == 443
    assert ServerUrls("tls://foo.com").bind_port == 8786


@pytest.mark.asyncio
async def test_timeout():
    async def inner():
        try:
            await asyncio.sleep(2)
        except asyncio.CancelledError:
            raise
        else:
            assert False, "Not cancelled"

    with pytest.raises(asyncio.TimeoutError):
        async with timeout(0.01) as t:
            await inner()
    assert t.expired


@pytest.mark.asyncio
async def test_timeout_forwards_exceptions():
    with pytest.raises(Exception):
        async with timeout(0.01) as t:
            raise Exception
    assert t._waiter is None
    assert not t.expired


@pytest.mark.asyncio
async def test_timeout_supports_cancellation():
    # Unrelated CancelledError raises properly
    with pytest.raises(asyncio.CancelledError):
        async with timeout(0.01) as t:
            raise asyncio.CancelledError
    assert t._waiter is None
    assert not t.expired

    # Cancelling an outer task cancels the inner and clears the timeout
    inner_task = asyncio.ensure_future(asyncio.sleep(2))

    async def waiter():
        try:
            async with timeout(2) as t:
                outer_task.cancel()
                await inner_task
        finally:
            assert not t.expired
            assert t._waiter is None

    outer_task = asyncio.ensure_future(waiter())
    with pytest.raises(asyncio.CancelledError):
        await outer_task
    assert inner_task.cancelled()


def test_format_bytes():
    assert format_bytes(105) == "105 B"
    assert format_bytes(1.5 * 2 ** 10) == "1.50 KiB"
    assert format_bytes(1.5 * 2 ** 20) == "1.50 MiB"
    assert format_bytes(1.5 * 2 ** 30) == "1.50 GiB"
    assert format_bytes(1.5 * 2 ** 40) == "1.50 TiB"
    assert format_bytes(1.5 * 2 ** 50) == "1.50 PiB"


class Foo(object):
    pass


def test_classname():
    assert classname(Foo) == f"{Foo.__module__}.Foo"


@pytest.mark.asyncio
async def test_nullcontext():
    async with nullcontext(0) as c:
        assert c == 0

    with nullcontext(0) as c:
        assert c == 0

    with nullcontext() as c:
        assert c is None
