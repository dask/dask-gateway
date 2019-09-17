import asyncio
import socket

import pytest
from traitlets import HasTraits, TraitError

from dask_gateway_server.utils import Type, get_connect_urls, timeout


def test_Type_traitlet():
    class Foo(HasTraits):
        typ = Type(klass="dask_gateway_server.managers.ClusterManager")

    with pytest.raises(TraitError) as exc:
        Foo(typ="dask_gateway_server.managers.not_a_real_path")
    assert "Failed to import" in str(exc.value)

    Foo(typ="dask_gateway_server.managers.local.LocalClusterManager")


def test_get_connect_urls():
    hostname = socket.gethostname()
    # When binding on all interfaces, report localhost and hostname
    for url in ["http://:8000", "http://0.0.0.0:8000"]:
        urls = get_connect_urls(url)
        assert urls == ["http://127.0.0.1:8000", "http://%s:8000" % hostname]
    # Otherwise return as is
    urls = get_connect_urls("http://example.com:8000")
    assert urls == ["http://example.com:8000"]


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
