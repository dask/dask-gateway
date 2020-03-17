import asyncio
import socket
import time

import pytest

from dask_gateway_server.utils import (
    format_bytes,
    classname,
    normalize_address,
    awaitable,
    cancel_task,
    TaskPool,
    LRUCache,
    Flag,
    FrozenAttrDict,
    CancelGroup,
    RateLimiter,
)


def test_normalize_address():
    host = socket.gethostname()
    assert normalize_address(":8000") == ":8000"
    assert normalize_address(":8000", resolve_host=True) == f"{host}:8000"
    assert normalize_address("0.0.0.0:8000", resolve_host=True) == f"{host}:8000"
    assert normalize_address("localhost:8000", resolve_host=True) == "localhost:8000"
    res = normalize_address("localhost:0")
    h, p = res.split(":")
    assert h == "localhost"
    assert int(p) > 0

    with pytest.raises(ValueError):
        normalize_address("http://foo.bar:80")

    with pytest.raises(ValueError):
        normalize_address("http://foo.bar")


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
async def test_task_pool():
    pool = TaskPool()

    async def foo():
        return "hello"

    res = await pool.spawn(foo())
    assert res == "hello"
    await asyncio.sleep(0.001)
    assert len(pool.tasks) == 0

    task_stopped = False

    async def background():
        nonlocal task_stopped
        try:
            await asyncio.sleep(1000)
        except asyncio.CancelledError:
            task_stopped = True

    pool.spawn(background())
    await asyncio.sleep(0.001)
    assert not task_stopped
    await pool.close()
    assert task_stopped
    assert len(pool.tasks) == 0


@pytest.mark.asyncio
async def test_awaitable():
    async def afoo():
        return 1

    def foo():
        return 1

    res = await awaitable(foo())
    assert res == 1

    res = await awaitable(afoo())
    assert res == 1


def test_lru_cache():
    cache = LRUCache(2)
    cache.put(1, 2)
    cache.put(3, 4)
    assert cache.get(1) == 2
    assert cache.get(3) == 4
    cache.put(5, 6)
    assert cache.get(1) is None
    assert cache.get(3) == 4
    assert cache.get(5) == 6

    cache.discard(5)
    assert cache.get(5) is None
    # No-op if not present
    cache.discard(5)

    cache.put(7, 8)
    assert cache.get(3) == 4
    assert cache.get(7) == 8


@pytest.mark.asyncio
@pytest.mark.parametrize("use_wait", [False, True])
async def test_flag(use_wait):
    flag = Flag()

    triggered = False

    async def wait(flag):
        nonlocal triggered
        if use_wait:
            await asyncio.wait([flag])
        else:
            await flag
        if flag.is_set():
            triggered = True

    res = asyncio.ensure_future(wait(flag))

    flag.set()
    await res
    assert triggered

    assert flag.is_set()
    # Further calls to set don't fail
    flag.set()

    # Further awaits trigger right away
    await flag

    # Cancelling a waiter doesn't trigger the flag
    flag = Flag()
    triggered = False

    res = asyncio.ensure_future(wait(flag))
    await cancel_task(res)
    assert res.cancelled()
    assert not triggered
    assert not flag.is_set()
    flag.set()
    assert flag.is_set()
    await flag


def test_frozenattrdict():
    d = {"a": 1, "b": 2, "if": 3, "has space": 4}

    a = FrozenAttrDict(d)
    assert a.a == 1
    assert a["a"] == 1
    assert dict(a) == d
    assert len(a) == len(d)

    tab_completion = set(dir(a))
    assert "a" in tab_completion
    assert "if" not in tab_completion
    assert "has space" not in tab_completion

    with pytest.raises(AttributeError):
        a.missing

    with pytest.raises(AttributeError):
        a.b = 1

    with pytest.raises(AttributeError):
        a.missing = 1


@pytest.mark.asyncio
async def test_cancel_group():
    cg = CancelGroup()

    async def waiter(cg):
        async with cg.cancellable():
            await asyncio.sleep(1000)

    async def noop(cg):
        async with cg.cancellable():
            pass

    task1 = asyncio.ensure_future(waiter(cg))
    task2 = asyncio.ensure_future(waiter(cg))
    task3 = asyncio.ensure_future(noop(cg))

    await cancel_task(task2)

    assert not task1.done()
    assert task2.done()
    assert task3.done()

    await cg.cancel()

    assert task1.done()

    try:
        # Further use of the cancel group raises immediately
        async with cg.cancellable():
            assert False
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_rate_limiter():
    rl = RateLimiter(rate=1, burst=2)
    assert rl._delay() == 0
    assert rl._delay() == 0
    delay = rl._delay()
    assert 0.9 < delay < 1.1

    rl = RateLimiter(rate=500, burst=500)
    rl._tokens = 0.0
    start = time.monotonic()
    await rl.acquire()
    end = time.monotonic()
    # Call to acquire blocked ~0.002 s
    delay = end - start
    assert delay > 0.001
    assert delay < 0.003
