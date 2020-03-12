import asyncio

import pytest

from dask_gateway_server.workqueue import WorkQueue, Backoff, WorkQueueClosed
from dask_gateway_server.utils import cancel_task


def test_backoff():
    backoff = Backoff(base_delay=0.5, max_delay=5)

    assert backoff.failures("foo") == 0
    assert backoff.backoff("foo") == 0.5

    assert backoff.failures("foo") == 1
    assert backoff.backoff("foo") == 1.0

    assert backoff.failures("foo") == 2
    assert backoff.backoff("foo") == 2.0

    backoff.reset("foo")
    assert backoff.failures("foo") == 0
    assert backoff.backoff("foo") == 0.5

    # No-op on unknown key
    backoff.reset("bar")


@pytest.mark.asyncio
async def test_workqueue_deduplicates():
    q = WorkQueue()

    q.put("foo")
    q.put("bar")
    q.put("foo")

    assert await q.get() == "foo"
    assert await q.get() == "bar"
    assert q.is_empty()


@pytest.mark.asyncio
async def test_workqueue_no_concurrent_execution():
    q = WorkQueue()

    q.put("foo")

    assert await q.get() == "foo"
    assert q.is_empty()
    q.put("foo")
    assert not q.is_empty()
    assert not q._queue
    assert "foo" in q._dirty

    # Cannot concurrently process "foo", even though it's requeued
    f = asyncio.ensure_future(q.get())
    done, pending = await asyncio.wait([f], timeout=0.001)
    assert not done

    # But after marking done, can continue processing
    q.task_done("foo")
    assert q._queue
    res = await f
    assert res == "foo"


@pytest.mark.asyncio
async def test_workqueue_cancellable():
    q = WorkQueue()

    # No items, blocks
    f = asyncio.ensure_future(q.get())
    done, pending = await asyncio.wait([f], timeout=0.001)
    assert not done

    # Cancel the get task, waiter cleaned up
    await cancel_task(f)
    assert not q._waiting

    # No items, blocks
    f = asyncio.ensure_future(q.get())
    done, pending = await asyncio.wait([f], timeout=0.001)
    assert not done

    # Cancel after a put, item still available
    q.put("foo")
    await cancel_task(f)
    assert await q.get() == "foo"


@pytest.mark.asyncio
async def test_workqueue_put_after():
    q = WorkQueue()

    # Fine if already enqueued
    q.put("foo")
    q.put_after("foo", 0.001)
    assert len(q._queue) == 1
    assert q._timers

    assert await q.get() == "foo"

    # Cannot concurrently process "foo", even though it's scheduled
    f = asyncio.ensure_future(q.get())
    done, pending = await asyncio.wait([f], timeout=0.005)
    assert not done

    # But after marking done, can continue processing
    q.task_done("foo")
    assert q._queue
    res = await f
    assert res == "foo"


@pytest.mark.asyncio
async def test_workqueue_put_after_reschedules():
    q = WorkQueue()
    q.put_after("foo", 0.1)
    _, when = q._timers["foo"]

    # Scheduling after is no-op
    q.put_after("foo", 0.5)
    assert len(q._queue) == 0
    assert len(q._timers) == 1
    assert q._timers["foo"][1] == when

    # Scheduling before reschedules
    q.put_after("foo", 0.005)
    assert len(q._queue) == 0
    assert len(q._timers) == 1
    assert q._timers["foo"][1] < when

    assert await q.get() == "foo"

    q.task_done("foo")

    # Scheduling at 0 avoids timer creation
    q.put_after("foo", 0)
    assert not q._timers
    assert q._queue
    assert await q.get() == "foo"


@pytest.mark.asyncio
async def test_workqueue_put_backoff():
    q = WorkQueue()
    q.put_backoff("foo")
    assert q.failures("foo") == 1
    assert q.failures("bar") == 0

    assert await q.get() == "foo"
    q.task_done("foo")

    assert q.failures("foo") == 1

    q.reset_backoff("foo")
    assert q.failures("foo") == 0


@pytest.mark.asyncio
async def test_workqueue_close():
    q = WorkQueue()
    q.put("foo")
    q.close()
    with pytest.raises(WorkQueueClosed):
        await q.get()

    assert q.closed

    q = WorkQueue()

    fs = [asyncio.ensure_future(q.get()) for _ in range(3)]
    done, pending = await asyncio.wait(fs, timeout=0.001)
    assert not done

    q.close()

    for f in fs:
        with pytest.raises(WorkQueueClosed):
            await f
