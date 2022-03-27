import asyncio
import socket
import time
import weakref
from collections import OrderedDict
from collections.abc import Mapping
from inspect import isawaitable
from keyword import iskeyword

import aiohttp.abc
from colorlog import ColoredFormatter


def timestamp():
    """An integer timestamp represented as milliseconds since the epoch UTC"""
    return int(time.time() * 1000)


class TaskPool:
    def __init__(self):
        self.tasks = weakref.WeakSet()
        self.closed = False

    def spawn(self, task):
        out = asyncio.ensure_future(task)
        self.tasks.add(out)
        return out

    async def close(self):
        # Stop all tasks
        self.closed = True
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()


class CancelGroup:
    def __init__(self):
        self.tasks = set()
        self.cancelled = False

    def cancellable(self):
        return _cancel_context(self)

    async def cancel(self):
        if self.cancelled:
            raise asyncio.CancelledError
        else:
            self.cancelled = True
            tasks = [t._task for t in self.tasks if t._task is not None]
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.tasks.clear()

    def _register(self, it):
        self.tasks.add(it)

    def _unregister(self, it):
        self.tasks.discard(it)


class _cancel_context:
    def __init__(self, context):
        self.context = context
        self._task = None

    async def __aenter__(self):
        if self.context.cancelled:
            raise asyncio.CancelledError
        try:
            self._task = asyncio.current_task()
        except AttributeError:
            self._task = asyncio.Task.current_task()
        self.context._register(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.context._unregister(self)
        self._task = None


class RateLimiter:
    """A token-bucket based rate limiter.

    The bucket starts out with ``burst`` tokens, and is replenished at ``rate``
    tokens per-second. Calls to ``acquire`` take a token, possibly blocking
    until one is available.
    """

    def __init__(self, rate=10, burst=100):
        self.rate = float(rate)
        self.burst = float(burst)
        self._tokens = float(burst)
        self._max_elapsed = burst / rate

        now = time.monotonic()
        self._last = now
        self._last_event = now

    def _delay(self):
        now = time.monotonic()
        elapsed = min(now - self._last, self._max_elapsed)
        tokens = min(self._tokens + elapsed * self.rate, self.burst)

        tokens -= 1
        if tokens < 0:
            delay = -tokens / self.rate
        else:
            delay = 0

        self._tokens = tokens
        self._last = now
        self._last_event = now + delay
        return delay

    async def acquire(self):
        """Acquire a rate-limited token."""
        delay = self._delay()
        if delay:
            await asyncio.sleep(delay)


def random_port():
    """Get a single random port."""
    with socket.socket() as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


def normalize_address(address, resolve_host=False):
    try:
        host, port = address.split(":")
        port = int(port)
        if resolve_host and host in {"", "0.0.0.0"}:
            host = socket.gethostname()
        if port == 0:
            port = random_port()
    except Exception:
        raise ValueError(f"Invalid address `{address}`, should be of form `host:port`")

    return f"{host}:{port}"


async def cancel_task(task):
    if task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def format_bytes(nbytes):
    """Format ``nbytes`` as a human-readable string with units"""
    if nbytes > 2**50:
        return "%0.2f PiB" % (nbytes / 2**50)
    if nbytes > 2**40:
        return "%0.2f TiB" % (nbytes / 2**40)
    if nbytes > 2**30:
        return "%0.2f GiB" % (nbytes / 2**30)
    if nbytes > 2**20:
        return "%0.2f MiB" % (nbytes / 2**20)
    if nbytes > 2**10:
        return "%0.2f KiB" % (nbytes / 2**10)
    return "%d B" % nbytes


def classname(cls):
    """Return the full qualified name of a class"""
    mod = cls.__module__
    return cls.__name__ if mod is None else f"{mod}.{cls.__name__}"


class LogFormatter(ColoredFormatter):
    def __init__(self, fmt=None, datefmt=None):
        super().__init__(
            fmt=fmt,
            datefmt=datefmt,
            reset=False,
            log_colors={
                "DEBUG": "blue",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )


class AccessLogger(aiohttp.abc.AbstractAccessLogger):
    def log(self, request, response, time):
        if response.status >= 500:
            level = self.logger.error
        elif response.status >= 400:
            level = self.logger.warning
        elif request.path.endswith("/api/health"):
            level = self.logger.debug
        else:
            level = self.logger.info
        level(
            "%d %s %s %.3fms",
            response.status,
            request.method,
            request.path_qs,
            time * 1000,
        )


async def awaitable(obj):
    if isawaitable(obj):
        return await obj
    return obj


class FrozenAttrDict(Mapping):
    """A dict that also allows attribute access for keys"""

    __slots__ = ("_mapping",)

    def __init__(self, mapping):
        self._mapping = mapping

    def __getattr__(self, k):
        if k in self._mapping:
            return self._mapping[k]
        raise AttributeError(k)

    def __getitem__(self, k):
        return self._mapping[k]

    def __iter__(self):
        return iter(self._mapping)

    def __len__(self):
        return len(self._mapping)

    def __dir__(self):
        out = set(dir(type(self)))
        out.update(k for k in self if k.isidentifier() and not iskeyword(k))
        return list(out)


class LRUCache:
    """A LRU cache"""

    def __init__(self, max_size):
        self.cache = OrderedDict()
        self.max_size = max_size

    def get(self, key):
        """Get an item from the cache. Returns None if not present"""
        try:
            self.cache.move_to_end(key)
            return self.cache[key]
        except KeyError:
            return None

    def put(self, key, value):
        """Add an item to the cache"""
        self.cache[key] = value
        if len(self.cache) > self.max_size:
            self.cache.popitem(False)

    def discard(self, key):
        """Remove an item from the cache. No-op if not present."""
        try:
            del self.cache[key]
        except KeyError:
            pass


class Flag:
    """A simpler version of asyncio.Event"""

    def __init__(self):
        self._future = asyncio.get_running_loop().create_future()

    def set(self):
        if not self._future.done():
            self._future.set_result(None)

    def __await__(self):
        return asyncio.shield(self._future).__await__()

    def is_set(self):
        return self._future.done()


def run_main(main):
    """Main entrypoint for asyncio tasks.

    This differs from `asyncio.run` in one key way - the main task is cancelled
    *first*, then any outstanding tasks are cancelled (and logged, remaining
    tasks are indicative of bugs/failures).
    """
    if asyncio._get_running_loop() is not None:
        raise RuntimeError("Cannot be called from inside a running event loop")

    main_task = None
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        main_task = loop.create_task(main)
        return loop.run_until_complete(main_task)
    finally:
        try:
            if main_task is not None:
                loop.run_until_complete(cancel_task(main_task))
            _cancel_all_tasks(loop)
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            asyncio.set_event_loop(None)
            loop.close()


def _cancel_all_tasks(loop):
    to_cancel = asyncio.all_tasks(loop)
    if not to_cancel:
        return

    for task in to_cancel:
        task.cancel()

    loop.run_until_complete(asyncio.gather(*to_cancel, return_exceptions=True))

    for task in to_cancel:
        if task.cancelled():
            continue
        if task.exception() is not None:
            loop.call_exception_handler(
                {
                    "message": "unhandled exception during shutdown",
                    "exception": task.exception(),
                    "task": task,
                }
            )
