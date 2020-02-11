import asyncio
import socket
import weakref
from collections import OrderedDict
from collections.abc import Mapping
from inspect import isawaitable
from keyword import iskeyword

from colorlog import ColoredFormatter

from .compat import get_running_loop


class TaskPool(object):
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


class CancelGroup(object):
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


class _cancel_context(object):
    def __init__(self, context):
        self.context = context
        self._task = None

    async def __aenter__(self):
        if self.context.cancelled:
            raise asyncio.CancelledError
        loop = get_running_loop()
        try:
            self._task = asyncio.current_task(loop=loop)
        except AttributeError:
            self._task = asyncio.Task.current_task(loop=loop)
        self.context._register(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.context._unregister(self)
        self._task = None


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
    if nbytes > 2 ** 50:
        return "%0.2f PiB" % (nbytes / 2 ** 50)
    if nbytes > 2 ** 40:
        return "%0.2f TiB" % (nbytes / 2 ** 40)
    if nbytes > 2 ** 30:
        return "%0.2f GiB" % (nbytes / 2 ** 30)
    if nbytes > 2 ** 20:
        return "%0.2f MiB" % (nbytes / 2 ** 20)
    if nbytes > 2 ** 10:
        return "%0.2f KiB" % (nbytes / 2 ** 10)
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


class LRUCache(object):
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


class UniqueQueue(asyncio.Queue):
    """A queue that may only contain each item once."""

    def __init__(self, maxsize=0, *, loop=None):
        super().__init__(maxsize=maxsize, loop=loop)
        self._items = set()

    def _put(self, item):
        if item not in self._items:
            self._items.add(item)
            super()._put(item)

    def _get(self):
        item = super()._get()
        self._items.discard(item)
        return item


class Flag(object):
    """A simpler version of asyncio.Event"""

    def __init__(self):
        self._future = get_running_loop().create_future()

    def set(self):
        if not self._future.done():
            self._future.set_result(None)

    def __await__(self):
        return asyncio.shield(self._future).__await__()

    def is_set(self):
        return self._future.done()
