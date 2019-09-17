import asyncio
import shutil
import socket
import weakref
from urllib.parse import urlparse, urlunparse

from traitlets import Integer, TraitError, Type as _Type

from .compat import get_running_loop


class TaskPool(object):
    def __init__(self):
        self.pending_tasks = weakref.WeakSet()
        self.background_tasks = weakref.WeakSet()

    def create_task(self, task):
        out = asyncio.ensure_future(task)
        self.pending_tasks.add(out)
        return out

    def create_background_task(self, task):
        out = asyncio.ensure_future(task)
        self.background_tasks.add(out)
        return out

    async def close(self, timeout=5):
        # Cancel all background tasks
        for task in self.background_tasks:
            task.cancel()

        # Wait for a short period for any ongoing tasks to complete, before
        # canceling them
        try:
            await asyncio.wait_for(
                asyncio.gather(*self.pending_tasks, return_exceptions=True), timeout
            )
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass

        # Now wait for all tasks to be actually completed
        try:
            await asyncio.gather(
                *self.pending_tasks, *self.background_tasks, return_exceptions=True
            )
        except asyncio.CancelledError:
            pass


class timeout(object):
    """An async-contextmanager for managing timeouts.

    If the timeout occurs before the block exits, any running operation under
    the context will be cancelled, and a ``asyncio.TimeoutError`` will be
    raised.

    To check if the timeout expired, you can check the ``expired`` attribute.
    """

    def __init__(self, t):
        self.t = t
        self._task = None
        self._waiter = None
        self.expired = False

    def _cancel_task(self):
        if self._task is not None:
            self._task.cancel()
            self.expired = True

    async def __aenter__(self):
        loop = get_running_loop()
        try:
            self._task = asyncio.current_task(loop=loop)
        except AttributeError:
            self._task = asyncio.Task.current_task(loop=loop)
        self._waiter = loop.call_later(self.t, self._cancel_task)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is asyncio.CancelledError and self.expired:
            self._waiter = None
            self._task = None
            raise asyncio.TimeoutError
        elif self._waiter is not None:
            self._waiter.cancel()
            self._waiter = None
        self._task = None


def random_port():
    """Get a single random port."""
    with socket.socket() as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


def get_ip():
    try:
        # Try resolving by hostname first
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        pass

    # By using a UDP socket, we don't actually try to connect but
    # simply select the local address through which *host* is reachable.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # We use google's DNS server as a common public host to resolve the
        # address. Any ip could go here.
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    except Exception:
        pass
    finally:
        sock.close()

    raise ValueError("Failed to determine local IP address")


def get_connect_urls(url):
    parsed = urlparse(url)
    if parsed.hostname in {None, "", "0.0.0.0"}:
        host = socket.gethostname()
        hosts = ["127.0.0.1", host]
    else:
        hosts = [parsed.hostname]
    return [
        urlunparse(parsed._replace(netloc="%s:%i" % (h, parsed.port))) for h in hosts
    ]


def cleanup_tmpdir(log, path):
    try:
        log.debug("Removing temporary directory %r", path)
        shutil.rmtree(path)
    except Exception as exc:
        log.error("Failed to remove temporary directory %r.", path, exc_info=exc)


async def cancel_task(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# Adapted from JupyterHub
class MemoryLimit(Integer):
    """A specification of a memory limit, with optional units.

    Supported units are:
      - K -> Kibibytes
      - M -> Mebibytes
      - G -> Gibibytes
      - T -> Tebibytes
    """

    UNIT_SUFFIXES = {"K": 2 ** 10, "M": 2 ** 20, "G": 2 ** 30, "T": 2 ** 40}

    def validate(self, obj, value):
        if isinstance(value, (int, float)):
            return int(value)

        try:
            num = float(value[:-1])
        except ValueError:
            raise TraitError(
                "{val} is not a valid memory specification. Must be an int or "
                "a string with suffix K, M, G, T".format(val=value)
            )
        suffix = value[-1]

        if suffix not in self.UNIT_SUFFIXES:
            raise TraitError(
                "{val} is not a valid memory specification. Must be an int or "
                "a string with suffix K, M, G, T".format(val=value)
            )
        return int(float(num) * self.UNIT_SUFFIXES[suffix])


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


class Type(_Type):
    """An implementation of `Type` with better errors"""

    def validate(self, obj, value):
        if isinstance(value, str):
            try:
                value = self._resolve_string(value)
            except ImportError as exc:
                raise TraitError(
                    "Failed to import %r for trait '%s.%s':\n\n%s"
                    % (value, type(obj).__name__, self.name, exc)
                )
        return super().validate(obj, value)


def classname(cls):
    """Return the full qualified name of a class"""
    mod = cls.__module__
    return cls.__name__ if mod is None else f"{mod}.{cls.__name__}"
