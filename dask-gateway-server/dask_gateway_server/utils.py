import asyncio
import shutil
import socket
import weakref

from traitlets import Integer, TraitError, Type as _Type


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
