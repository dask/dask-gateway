import asyncio
import shutil
import socket

from traitlets import Integer, TraitError


def random_port():
    """Get a single random port."""
    with socket.socket() as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


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
