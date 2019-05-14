import pytest

from tornado.ioloop import IOLoop
from tornado.platform.asyncio import AsyncIOMainLoop


@pytest.fixture
def loop():
    AsyncIOMainLoop().install()
    IOLoop.clear_instance()
    IOLoop.clear_current()
    loop = IOLoop()
    loop.make_current()
    assert IOLoop.current() is loop
    try:
        yield loop
    finally:
        try:
            loop.close(all_fds=True)
        except (KeyError, ValueError):
            pass
        IOLoop.clear_instance()
        IOLoop.clear_current()
