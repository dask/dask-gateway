import sys

if sys.version_info[:2] >= (3, 7):
    from asyncio import get_running_loop, all_tasks
else:
    import asyncio
    from asyncio import _get_running_loop as get_running_loop  # noqa

    def all_tasks(loop=None):
        return {t for t in asyncio.Task.all_tasks(loop) if not t.done()}
