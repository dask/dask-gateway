import sys

if sys.version_info[:2] >= (3, 7):
    from asyncio import get_running_loop, run as asyncio_run
else:
    import asyncio
    from asyncio import _get_running_loop as get_running_loop

    # Borrowed from https://github.com/python/cpython/blob/master/Lib/asyncio/runners.py
    # Can be deleted once we drop 3.6 support

    def asyncio_run(main):
        if get_running_loop() is not None:
            raise RuntimeError(
                "asyncio.run() cannot be called from a running event loop"
            )

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(main)
        finally:
            try:
                _cancel_all_tasks(loop)
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                asyncio.set_event_loop(None)
                loop.close()

    def _cancel_all_tasks(loop):
        to_cancel = asyncio.Task.all_tasks(loop)
        if not to_cancel:
            return

        for task in to_cancel:
            task.cancel()

        loop.run_until_complete(
            asyncio.gather(*to_cancel, loop=loop, return_exceptions=True)
        )

        for task in to_cancel:
            if task.cancelled():
                continue
            if task.exception() is not None:
                loop.call_exception_handler(
                    {
                        "message": "unhandled exception during asyncio.run() shutdown",
                        "exception": task.exception(),
                        "task": task,
                    }
                )
