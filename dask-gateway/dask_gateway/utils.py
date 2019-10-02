import asyncio
import os


def format_template(x):
    if isinstance(x, str):
        return x.format(**os.environ)
    return x


async def cancel_task(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
