# flake8: noqa

import sys

if sys.version_info[:2] >= (3, 7):
    from asyncio import get_running_loop
else:
    from asyncio import _get_running_loop as get_running_loop
