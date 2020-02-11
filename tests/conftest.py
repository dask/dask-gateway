import logging

import pytest


@pytest.fixture(autouse=True)
def reset_logs():
    # PDB's stdout/stderr capture can close fds that our loggers are configured
    # to write to. To prevent this, reset the log handlers before every test.
    logging.getLogger("DaskGateway").handlers.clear()
