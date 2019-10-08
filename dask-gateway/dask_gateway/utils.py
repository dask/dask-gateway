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


def _has_pycurl():
    try:
        import pycurl
        return True
    except ImportError:
        return False


def select_tornado_backend(backend=None):
    """Helper function to handle which tornado backend to use

    Parameters
    ----------
    backend : str, optional, one of ``("curl", "simple")``
        Which backend to use -- ``curl`` backend requires ``pycurl`` to be
        installed

    Returns
    -------
    name of backend to use : str
    """

    if backend is not None and backend not in ("curl", "simple"):
        raise ValueError("`backend` should be one of `curl` or `simple`")

    # Configure default AsyncClient backend
    HTTP_CLIENTS = {
        "curl": "tornado.curl_httpclient.CurlAsyncHTTPClient",
        "simple": "tornado.simple_httpclient.SimpleAsyncHTTPClient",
    }

    HAS_PYCURL = _has_pycurl()

    if backend is None and HAS_PYCURL:
        return HTTP_CLIENTS["curl"]
    elif backend is "curl" and not HAS_PYCURL:
        raise ValueError("`curl` client requires `pycurl` but it is not installed")
    elif backend is None:
        return HTTP_CLIENTS["simple"]
    else:
        return HTTP_CLIENTS[backend]
