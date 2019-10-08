import pytest

from dask_gateway import utils

HTTP_CLIENTS = {
    "curl": "tornado.curl_httpclient.CurlAsyncHTTPClient",
    "simple": "tornado.simple_httpclient.SimpleAsyncHTTPClient",
}


def test_tornado_backend_selector(monkeypatch):
    pytest.importorskip("pycurl")

    with pytest.raises(ValueError):
        utils.select_tornado_backend("junk")

    monkeypatch.setattr(utils, "_has_pycurl", lambda: False)
    with pytest.raises(ValueError):
        utils.select_tornado_backend(backend="curl")

    monkeypatch.setattr(utils, "_has_pycurl", lambda: True)
    backend = utils.select_tornado_backend()
    assert backend == HTTP_CLIENTS["curl"]
    backend = utils.select_tornado_backend(backend="curl")
    assert backend == HTTP_CLIENTS["curl"]

    backend = utils.select_tornado_backend(backend="simple")
    assert backend == HTTP_CLIENTS["simple"]

    monkeypatch.setattr(utils, "_has_pycurl", lambda: False)
    backend = utils.select_tornado_backend()
    assert backend == HTTP_CLIENTS["simple"]
    backend = utils.select_tornado_backend(backend="simple")
    assert backend == HTTP_CLIENTS["simple"]
