import socket

import pytest
from traitlets import HasTraits, TraitError

from dask_gateway_server.utils import Type, get_connect_urls


def test_Type_traitlet():
    class Foo(HasTraits):
        typ = Type(klass="dask_gateway_server.managers.ClusterManager")

    with pytest.raises(TraitError) as exc:
        Foo(typ="dask_gateway_server.managers.not_a_real_path")
    assert "Failed to import" in str(exc.value)

    Foo(typ="dask_gateway_server.managers.local.LocalClusterManager")


def test_get_connect_urls():
    hostname = socket.gethostname()
    # When binding on all interfaces, report localhost and hostname
    for url in ["http://:8000", "http://0.0.0.0:8000"]:
        urls = get_connect_urls(url)
        assert urls == ["http://127.0.0.1:8000", "http://%s:8000" % hostname]
    # Otherwise return as is
    urls = get_connect_urls("http://example.com:8000")
    assert urls == ["http://example.com:8000"]
