import pytest
from traitlets import HasTraits, TraitError

from dask_gateway_server.traitlets import Type


def test_Type_traitlet():
    class Foo(HasTraits):
        typ = Type(klass="dask_gateway_server.auth.Authenticator")

    with pytest.raises(TraitError) as exc:
        Foo(typ="dask_gateway_server.auth.not_a_real_path")
    assert "Failed to import" in str(exc.value)

    Foo(typ="dask_gateway_server.auth.SimpleAuthenticator")
