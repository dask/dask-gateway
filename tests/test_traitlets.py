import pytest
from traitlets import HasTraits, TraitError

from dask_gateway_server.traitlets import Command, Type


def test_Type_traitlet():
    class Foo(HasTraits):
        typ = Type(klass="dask_gateway_server.auth.Authenticator")

    with pytest.raises(TraitError) as exc:
        Foo(typ="dask_gateway_server.auth.not_a_real_path")
    assert "Failed to import" in str(exc.value)

    Foo(typ="dask_gateway_server.auth.SimpleAuthenticator")


def test_Command_traitlet():
    class C(HasTraits):
        cmd = Command("default command")
        cmd2 = Command(["default_cmd"])

    c = C()
    assert c.cmd == ["default command"]
    assert c.cmd2 == ["default_cmd"]
    c.cmd = "foo bar"
    assert c.cmd == ["foo bar"]


def test_worker_threads_kube_cluster():
    kube_backend = pytest.importorskip("dask_gateway_server.backends.kubernetes")
    # KubeClusterConfig allows floats, so determining worker_threads is more complex
    assert kube_backend.KubeClusterConfig().worker_threads == 1
    assert kube_backend.KubeClusterConfig(worker_threads=None).worker_threads == 1
    assert kube_backend.KubeClusterConfig(worker_cores=0.1).worker_threads == 1
    assert kube_backend.KubeClusterConfig(worker_cores_limit=0.1).worker_threads == 1

    assert kube_backend.KubeClusterConfig(worker_cores=2.1).worker_threads == 2
    assert kube_backend.KubeClusterConfig(worker_cores_limit=2.1).worker_threads == 1
    assert (
        kube_backend.KubeClusterConfig(
            worker_cores=2.1, worker_threads=None
        ).worker_threads
        == 2
    )
    assert (
        kube_backend.KubeClusterConfig(
            worker_cores_limit=2.1, worker_threads=None
        ).worker_threads
        == 1
    )
    assert (
        kube_backend.KubeClusterConfig(
            worker_cores=2.1, worker_threads=1
        ).worker_threads
        == 1
    )
    assert (
        kube_backend.KubeClusterConfig(
            worker_cores_limit=2.1, worker_threads=1
        ).worker_threads
        == 1
    )
