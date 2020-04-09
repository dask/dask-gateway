from copy import deepcopy

import pytest

kubernetes_asyncio = pytest.importorskip("kubernetes_asyncio")

from dask_gateway_server.utils import FrozenAttrDict
from dask_gateway_server.backends.kubernetes.backend import (
    KubeClusterConfig,
    KubeBackend,
)
from dask_gateway_server.backends.kubernetes.controller import (
    KubeController,
    ClusterInfo,
    get_container_state,
    get_container_status,
)
from dask_gateway_server.backends.kubernetes.utils import merge_json_objects


@pytest.mark.parametrize(
    "a,b,sol",
    [
        (
            {"a": {"b": {"c": 1}}},
            {"a": {"b": {"d": 2}}},
            {"a": {"b": {"c": 1, "d": 2}}},
        ),
        ({"a": {"b": {"c": 1}}}, {"a": {"b": 2}}, {"a": {"b": 2}}),
        ({"a": [1, 2]}, {"a": [3, 4]}, {"a": [1, 2, 3, 4]}),
        ({"a": {"b": 1}}, {"a2": 3}, {"a": {"b": 1}, "a2": 3}),
        ({"a": 1}, {}, {"a": 1}),
    ],
)
def test_merge_json_objects(a, b, sol):
    a_orig = deepcopy(a)
    b_orig = deepcopy(b)
    res = merge_json_objects(a, b)
    assert res == sol
    assert a == a_orig
    assert b == b_orig


def test_get_container_status():
    c1_status = {"name": "c1"}
    c2_status = {"name": "c2"}
    pod = {"status": {"containerStatuses": [c1_status, c2_status]}}
    assert get_container_status(pod, "c1") == c1_status
    assert get_container_status(pod, "c2") == c2_status
    assert get_container_status(pod, "c3") is None


def test_get_container_state():
    c1_status = {"name": "c1", "state": {"running": {}}}
    c2_status = {"name": "c2", "state": {"terminated": {}}}
    pod = {"status": {"phase": "Running", "containerStatuses": [c1_status, c2_status]}}
    assert get_container_state(pod, "c1") == "running"
    assert get_container_state(pod, "c2") == "terminated"
    assert get_container_state(pod, "c3") == "unknown"
    pod["status"]["phase"] = "Pending"
    assert get_container_state(pod, "c1") == "waiting"


def test_cluster_info():
    info = ClusterInfo()
    assert info.should_trigger()

    info.set_expectations(creates=3)

    for _ in range(3):
        info.on_worker_pending("a")
    info.on_worker_running("a")
    assert not info.should_trigger()

    info.on_worker_pending("b")
    info.on_worker_pending("c")
    assert info.should_trigger()

    assert info.all_pods == set("abc")
    assert info.running == {"a"}
    assert info.pending == {"b", "c"}

    info.on_worker_pending("a")
    assert info.pending == set("abc")
    assert not info.running

    info.on_worker_failed("b")
    assert "b" not in info.pending
    assert "b" in info.failed

    info.on_worker_succeeded("c")
    assert "c" not in info.pending
    assert "c" in info.succeeded

    assert info.should_trigger()
    info.set_expectations(deletes=2)
    info.on_worker_deleted("c")
    info.on_worker_deleted("c")
    assert not info.should_trigger()
    info.on_worker_deleted("b")
    assert info.should_trigger()


def test_make_cluster_object():
    backend = KubeBackend(gateway_instance="instance-1234")

    config = KubeClusterConfig()
    options = {"somekey": "someval"}

    obj = backend.make_cluster_object("alice", options, config)

    name = obj["metadata"]["name"]
    labels = obj["metadata"]["labels"]
    assert labels["gateway.dask.org/instance"] == "instance-1234"
    assert labels["gateway.dask.org/cluster"] == name

    spec = obj["spec"]
    sol = {"options": options, "config": config.to_dict(), "username": "alice"}
    assert spec == sol


def example_config():
    sched_tol = {
        "key": "foo",
        "operator": "Equal",
        "value": "bar",
        "effect": "NoSchedule",
    }
    worker_tol = {
        "key": "foo",
        "operator": "Equal",
        "value": "baz",
        "effect": "NoSchedule",
    }
    kwargs = {
        "namespace": "mynamespace",
        "worker_extra_pod_config": {"tolerations": [worker_tol]},
        "worker_extra_container_config": {"workingDir": "/worker"},
        "worker_memory": "4G",
        "worker_memory_limit": "6G",
        "worker_cores": 2,
        "worker_cores_limit": 3,
        "scheduler_extra_pod_config": {"tolerations": [sched_tol]},
        "scheduler_extra_container_config": {"workingDir": "/scheduler"},
        "scheduler_memory": "2G",
        "scheduler_memory_limit": "3G",
        "scheduler_cores": 1,
        "scheduler_cores_limit": 2,
        "scheduler_extra_pod_labels": {"user-specific-label": "orkbork_scheduler"},
        "worker_extra_pod_labels": {"user-specific-label": "orkbork_worker"},
        "scheduler_extra_pod_annotations": {
            "user-specific-annotation": "scheduler_role1"
        },
        "worker_extra_pod_annotations": {"user-specific-annotation": "worker_role1"},
    }
    return FrozenAttrDict(KubeClusterConfig(**kwargs).to_dict())


@pytest.mark.parametrize("is_worker", [False, True])
def test_make_pod(is_worker):
    controller = KubeController(
        gateway_instance="instance-1234", api_url="http://example.com/api"
    )

    config = example_config()
    namespace = config.namespace
    cluster_name = "c1234"

    pod = controller.make_pod(namespace, cluster_name, config, is_worker=is_worker)

    if is_worker:
        component = "dask-worker"
        tolerations = config.worker_extra_pod_config["tolerations"]
        workdir = "/worker"
        resources = {
            "limits": {"cpu": "3.0", "memory": str(6 * 2 ** 30)},
            "requests": {"cpu": "2.0", "memory": str(4 * 2 ** 30)},
        }
    else:
        component = "dask-scheduler"
        tolerations = config.scheduler_extra_pod_config["tolerations"]
        workdir = "/scheduler"
        resources = {
            "limits": {"cpu": "2.0", "memory": str(3 * 2 ** 30)},
            "requests": {"cpu": "1.0", "memory": str(2 * 2 ** 30)},
        }

    labels = pod["metadata"]["labels"]
    assert labels["gateway.dask.org/instance"] == "instance-1234"
    assert labels["gateway.dask.org/cluster"] == cluster_name
    assert labels["app.kubernetes.io/component"] == component

    annotations = pod["metadata"]["annotations"]

    if is_worker:
        assert labels["user-specific-label"] == "orkbork_worker"
        assert annotations["user-specific-annotation"] == "worker_role1"
    else:
        assert labels["user-specific-label"] == "orkbork_scheduler"
        assert annotations["user-specific-annotation"] == "scheduler_role1"

    assert pod["spec"]["tolerations"] == tolerations
    container = pod["spec"]["containers"][0]
    assert container["workingDir"] == workdir

    assert container["resources"] == resources


def test_make_secret():
    controller = KubeController(
        gateway_instance="instance-1234", api_url="http://example.com/api"
    )

    cluster_name = "c1234"

    secret = controller.make_secret(cluster_name)

    labels = secret["metadata"]["labels"]
    assert labels["gateway.dask.org/instance"] == "instance-1234"
    assert labels["gateway.dask.org/cluster"] == cluster_name
    assert labels["app.kubernetes.io/component"] == "credentials"

    assert set(secret["data"].keys()) == {"dask.crt", "dask.pem", "api-token"}


def test_make_service():
    controller = KubeController(
        gateway_instance="instance-1234", api_url="http://example.com/api"
    )

    cluster_name = "c1234"

    service = controller.make_service(cluster_name)

    labels = service["metadata"]["labels"]
    assert labels["gateway.dask.org/instance"] == "instance-1234"
    assert labels["gateway.dask.org/cluster"] == cluster_name
    assert labels["app.kubernetes.io/component"] == "dask-scheduler"

    selector = service["spec"]["selector"]
    assert selector["gateway.dask.org/cluster"] == cluster_name
    assert selector["gateway.dask.org/instance"] == "instance-1234"
    assert selector["app.kubernetes.io/component"] == "dask-scheduler"


def test_make_ingressroute():
    middlewares = [{"name": "my-middleware"}]

    controller = KubeController(
        gateway_instance="instance-1234",
        api_url="http://example.com/api",
        proxy_prefix="/foo/bar",
        proxy_web_middlewares=middlewares,
    )

    cluster_name = "c1234"
    namespace = "mynamespace"

    ingress = controller.make_ingressroute(cluster_name, namespace)

    labels = ingress["metadata"]["labels"]
    assert labels["gateway.dask.org/instance"] == "instance-1234"
    assert labels["gateway.dask.org/cluster"] == cluster_name
    assert labels["app.kubernetes.io/component"] == "dask-scheduler"

    route = ingress["spec"]["routes"][0]
    assert route["middlewares"] == middlewares
    assert (
        route["match"] == f"PathPrefix(`/foo/bar/clusters/{namespace}.{cluster_name}/`)"
    )


def test_make_ingressroutetcp():
    controller = KubeController(
        gateway_instance="instance-1234", api_url="http://example.com/api"
    )

    cluster_name = "c1234"
    namespace = "mynamespace"

    ingress = controller.make_ingressroutetcp(cluster_name, namespace)

    labels = ingress["metadata"]["labels"]
    assert labels["gateway.dask.org/instance"] == "instance-1234"
    assert labels["gateway.dask.org/cluster"] == cluster_name
    assert labels["app.kubernetes.io/component"] == "dask-scheduler"

    route = ingress["spec"]["routes"][0]
    assert route["match"] == f"HostSNI(`daskgateway-{namespace}.{cluster_name}`)"
    assert ingress["spec"]["tls"]["passthrough"]
