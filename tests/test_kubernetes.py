from copy import deepcopy

import pytest

kubernetes_asyncio = pytest.importorskip("kubernetes_asyncio")

from dask_gateway_server.backends.kubernetes import (
    KubeClusterConfig,
    KubeBackend,
    merge_json_objects,
)


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


@pytest.mark.asyncio
async def test_make_cluster_object():
    backend = KubeBackend(
        api_url="http://phony.com/api",
        instance="instance-1234",
        namespace="crd-namespace",
    )
    backend.api_client = kubernetes_asyncio.client.ApiClient()

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
        "namespace": "pod-namespace",
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
    }

    options = {"somekey": "someval"}
    config = KubeClusterConfig(**kwargs)

    obj = backend.make_cluster_object("alice", options, config)

    sched = obj["spec"]["scheduler"]["template"]
    worker = obj["spec"]["worker"]["template"]

    for o in [worker, sched]:
        # namespace forwarded
        assert o["metadata"]["namespace"] == "pod-namespace"
        # access to k8s api forbidden
        assert not o["spec"]["automountServiceAccountToken"]

    # Labels forwarded
    for o in [obj, sched, worker]:
        assert o["metadata"]["labels"]["gateway.dask.org/instance"] == "instance-1234"

    # resources are forwarded
    wct = worker["spec"]["containers"][0]
    assert wct["resources"]["requests"]["memory"] == str(4 * 2 ** 30)
    assert wct["resources"]["limits"]["memory"] == str(6 * 2 ** 30)
    assert wct["resources"]["requests"]["cpu"] == "2.0"
    assert wct["resources"]["limits"]["cpu"] == "3.0"
    sct = sched["spec"]["containers"][0]
    assert sct["resources"]["requests"]["memory"] == str(2 * 2 ** 30)
    assert sct["resources"]["limits"]["memory"] == str(3 * 2 ** 30)
    assert sct["resources"]["requests"]["cpu"] == "1.0"
    assert sct["resources"]["limits"]["cpu"] == "2.0"

    # extra config picked up
    assert wct["workingDir"] == "/worker"
    assert worker["spec"]["tolerations"] == [worker_tol]
    assert sct["workingDir"] == "/scheduler"
    assert sched["spec"]["tolerations"] == [sched_tol]
