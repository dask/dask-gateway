import os
import uuid
from copy import deepcopy

import pytest

pytest.importorskip("kubernetes")

import kubernetes.client
from kubernetes.client.rest import ApiException

from dask_gateway_server.compat import get_running_loop
from dask_gateway_server.managers.kubernetes import (
    KubeClusterManager,
    PodReflector,
    merge_kube_objects,
    merge_json_objects,
    kubequote,
)

from .utils import ClusterManagerTests


KUBE_RUNNING = os.environ.get("TEST_DASK_GATEWAY_KUBE")
NAMESPACE = os.environ.get("TEST_DASK_GATEWAY_KUBE_NAMESPACE", "dask-gateway")

pytestmark = pytest.mark.usefixtures("cleanup_applications")


@pytest.fixture(scope="module")
def cleanup_applications():
    yield
    if not KUBE_RUNNING:
        return

    api = kubernetes.client.CoreV1Api()

    pods = api.list_namespaced_pod(
        NAMESPACE, label_selector="apps.kubernetes.io/name = dask-gateway"
    ).items
    for p in pods:
        api.delete_namespaced_pod(p.metadata.name, NAMESPACE)

    secrets = api.list_namespaced_secret(
        NAMESPACE, label_selector="apps.kubernetes.io/name = dask-gateway"
    ).items
    for s in secrets:
        api.delete_namespaced_secret(s.metadata.name, NAMESPACE)

    if pods:
        print("-- Deleted %d lost pods --" % len(pods))
    if secrets:
        print("-- Deleted %d lost secrets --" % len(secrets))


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


def test_merge_kube_objects():
    # Can merge kubernetes objects
    orig = kubernetes.client.models.V1PodSpec(
        containers=[], termination_grace_period_seconds=30
    )
    tol_1 = {"key": "foo", "operator": "Equal", "value": "one", "effect": "NoSchedule"}
    changes = {"tolerations": [tol_1]}
    new = merge_kube_objects(orig, changes)
    sol = {
        "containers": [],
        "terminationGracePeriodSeconds": 30,
        "tolerations": [tol_1],
    }
    assert isinstance(new, dict)
    assert new == sol

    # Can merge dicts
    orig = {
        "containers": [],
        "terminationGracePeriodSeconds": 30,
        "tolerations": [tol_1],
    }
    tol_2 = {"key": "bar", "operator": "Equal", "value": "two", "effect": "NoSchedule"}
    changes = {"terminationGracePeriodSeconds": 25, "tolerations": [tol_2]}
    new = merge_kube_objects(orig, changes)
    sol = {
        "containers": [],
        "terminationGracePeriodSeconds": 25,
        "tolerations": [tol_1, tol_2],
    }
    assert isinstance(new, dict)
    assert new == sol


def test_kubequote():
    assert kubequote("testuser") == "testuser"
    assert kubequote("\x01\x02alice") == "x01x02alice"
    assert kubequote("user_name") == "userx5fname"


@pytest.mark.parametrize("kind", ["scheduler", "worker"])
def test_pod_spec(kind):
    tol = {"key": "foo", "operator": "Equal", "value": "bar", "effect": "NoSchedule"}
    kwargs = {
        f"{kind}_extra_pod_config": {"tolerations": [tol]},
        f"{kind}_extra_container_config": {"workingDir": "/dir"},
        f"{kind}_memory": "4G",
        f"{kind}_memory_limit": "6G",
        f"{kind}_cores": 2,
        f"{kind}_cores_limit": 3,
    }

    manager = KubeClusterManager(
        _testing=True,
        username="\x01alice",
        cluster_name=uuid.uuid4().hex,
        api_token=uuid.uuid4().hex,
        namespace=NAMESPACE,
        **kwargs,
    )
    if kind == "scheduler":
        pod = manager.make_pod_spec("sched-secret-name")
        suffix = manager.cluster_name
    else:
        suffix = uuid.uuid4().hex
        pod = manager.make_pod_spec("sched-secret-name", worker_name=suffix)

    assert pod.metadata.name == f"dask-gateway-x01alice-{kind}-{suffix}"

    spec = pod.spec
    ct = spec["containers"][0]

    # resources are forwarded
    assert ct["resources"]["requests"]["memory"] == 4 * 2 ** 30
    assert ct["resources"]["limits"]["memory"] == 6 * 2 ** 30
    assert ct["resources"]["requests"]["cpu"] == 2
    assert ct["resources"]["limits"]["cpu"] == 3

    # extra config picked up
    assert ct["workingDir"] == "/dir"
    assert spec["tolerations"] == [tol]

    # access to k8s api forbidden
    assert not spec["automountServiceAccountToken"]


@pytest.mark.skipif(not KUBE_RUNNING, reason="Not running Kubernetes tests")
class TestKubeClusterManager(ClusterManagerTests):
    def new_manager(self, **kwargs):
        PodReflector.clear_instance()
        return KubeClusterManager(
            namespace=NAMESPACE,
            scheduler_memory="256M",
            worker_memory="256M",
            scheduler_cores=0.1,
            worker_cores=0.1,
            **kwargs,
        )

    async def cleanup_cluster(self, manager, cluster_state, worker_states):
        for state in worker_states + [cluster_state]:
            await self.delete_pod(manager, state.get("pod_name"))

        secret_name = cluster_state.get("secret_name")
        if secret_name is not None:
            try:
                loop = get_running_loop()
                await loop.run_in_executor(
                    None,
                    manager.kube_client.delete_namespaced_secret,
                    secret_name,
                    manager.namespace,
                )
            except ApiException as e:
                if e.status == 404:
                    return
                raise

    async def delete_pod(self, manager, pod_name):
        loop = get_running_loop()
        if pod_name is not None:
            try:
                await loop.run_in_executor(
                    None,
                    manager.kube_client.delete_namespaced_pod,
                    pod_name,
                    manager.namespace,
                )
            except ApiException as e:
                if e.status == 404:
                    return
                raise

    def pod_is_running(self, manager, pod_name):
        if not pod_name:
            return False
        try:
            pod = manager.kube_client.read_namespaced_pod(pod_name, manager.namespace)
        except ApiException as e:
            if e.status == 404:
                return False
            raise
        return pod.status.phase.lower() in ("pending", "running")

    async def cluster_is_running(self, manager, cluster_state):
        return self.pod_is_running(manager, cluster_state.get("pod_name"))

    async def worker_is_running(self, manager, cluster_state, worker_state):
        return self.pod_is_running(manager, worker_state.get("pod_name"))

    def num_start_cluster_stages(self):
        return 2

    def num_start_worker_stages(self):
        return 1
