import os
import pytest

if not os.environ.get("TEST_DASK_GATEWAY_KUBE"):
    pytest.skip("Not running Kubernetes tests", allow_module_level=True)

pytest.importorskip("kubernetes")

import kubernetes.client
from kubernetes.client.rest import ApiException

from dask_gateway_server.compat import get_running_loop
from dask_gateway_server.managers.kubernetes import KubeClusterManager, PodReflector

from .utils import ClusterManagerTests


NAMESPACE = os.environ.get("TEST_DASK_GATEWAY_KUBE_NAMESPACE", "dask-gateway")


pytestmark = pytest.mark.usefixtures("cleanup_applications")


@pytest.fixture(scope="module")
def cleanup_applications():
    yield

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
