import asyncio
import os
import sys
import uuid
from base64 import b64decode
from collections import defaultdict
from datetime import datetime, timezone

from traitlets import Float, Dict, Unicode, default
from kubernetes_asyncio import client, config

from ... import __version__ as VERSION
from ... import models
from ...traitlets import MemoryLimit, Type
from ...utils import cancel_task, UniqueQueue
from ..base import Backend, ClusterConfig
from .utils import Reflector


__all__ = ("KubeClusterConfig", "KubeBackend")


if sys.version_info[:2] >= (3, 7):

    def parse_k8s_timestamp(ts):
        t = datetime.fromisoformat(ts[:-1])
        return int(t.replace(tzinfo=timezone.utc).timestamp() * 1000)


else:

    def parse_k8s_timestamp(ts):
        t = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
        return int(t.replace(tzinfo=timezone.utc).timestamp() * 1000)


class KubeClusterConfig(ClusterConfig):
    """Configuration for a single Dask cluster running on kubernetes"""

    namespace = Unicode(
        "default",
        help="""
        Kubernetes namespace to launch pods in.

        If running inside a kubernetes cluster with service accounts enabled,
        defaults to the current namespace. If not, defaults to `default`
        """,
        config=True,
    )

    @default("namespace")
    def _default_namespace(self):
        ns_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
        if os.path.exists(ns_path):
            with open(ns_path) as f:
                return f.read().strip()
        return "default"

    image = Unicode(
        "daskgateway/dask-gateway:latest",
        help="Docker image to use for running user's containers.",
        config=True,
    )

    image_pull_policy = Unicode(
        "IfNotPresent",
        help="The image pull policy of the docker image specified in ``image``",
        config=True,
    )

    # Kubernetes is a bit different in types/granularity of resource requests.
    # We redefine these common fields here to support that.
    worker_cores = Float(
        1,
        min=0,
        help="""
        Number of cpu-cores available for a dask worker.
        """,
        config=True,
    )

    worker_cores_limit = Float(
        min=0,
        help="""
        Maximum number of cpu-cores available for a dask worker.

        Defaults to ``worker_cores``.
        """,
        config=True,
    )

    @default("worker_cores_limit")
    def _default_worker_cores_limit(self):
        return self.worker_cores

    worker_memory_limit = MemoryLimit(
        help="""
        Maximum number of bytes available for a dask worker. Allows the
        following suffixes:

        - K -> Kibibytes
        - M -> Mebibytes
        - G -> Gibibytes
        - T -> Tebibytes

        Defaults to ``worker_memory``.
        """,
        config=True,
    )

    @default("worker_memory_limit")
    def _default_worker_memory_limit(self):
        return self.worker_memory

    scheduler_cores = Float(
        1,
        min=0,
        help="""
        Number of cpu-cores available for a dask scheduler.
        """,
        config=True,
    )

    scheduler_cores_limit = Float(
        min=0,
        help="""
        Maximum number of cpu-cores available for a dask scheduler.

        Defaults to ``scheduler_cores``.
        """,
        config=True,
    )

    @default("scheduler_cores_limit")
    def _default_scheduler_cores_limit(self):
        return self.scheduler_cores

    scheduler_memory_limit = MemoryLimit(
        help="""
        Maximum number of bytes available for a dask scheduler. Allows the
        following suffixes:

        - K -> Kibibytes
        - M -> Mebibytes
        - G -> Gibibytes
        - T -> Tebibytes

        Defaults to ``scheduler_memory``.
        """,
        config=True,
    )

    @default("scheduler_memory_limit")
    def _default_scheduler_memory_limit(self):
        return self.scheduler_memory

    worker_extra_container_config = Dict(
        help="""
        Any extra configuration for the worker container.

        This dict will be deep merged with the worker container (a
        ``V1Container`` object) before submission. Keys should match those in
        the `kubernetes spec
        <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.15/#container-v1-core>`__,
        and should be camelCase.

        For example, here we add environment variables from a secret to the
        worker container:

        .. code::

            c.KubeClusterConfig.worker_extra_container_config = {
                "envFrom": [
                    {"secretRef": {"name": "my-env-secret"}}
                ]
            }
        """,
        config=True,
    )

    scheduler_extra_container_config = Dict(
        help="""
        Any extra configuration for the scheduler container.

        This dict will be deep merged with the scheduler container (a
        ``V1Container`` object) before submission. Keys should match those in
        the `kubernetes spec
        <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.15/#container-v1-core>`__,
        and should be camelCase.

        See ``worker_extra_container_config`` for more information.
        """,
        config=True,
    )

    worker_extra_pod_config = Dict(
        help="""
        Any extra configuration for the worker pods.

        This dict will be deep merged with the worker pod spec (a ``V1PodSpec``
        object) before submission. Keys should match those in the `kubernetes
        spec
        <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.15/#container-v1-core>`__,
        and should be camelCase.

        For example, here we add a toleration to worker pods.

        .. code::

            c.KubeClusterConfig.worker_extra_pod_config = {
                "tolerations": [
                    {
                        "key": "key",
                        "operator": "Equal",
                        "value": "value",
                        "effect": "NoSchedule",
                    }
                ]
            }
        """,
        config=True,
    )

    scheduler_extra_pod_config = Dict(
        help="""
        Any extra configuration for the scheduler pods.

        This dict will be deep merged with the scheduler pod spec (a
        ``V1PodSpec`` object) before submission. Keys should match those in the
        `kubernetes spec
        <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.15/#container-v1-core>`__,
        and should be camelCase.

        See ``worker_extra_pod_config`` for more information.
        """,
        config=True,
    )


class KubeBackend(Backend):
    """A dask-gateway backend for running on Kubernetes"""

    cluster_config_class = Type(
        "dask_gateway_server.backends.kubernetes.backend.KubeClusterConfig",
        klass="dask_gateway_server.backends.base.ClusterConfig",
        help="The cluster config class to use",
        config=True,
    )

    gateway_instance = Unicode(
        help="""
        A unique ID for this instance of dask-gateway.

        The controller must also be configured with the same ID.
        """,
        config=True,
    )

    @default("instance")
    def _default_instance(self):
        instance = os.environ.get("DASK_GATEWAY_INSTANCE")
        if not instance:
            raise ValueError("Must specify `c.KubeBackend.instance`")
        return instance

    crd_version = Unicode(
        "v1alpha1", help="The version for the DaskCluster CRD", config=True
    )

    common_labels = Dict(
        {
            "app.kubernetes.io/name": "dask-gateway",
            "app.kubernetes.io/version": VERSION.replace("+", "_"),
        },
        help="Kubernetes labels to apply to all objects created by the gateway",
        config=True,
    )

    common_annotations = Dict(
        help="Kubernetes annotations to apply to all objects created by the gateway",
        config=True,
    )

    async def setup(self, app):
        try:
            # Not a coroutine for some reason
            config.load_incluster_config()
        except config.ConfigException:
            await config.load_kube_config()

        self.api_client = client.ApiClient()
        self.core_client = client.CoreV1Api(api_client=self.api_client)
        self.custom_client = client.CustomObjectsApi(api_client=self.api_client)

        self.clusters = {}
        self.username_to_clusters = defaultdict(dict)
        self.queue = UniqueQueue()
        self.reflector = Reflector(
            parent=self,
            name="cluster",
            client=self.custom_client,
            method="list_cluster_custom_object",
            method_kwargs=dict(
                group="gateway.dask.org",
                version=self.crd_version,
                plural="daskclusters",
                label_selector=self.get_label_selector(),
            ),
            queue=self.queue,
        )
        await self.reflector.start()
        self._watcher_task = asyncio.ensure_future(self.watcher_loop())

    async def cleanup(self):
        if hasattr(self, "reflector"):
            await self.reflector.stop()
        if hasattr(self, "_watcher_task"):
            await cancel_task(self._watcher_task)
        if hasattr(self, "api_client"):
            await self.api_client.rest_client.pool_manager.close()

    async def start_cluster(self, user, cluster_options):
        options, config = await self.process_cluster_options(user, cluster_options)

        obj = self.make_cluster_object(user.name, options, config)

        res = await self.custom_client.create_namespaced_custom_object(
            "gateway.dask.org", self.crd_version, config.namespace, "daskclusters", obj
        )

        name = res["metadata"]["name"]
        namespace = res["metadata"]["namespace"]
        cluster_name = f"{namespace}.{name}"

        self.reflector.put(res)
        await self.update_cluster_cache(cluster_name)
        return cluster_name

    async def stop_cluster(self, cluster_name, failed=False):
        namespace, name = cluster_name.split(".")
        await self.custom_client.patch_namespaced_custom_object(
            "gateway.dask.org",
            self.crd_version,
            namespace,
            "daskclusters",
            name,
            [{"op": "add", "path": "/spec/active", "value": False}],
        )

    async def get_cluster(self, cluster_name, wait=False):
        cluster = self.clusters.get(cluster_name)
        return cluster

    async def list_clusters(self, username=None, statuses=None):
        if statuses is None:
            select = lambda x: x.status <= models.ClusterStatus.RUNNING
        else:
            statuses = set(statuses)
            select = lambda x: x.status in statuses
        if username is None:
            return [cluster for cluster in self.clusters.values() if select(cluster)]
        else:
            clusters = self.username_to_clusters.get(username)
            if clusters is None:
                return []
            return [cluster for cluster in clusters.values() if select(cluster)]

    async def on_cluster_heartbeat(self, cluster_name, msg):
        raise NotImplementedError

    def get_label_selector(self):
        return f"gateway.dask.org/instance={self.gateway_instance}"

    def get_labels(self, cluster_name, username, component=None):
        labels = self.common_labels.copy()
        labels.update(
            {
                "gateway.dask.org/instance": self.gateway_instance,
                "gateway.dask.org/cluster": cluster_name,
                "gateway.dask.org/user": username,
            }
        )
        if component:
            labels["app.kubernetes.io/component"] = component
        return labels

    def make_cluster_object(self, username, options, config):
        cluster_name = uuid.uuid4().hex

        return {
            "apiVersion": f"gateway.dask.org/{self.crd_version}",
            "kind": "DaskCluster",
            "metadata": {
                "name": cluster_name,
                "labels": self.get_labels(cluster_name, username),
                "annotations": self.common_annotations,
            },
            "spec": {"options": options, "config": config.to_dict()},
        }

    async def watcher_loop(self):
        while True:
            cluster_name = await self.queue.get()
            try:
                await self.update_cluster_cache(cluster_name)
            except Exception as exc:
                self.log.info(
                    "Error updating cluster cache for %s", cluster_name, exc_info=exc
                )

    async def update_cluster_cache(self, cluster_name):
        obj, deleted = self.reflector.get(cluster_name)

        if deleted:
            self.log.debug("Deleting %s from cache", cluster_name)
            self.reflector.drop(cluster_name)
            # Delete the cluster from the cache
            cluster = self.clusters.pop(cluster_name, None)
            if cluster is not None:
                user = self.username_to_clusters.get(cluster.username)
                if user is not None:
                    user.pop(cluster.name, None)
                    if not user:
                        self.username_to_clusters.pop(cluster.username, None)
        else:
            self.log.debug("Updating %s in cache", cluster_name)

            old = self.clusters.get(cluster_name)

            status = obj.get("status", {})
            phase = status.get("phase", "Pending")
            status = models.ClusterStatus.from_name(phase)

            cluster = models.Cluster(
                name=cluster_name,
                username=obj["metadata"]["labels"]["gateway.dask.org/user"],
                options=obj["spec"].get("options") or {},
                token="",
                status=status,
                start_time=parse_k8s_timestamp(obj["metadata"]["creationTimestamp"]),
            )

            if old is not None and old.tls_cert:
                cluster.tls_cert = old.tls_cert
                cluster.tls_key = old.tls_key
                cluster.token = old.token
            else:
                secret_name = obj.get("status", {}).get("credentials")
                if secret_name:
                    namespace = obj["metadata"]["namespace"]
                    secret = await self.core_client.read_namespaced_secret(
                        secret_name, namespace
                    )
                    cluster.tls_cert = b64decode(secret.data["dask.crt"])
                    cluster.tls_key = b64decode(secret.data["dask.pem"])
                    cluster.token = b64decode(secret.data["api-token"]).decode()

            self.clusters[cluster.name] = cluster
            self.username_to_clusters[cluster.username][cluster.name] = cluster
