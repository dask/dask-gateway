import asyncio
import os
import uuid
from base64 import b64decode
from collections import defaultdict

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.rest import ApiException
from traitlets import Dict, Float, Unicode, default
from traitlets.config import LoggingConfigurable

from ... import __version__ as VERSION
from ... import models
from ...traitlets import MemoryLimit, Type
from ...utils import Flag
from ...workqueue import WorkQueue, WorkQueueClosed
from ..base import Backend, ClusterConfig
from .utils import Informer, parse_k8s_timestamp

__all__ = ("KubeClusterConfig", "KubeBackend", "KubeBackendAndControllerMixin")


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
        <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#container-v1-core>`__,
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
        <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#container-v1-core>`__,
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
        <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#podspec-v1-core>`__,
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
        <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#podspec-v1-core>`__,
        and should be camelCase.

        See ``worker_extra_pod_config`` for more information.
        """,
        config=True,
    )

    worker_extra_pod_annotations = Dict(
        help="""
        Any extra annotations to be applied to a user's worker pods.

        These annotations can be set using with cluster options (See
        :ref:`exposing-cluster-options`) to allow for injecting user-specific
        information, e.g. adding an annotation based on a user's group or
        username.

        This dict will be merged with ``common_annotations`` before being
        applied to user pods.
        """,
        config=True,
    )

    scheduler_extra_pod_annotations = Dict(
        help="""
        Any extra annotations to be applied to a user's scheduler pods.

        These annotations can be set using with cluster options (See
        :ref:`exposing-cluster-options`) to allow for injecting user-specific
        information, e.g. adding an annotation based on a user's group or
        username.

        This dict will be merged with ``common_annotations`` before being
        applied to user pods.
        """,
        config=True,
    )

    worker_extra_pod_labels = Dict(
        help="""
        Any extra labels to be applied to a user's worker pods.

        These labels can be set using with cluster options (See
        :ref:`exposing-cluster-options`) to allow for injecting user-specific
        information, e.g. adding a label based on a user's group or username.

        This dict will be merged with ``common_labels`` before being
        applied to user pods.
        """,
        config=True,
    )

    scheduler_extra_pod_labels = Dict(
        help="""
        Any extra labels to be applied to a user's scheduler pods.

        These labels can be set using with cluster options (See
        :ref:`exposing-cluster-options`) to allow for injecting user-specific
        information, e.g. adding a label based on a user's group or username.

        This dict will be merged with ``common_labels`` before being
        applied to user pods.
        """,
        config=True,
    )


class KubeBackendAndControllerMixin(LoggingConfigurable):
    """Shared config between the backend and controller"""

    gateway_instance = Unicode(
        help="""
        A unique ID for this instance of dask-gateway.

        The controller must also be configured with the same ID.
        """,
        config=True,
    )

    @default("gateway_instance")
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

    label_selector = Unicode(
        help="""
        The label selector to use when watching objects managed by the gateway.
        """,
        config=True,
    )

    @default("label_selector")
    def _default_label_selector(self):
        return f"gateway.dask.org/instance={self.gateway_instance}"


class KubeBackend(KubeBackendAndControllerMixin, Backend):
    """A dask-gateway backend for running on Kubernetes"""

    cluster_config_class = Type(
        "dask_gateway_server.backends.kubernetes.backend.KubeClusterConfig",
        klass="dask_gateway_server.backends.base.ClusterConfig",
        help="The cluster config class to use",
        config=True,
    )

    async def setup(self, app):
        await super().setup(app)
        try:
            # Not a coroutine for some reason
            config.load_incluster_config()
        except config.ConfigException:
            await config.load_kube_config()

        self.api_client = client.ApiClient()
        self.core_client = client.CoreV1Api(api_client=self.api_client)
        self.custom_client = client.CustomObjectsApi(api_client=self.api_client)

        self.cluster_waiters = defaultdict(Flag)
        self.clusters = {}
        self.username_to_clusters = defaultdict(dict)
        self.queue = WorkQueue()
        self.informer = Informer(
            parent=self,
            name="cluster",
            client=self.custom_client,
            method="list_cluster_custom_object",
            method_kwargs=dict(
                group="gateway.dask.org",
                version=self.crd_version,
                plural="daskclusters",
                label_selector=self.label_selector,
            ),
            on_update=self.on_cluster_event,
            on_delete=self.on_cluster_event,
        )
        await self.informer.start()
        self.sync_task = asyncio.ensure_future(self.sync_clusters_loop())

    async def cleanup(self):
        if hasattr(self, "informer"):
            await self.informer.stop()
        if hasattr(self, "sync_task"):
            self.queue.close()
            try:
                await self.sync_task
            except Exception:
                pass
        if hasattr(self, "api_client"):
            await self.api_client.rest_client.pool_manager.close()
        await super().cleanup()

    async def start_cluster(self, user, cluster_options):
        options, config = await self.process_cluster_options(user, cluster_options)

        obj = self.make_cluster_object(user.name, options, config)

        name = obj["metadata"]["name"]
        cluster_name = f"{config.namespace}.{name}"

        self.log.info("Creating cluster %s for user %s", cluster_name, user.name)

        await self.custom_client.create_namespaced_custom_object(
            "gateway.dask.org", self.crd_version, config.namespace, "daskclusters", obj
        )
        return cluster_name

    async def stop_cluster(self, cluster_name, failed=False):
        cluster = self.clusters.get(cluster_name)
        if cluster is not None and cluster.status >= models.ClusterStatus.STOPPED:
            # We know for sure the cluster was already stopped, do nothing
            return

        self.log.info("Stopping cluster %s", cluster_name)

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
        if wait:
            try:
                waiter = self.cluster_waiters[cluster_name]
                await asyncio.wait_for(waiter, 20)
            except asyncio.TimeoutError:
                pass
        return self.clusters.get(cluster_name)

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
        count = msg["count"]
        namespace, name = cluster_name.split(".")

        self.log.info(
            "Cluster %s heartbeat [count: %d, n_active: %d, n_closing: %d, n_closed: %d]",
            cluster_name,
            count,
            len(msg["active_workers"]),
            len(msg["closing_workers"]),
            len(msg["closed_workers"]),
        )

        cluster = self.clusters.get(cluster_name)
        if cluster is None:
            return
        max_workers = cluster.config.get("cluster_max_workers")
        if max_workers is not None and count > max_workers:
            # This shouldn't happen under normal operation, but could if the
            # user does something malicious (or there's a bug).
            self.log.info(
                "Cluster %s heartbeat requested %d workers, exceeding limit of %s.",
                cluster_name,
                count,
                max_workers,
            )
            count = max_workers

        try:
            await self.custom_client.patch_namespaced_custom_object(
                "gateway.dask.org",
                self.crd_version,
                namespace,
                "daskclusters",
                name,
                [{"op": "add", "path": "/spec/replicas", "value": count}],
            )
        except ApiException as exc:
            if exc.status != 404:
                raise

    def get_labels(self, cluster_name, component=None):
        labels = self.common_labels.copy()
        labels.update(
            {
                "gateway.dask.org/instance": self.gateway_instance,
                "gateway.dask.org/cluster": cluster_name,
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
                "labels": self.get_labels(cluster_name),
                "annotations": self.common_annotations,
            },
            "spec": {
                "username": username,
                "options": options,
                "config": config.to_dict(),
            },
        }

    def get_cluster_name(self, obj):
        namespace = obj["metadata"]["namespace"]
        name = obj["metadata"]["name"]
        return f"{namespace}.{name}"

    def on_cluster_event(self, cluster, old=None):
        key = self.get_cluster_name(cluster)
        self.queue.put(key)

    async def sync_clusters_loop(self):
        while True:
            try:
                name = await self.queue.get()
            except WorkQueueClosed:
                return
            try:
                await self.sync_cluster(name)
            except Exception:
                self.log.warning("Error while syncing cluster %s", name, exc_info=True)
                self.queue.put_backoff(name)
            else:
                self.queue.reset_backoff(name)
            finally:
                self.queue.task_done(name)

    async def sync_cluster(self, cluster_name):
        obj = self.informer.get(cluster_name)

        if obj is None:
            self.log.debug("Deleting %s from cache", cluster_name)
            cluster = self.clusters.pop(cluster_name, None)
            if cluster is not None:
                user = self.username_to_clusters.get(cluster.username)
                if user is not None:
                    user.pop(cluster.name, None)
                    if not user:
                        self.username_to_clusters.pop(cluster.username, None)
            waiter = self.cluster_waiters.pop(cluster_name, None)
            if waiter is not None:
                waiter.set()
        else:
            self.log.debug("Updating %s in cache", cluster_name)

            old = self.clusters.get(cluster_name)

            status = obj.get("status", {})
            phase = status.get("phase", "Pending")
            cluster_status = models.ClusterStatus.from_name(phase)

            service_name = status.get("service")
            if cluster_status == models.ClusterStatus.RUNNING and service_name:
                namespace = obj["metadata"]["namespace"]
                scheduler_address = f"tls://{service_name}.{namespace}:8786"
                dashboard_address = f"http://{service_name}.{namespace}:8787"
                api_address = f"http://{service_name}.{namespace}:8788"
            else:
                scheduler_address = dashboard_address = api_address = ""

            start_time = parse_k8s_timestamp(obj["metadata"]["creationTimestamp"])
            if "completionTime" in status:
                stop_time = parse_k8s_timestamp(status["completionTime"])
            else:
                stop_time = None

            cluster = models.Cluster(
                name=cluster_name,
                username=obj["spec"].get("username", ""),
                options=obj["spec"].get("options") or {},
                config=obj["spec"].get("config") or {},
                token="",
                scheduler_address=scheduler_address,
                dashboard_address=dashboard_address,
                api_address=api_address,
                status=cluster_status,
                start_time=start_time,
                stop_time=stop_time,
            )

            if old is not None and old.tls_cert:
                cluster.tls_cert = old.tls_cert
                cluster.tls_key = old.tls_key
                cluster.token = old.token
            elif cluster_status <= models.ClusterStatus.RUNNING:
                secret_name = status.get("credentials")
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
            if cluster_status >= models.ClusterStatus.RUNNING:
                self.cluster_waiters[cluster.name].set()
