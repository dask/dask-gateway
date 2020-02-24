import asyncio
import json
import os
import sys
import uuid
from base64 import b64decode
from collections import defaultdict
from datetime import datetime, timezone

from traitlets import Float, Dict, Unicode, Any, Int, Instance, default
from traitlets.config import LoggingConfigurable
from kubernetes_asyncio import client, config, watch
from kubernetes_asyncio.client.models import (
    V1Container,
    V1EnvVar,
    V1ResourceRequirements,
    V1VolumeMount,
)

from .. import __version__ as VERSION
from .. import models
from ..traitlets import MemoryLimit, Type
from ..utils import cancel_task, UniqueQueue
from .base import Backend, ClusterConfig


__all__ = ("KubeClusterConfig", "KubeBackend")

# Monkeypatch kubernetes_asyncio to cleanup resources better
client.rest.RESTClientObject.__del__ = lambda self: None


class Watch(watch.Watch):
    def __init__(self, api_client=None, return_type=None):
        self._raw_return_type = return_type
        self._stop = False
        self._api_client = api_client or client.ApiClient()
        self.resource_version = 0
        self.resp = None


if sys.version_info[:2] >= (3, 7):

    def parse_k8s_timestamp(ts):
        t = datetime.fromisoformat(ts[:-1])
        return int(t.replace(tzinfo=timezone.utc).timestamp() * 1000)


else:

    def parse_k8s_timestamp(ts):
        t = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
        return int(t.replace(tzinfo=timezone.utc).timestamp() * 1000)


def merge_json_objects(a, b):
    """Merge two JSON objects recursively.

    - If a dict, keys are merged, preferring ``b``'s values
    - If a list, values from ``b`` are appended to ``a``

    Copying is minimized. No input collection will be mutated, but a deep copy
    is not performed.

    Parameters
    ----------
    a, b : dict
        JSON objects to be merged.

    Returns
    -------
    merged : dict
    """
    if b:
        # Use a shallow copy here to avoid needlessly copying
        a = a.copy()
        for key, b_val in b.items():
            if key in a:
                a_val = a[key]
                if isinstance(a_val, dict) and isinstance(b_val, dict):
                    a[key] = merge_json_objects(a_val, b_val)
                elif isinstance(a_val, list) and isinstance(b_val, list):
                    a[key] = a_val + b_val
                else:
                    a[key] = b_val
            else:
                a[key] = b_val
    return a


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
        "dask_gateway_server.backends.kubernetes.KubeClusterConfig",
        klass="dask_gateway_server.backends.base.ClusterConfig",
        help="The cluster config class to use",
        config=True,
    )

    instance = Unicode(
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

    namespace = Unicode(
        "default",
        help="""
        Kubernetes namespace to create CRD objects in.

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
        self.reflectors = {
            "cluster": Reflector(
                parent=self,
                kind="cluster",
                api_client=self.api_client,
                queue=self.queue,
                method=self.custom_client.list_namespaced_custom_object,
                method_kwargs=dict(
                    namespace=self.namespace,
                    group="gateway.dask.org",
                    version=self.crd_version,
                    plural="daskclusters",
                    label_selector=self.get_label_selector(),
                ),
            ),
            "secret": Reflector(
                parent=self,
                kind="secret",
                api_client=self.api_client,
                queue=self.queue,
                method=self.core_client.list_secret_for_all_namespaces,
                method_kwargs=dict(label_selector=self.get_label_selector()),
            ),
        }
        for reflector in self.reflectors.values():
            await reflector.start()
        self._watcher_task = asyncio.ensure_future(self.watcher_loop())

    async def cleanup(self):
        if hasattr(self, "reflectors"):
            for reflector in self.reflectors.values():
                await reflector.stop()
        if hasattr(self, "_watcher_task"):
            await cancel_task(self._watcher_task)
        if hasattr(self, "api_client"):
            await self.api_client.rest_client.pool_manager.close()

    async def start_cluster(self, user, cluster_options):
        options, config = await self.process_cluster_options(user, cluster_options)

        obj = self.make_cluster_object(user.name, options, config)

        res = await self.custom_client.create_namespaced_custom_object(
            "gateway.dask.org", self.crd_version, self.namespace, "daskclusters", obj
        )

        cluster_name = res["metadata"]["name"]

        self.reflectors["cluster"].put(cluster_name, res)
        self.update_cluster_cache(cluster_name)

        return cluster_name

    async def stop_cluster(self, cluster_name, failed=False):
        raise NotImplementedError

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
        return f"gateway.dask.org/instance={self.instance}"

    def get_env(self, cluster_name, config):
        out = dict(config.environment)
        out.update(
            {
                "DASK_GATEWAY_API_URL": self.api_url,
                "DASK_GATEWAY_CLUSTER_NAME": cluster_name,
                "DASK_GATEWAY_API_TOKEN": "/etc/dask-credentials/api-token",
                "DASK_GATEWAY_TLS_CERT": "/etc/dask-credentials/dask.crt",
                "DASK_GATEWAY_TLS_KEY": "/etc/dask-credentials/dask.pem",
            }
        )
        return out

    def get_scheduler_command(self, config):
        return [
            config.scheduler_cmd,
            "--heartbeat-period",
            str(config.adaptive_period),
            "--idle-timeout",
            str(config.idle_timeout),
        ]

    def get_worker_command(self, config):
        return [
            config.worker_cmd,
            "--nthreads",
            str(int(config.worker_cores_limit)),
            "--memory-limit",
            str(config.worker_memory_limit),
        ]

    def get_labels(self, cluster_name, username, component=None):
        labels = self.common_labels.copy()
        labels.update(
            {
                "gateway.dask.org/instance": self.instance,
                "gateway.dask.org/cluster": cluster_name,
                "gateway.dask.org/user": username,
            }
        )
        if component:
            labels["app.kubernetes.io/component"] = component
        return labels

    def make_pod_templates(self, cluster_name, username, config):
        annotations = self.common_annotations

        env = self.get_env(cluster_name, config)

        def make_pod_template(
            container_name,
            labels,
            mem_req,
            mem_lim,
            cpu_req,
            cpu_lim,
            cmd,
            extra_pod_config,
            extra_container_config,
        ):
            container = V1Container(
                name=container_name,
                image=config.image,
                args=cmd,
                env=[V1EnvVar(k, v) for k, v in env.items()],
                image_pull_policy=config.image_pull_policy,
                resources=V1ResourceRequirements(
                    requests={"cpu": f"{cpu_req:.1f}", "memory": str(mem_req)},
                    limits={"cpu": f"{cpu_lim:.1f}", "memory": str(mem_lim)},
                ),
                volume_mounts=[
                    V1VolumeMount(
                        name="dask-credentials",
                        mount_path="/etc/dask-credentials/",
                        read_only=True,
                    )
                ],
            )

            container = self.api_client.sanitize_for_serialization(container)

            if extra_container_config:
                extra_container_config = self.api_client.sanitize_for_serialization(
                    extra_container_config
                )
                container = merge_json_objects(container, extra_container_config)

            pod = {
                "metadata": {
                    "labels": labels,
                    "annotations": annotations,
                    "namespace": config.namespace,
                },
                "spec": {
                    "containers": [container],
                    "restart_policy": "OnFailure",
                    "automountServiceAccountToken": False,
                },
            }

            if extra_pod_config:
                extra_pod_config = self.api_client.sanitize_for_serialization(
                    extra_pod_config
                )
                pod["spec"] = merge_json_objects(pod["spec"], extra_pod_config)

            return pod

        scheduler = make_pod_template(
            container_name="dask-gateway-scheduler",
            labels=self.get_labels(cluster_name, username, "dask-gateway-scheduler"),
            mem_req=config.scheduler_memory,
            mem_lim=config.scheduler_memory_limit,
            cpu_req=config.scheduler_cores,
            cpu_lim=config.scheduler_cores_limit,
            cmd=self.get_scheduler_command(config),
            extra_pod_config=config.scheduler_extra_pod_config,
            extra_container_config=config.scheduler_extra_container_config,
        )
        worker = make_pod_template(
            container_name="dask-gateway-worker",
            labels=self.get_labels(cluster_name, username, "dask-gateway-worker"),
            mem_req=config.worker_memory,
            mem_lim=config.worker_memory_limit,
            cpu_req=config.worker_cores,
            cpu_lim=config.worker_cores_limit,
            cmd=self.get_worker_command(config),
            extra_pod_config=config.worker_extra_pod_config,
            extra_container_config=config.worker_extra_container_config,
        )
        return scheduler, worker

    def make_cluster_object(self, user_name, options, config):
        cluster_name = uuid.uuid4().hex

        scheduler, worker = self.make_pod_templates(cluster_name, user_name, config)

        annotations = self.common_annotations
        labels = self.get_labels(cluster_name, user_name)

        obj = {
            "apiVersion": f"gateway.dask.org/{self.crd_version}",
            "kind": "DaskCluster",
            "metadata": {
                "name": cluster_name,
                "labels": labels,
                "annotations": annotations,
            },
            "spec": {
                "scheduler": {"template": scheduler},
                "worker": {"template": worker},
                "info": {"options": json.dumps(options, separators=(",", ":"))},
            },
        }
        return obj

    async def watcher_loop(self):
        while True:
            kind, name = await self.queue.get()
            try:
                self.update_cluster_cache(name)
            except Exception as exc:
                self.log.info(
                    "Error updating cluster cache for %s:", name, exc_info=exc
                )

    def update_cluster_cache(self, name):
        obj = self.reflectors["cluster"].get(name)

        if obj is None:
            self.log.debug("Deleting %s from cache", name)
            # Delete the cluster from the cache
            cluster = self.clusters.pop(name, None)
            if cluster is not None:
                user = self.username_to_clusters.get(cluster.username)
                if user is not None:
                    user.pop(cluster.name, None)
                    if not user:
                        self.username_to_clusters.pop(cluster.username, None)
        else:
            self.log.debug("Updating %s in cache", name)

            # TODO: infer more of these fields from the backing kubernetes objects
            # Not all of these are correct
            cluster = models.Cluster(
                name=name,
                username=obj["metadata"]["labels"]["gateway.dask.org/user"],
                token="",
                options=json.loads(obj["spec"]["info"]["options"]),
                status=models.ClusterStatus.PENDING,
                start_time=parse_k8s_timestamp(obj["metadata"]["creationTimestamp"]),
            )

            secret = self.reflectors["secret"].get(name)
            if secret is not None:
                cluster.tls_key = b64decode(secret["data"]["dask.pem"])
                cluster.tls_cert = b64decode(secret["data"]["dask.crt"])
                cluster.token = b64decode(secret["data"]["api-token"])

            self.clusters[cluster.name] = cluster
            self.username_to_clusters[cluster.username][cluster.name] = cluster


class Reflector(LoggingConfigurable):
    kind = Unicode()
    method = Any()
    method_kwargs = Dict()
    timeout_seconds = Int(10)
    api_client = Instance(client.ApiClient)
    cache = Dict()
    queue = Instance(asyncio.Queue, allow_none=True)

    def get(self, name, default=None):
        return self.cache.get(name, default)

    def pop(self, name, default=None):
        self.cache.pop(name, default)

    def put(self, name, value):
        self.cache[name] = value

    async def handle(self, obj, event_type="INITIAL"):
        name = obj["metadata"]["name"]
        self.log.debug("Received %s event for %s[%s]", event_type, self.kind, name)
        if event_type in {"INITIAL", "ADDED", "MODIFIED"}:
            self.cache[name] = obj
        elif event_type == "DELETED":
            self.cache.pop(name, None)
        if self.queue is not None:
            await self.queue.put((self.kind, name))

    async def start(self):
        if not hasattr(self, "_task"):
            self._task = asyncio.ensure_future(self.run())

    async def stop(self):
        if hasattr(self, "_task"):
            await cancel_task(self._task)
            del self._task

    async def run(self):
        self.log.debug("Starting %s watch stream...", self.kind)

        watch = Watch(self.api_client)

        while True:
            try:
                initial = await self.method(**self.method_kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.error(
                    "Error in %s watch stream, retrying...", self.kind, exc_info=exc
                )

            initial = self.api_client.sanitize_for_serialization(initial)
            for obj in initial["items"]:
                await self.handle(obj)
            resource_version = initial["metadata"]["resourceVersion"]

            try:
                while True:
                    kwargs = {
                        "resource_version": resource_version,
                        "timeout_seconds": self.timeout_seconds,
                    }
                    kwargs.update(self.method_kwargs)
                    async with watch.stream(self.method, **kwargs) as stream:
                        async for ev in stream:
                            ev = self.api_client.sanitize_for_serialization(ev)
                            await self.handle(ev["object"], event_type=ev["type"])
                            resource_version = stream.resource_version
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.error(
                    "Error in %s watch stream, retrying...", self.kind, exc_info=exc
                )
        self.log.debug("%s watch stream stopped", self.kind)
