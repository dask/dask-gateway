import json
import os
import uuid

from traitlets import Float, Dict, Unicode, default
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.models import (
    V1Container,
    V1EnvVar,
    V1ResourceRequirements,
    V1VolumeMount,
)

from .. import __version__ as VERSION
from ..traitlets import MemoryLimit, Type
from .base import Backend, ClusterConfig


__all__ = ("KubeClusterConfig", "KubeBackend")


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
        await config.load_kube_config()
        self.api_client = client.ApiClient()
        self.core_client = client.CoreV1Api(api_client=self.api_client)
        self.custom_client = client.CustomObjectsApi(api_client=self.api_client)

    async def cleanup(self):
        pass

    async def start_cluster(self, user, cluster_options):
        options, config = await self.process_cluster_options(user, cluster_options)

        obj = self.make_cluster_object(user.name, options, config)

        await self.custom_client.create_namespaced_custom_object(
            "gateway.dask.org", self.crd_version, self.namespace, "daskclusters", obj
        )

    async def stop_cluster(self, cluster_name, failed=False):
        raise NotImplementedError

    async def get_cluster(self, cluster_name, wait=False):
        raise NotImplementedError

    async def list_clusters(self, username=None, statuses=None):
        raise NotImplementedError

    async def on_cluster_heartbeat(self, cluster_name, msg):
        raise NotImplementedError

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
                "gateway.dask.org/cluster-name": cluster_name,
                "gateway.dask.org/user-name": username,
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
                    requests={"cpu": cpu_req, "memory": mem_req},
                    limits={"cpu": cpu_lim, "memory": mem_lim},
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
                "kind": "Pod",
                "apiVersion": "v1",
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
